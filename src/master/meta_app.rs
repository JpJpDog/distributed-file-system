use crate::{
    datapb::data_rpc_client::DataRpcClient,
    datapb::{
        CommitDataReq, CommitDataRsp, RemoveDataReq, RemoveDataRsp, TransferOp, TransferReq,
        TransferRsp,
    },
    error::MetaError,
    file_meta::{ClientOp, FileMeta, MetaLog, RemoveOp, TransOp},
    metapb::{
        CreateReq, CreateRsp, ReadOp, ReadReq, ReadRsp, RemoveReq, RemoveRsp, WriteReq, WriteRsp,
    },
};
use bincode::{deserialize, serialize};
use futures::future::{join, join_all};
use log::{info, warn};
use rand::Rng;
use sled::Db;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::{spawn, sync::RwLock};
use tonic::{transport::Channel, Request};

pub const REPLI_N: usize = 3;

// type ChunkMapEntry = [u64; REPLI_N];

pub struct MetaRaftApp {
    addrs: Vec<String>,
    addr_map: RwLock<HashMap<u64, String>>,
    // chunk_map: Db,
    meta_db: RwLock<Db>,
    // log_db: Db,
}

impl MetaRaftApp {
    #[inline]
    fn gene_lid() -> u64 {
        rand::thread_rng().gen_range(0..u64::MAX)
    }

    async fn link_clients(addrs: Vec<String>) -> Vec<Arc<RwLock<DataRpcClient<Channel>>>> {
        let handlers = addrs.into_iter().map(|addr| {
            spawn(async move {
                info!("connecting to {}", addr);
                DataRpcClient::connect(addr).await
            })
        });
        let results = join_all(handlers).await;
        let clients = results
            .into_iter()
            .map_while(|result| match result {
                Ok(result) => match result {
                    Ok(r) => Some(Arc::new(RwLock::new(r))),
                    Err(_) => None,
                },
                Err(_) => None,
            })
            .collect::<Vec<_>>();
        clients
    }

    async fn compress_trans_ops(
        &self,
        trans_ops: Vec<TransOp>,
        lid: u64,
        client_addr: String,
    ) -> Result<(Vec<String>, Vec<TransferReq>), MetaError> {
        let mut addr_map = self.addr_map.write().await;
        let mut trans_reqs = HashMap::<String, TransferReq>::new();
        for op in trans_ops {
            if op.new {
                let idx= (op.to_cid % 2) as usize;
                let addr = self.addrs[idx].clone();
                addr_map.insert(op.to_cid, addr);
            }
            match addr_map.get(&op.to_cid) {
                Some(to_addr) => match trans_reqs.get_mut(to_addr) {
                    Some(req) => {
                        let from_addr = match addr_map.get(&op.from_cid) {
                            Some(to_addr) => to_addr.clone(),
                            None => {
                                if op.from_cid == 0 {
                                    client_addr.clone()
                                } else {
                                    todo!()
                                }
                            }
                        };
                        req.ops.push(TransferOp {
                            from_addr,
                            to_cid: op.to_cid,
                            to_offset: op.to_off,
                            from_cid: op.from_cid,
                            from_offset: op.from_off,
                            size: op.size,
                        });
                    }
                    None => {
                        let from_addr = match addr_map.get(&op.from_cid) {
                            Some(from_addr) => from_addr.clone(),
                            None => {
                                if op.from_cid == 0 {
                                    client_addr.clone()
                                } else {
                                    todo!()
                                }
                            }
                        };
                        trans_reqs.insert(
                            to_addr.clone(),
                            TransferReq {
                                lid,
                                ops: vec![TransferOp {
                                    from_addr,
                                    to_cid: op.to_cid,
                                    to_offset: op.to_off,
                                    from_cid: op.from_cid,
                                    from_offset: op.from_off,
                                    size: op.size,
                                }],
                            },
                        );
                    }
                },
                None => todo!(),
            }
        }
        let addrs = trans_reqs.keys().cloned().collect::<Vec<_>>();
        let reqs = trans_reqs.into_values().collect::<Vec<_>>();
        Ok((addrs, reqs))
    }

    async fn handle_trans_reqs(
        clients: Vec<Arc<RwLock<DataRpcClient<Channel>>>>,
        reqs: Vec<TransferReq>,
    ) -> Vec<TransferRsp> {
        let results = clients.into_iter().enumerate().map(|(idx, client)| {
            spawn({
                let req = reqs.get(idx).unwrap().clone();
                info!("start transfer: {:?}", req);
                async move { client.write().await.transfer(Request::new(req)).await }
            })
        });
        let rsps = join_all(results).await;
        let rsps = rsps
            .into_iter()
            .map_while(|rsp| match rsp {
                Ok(rsp) => match rsp {
                    Ok(rsp) => Some(rsp.into_inner()),
                    Err(_) => None,
                },
                Err(_) => None,
            })
            .collect::<Vec<_>>();
        rsps
    }

    async fn handle_commit_reqs(
        clients: Vec<Arc<RwLock<DataRpcClient<Channel>>>>,
        req: CommitDataReq,
    ) -> Vec<CommitDataRsp> {
        let handlers = clients.into_iter().map(|client| {
            let req = req.clone();
            spawn(async move {
                let mut client = client.write().await;
                client.commit(req).await
            })
        });
        let rsps = join_all(handlers).await;
        let rsps = rsps
            .into_iter()
            .map_while(|rsp| match rsp {
                Ok(rsp) => match rsp {
                    Ok(rsp) => Some(rsp.into_inner()),
                    Err(_) => None,
                },
                Err(_) => None,
            })
            .collect::<Vec<_>>();
        rsps
    }

    async fn compress_remove_ops(
        &self,
        remove_ops: Vec<RemoveOp>,
        lid: u64,
    ) -> Result<(Vec<String>, Vec<RemoveDataReq>), MetaError> {
        let addr_map = self.addr_map.read().await;
        let mut remove_reqs = HashMap::<String, RemoveDataReq>::new();
        for RemoveOp { cid } in remove_ops {
            match addr_map.get(&cid) {
                Some(addr) => match remove_reqs.get_mut(addr) {
                    Some(req) => req.cids.push(cid),
                    None => {
                        remove_reqs.insert(
                            addr.clone(),
                            RemoveDataReq {
                                cids: vec![cid],
                                lid,
                            },
                        );
                    }
                },
                None => todo!(),
            }
        }
        let addrs = remove_reqs.keys().cloned().collect::<Vec<_>>();
        let reqs = remove_reqs.into_values().collect::<Vec<_>>();
        Ok((addrs, reqs))
    }

    async fn handle_remove_reqs(
        clients: Vec<Arc<RwLock<DataRpcClient<Channel>>>>,
        reqs: Vec<RemoveDataReq>,
    ) -> Vec<RemoveDataRsp> {
        let results = clients.into_iter().enumerate().map(|(idx, client)| {
            let req = reqs.get(idx).unwrap().clone();
            spawn(async move { client.write().await.remove(req).await })
        });
        let rsps = join_all(results).await;
        let rsps = rsps
            .into_iter()
            .map_while(|rsp| match rsp {
                Ok(rsp) => match rsp {
                    Ok(rsp) => Some(rsp.into_inner()),
                    Err(_) => None,
                },
                Err(_) => None,
            })
            .collect::<Vec<_>>();
        rsps
    }

    fn add_trans_op(trans_ops: &mut Vec<TransOp>, client_ops: Vec<ClientOp>) {
        let mut off = 0;
        for op in client_ops {
            trans_ops.push(TransOp {
                from_cid: 0,
                from_off: off,
                to_cid: op.cid,
                to_off: op.offset,
                size: op.size,
                new: op.new,
            });
            off += op.size
        }
    }

    fn join_client(
        mut clients1: Vec<Arc<RwLock<DataRpcClient<Channel>>>>,
        clients2: Vec<Arc<RwLock<DataRpcClient<Channel>>>>,
        addrs1: Vec<String>,
        addrs2: Vec<String>,
    ) -> Vec<Arc<RwLock<DataRpcClient<Channel>>>> {
        let mut set = HashSet::new();
        for addr in addrs1 {
            let not_exist = set.insert(addr);
            assert!(not_exist);
        }
        for (idx, addr) in addrs2.into_iter().enumerate() {
            let not_exist = set.insert(addr);
            if not_exist {
                clients1.push(clients2.get(idx).unwrap().clone())
            }
        }
        clients1
    }
}

impl MetaRaftApp {
    async fn remove_to_worker(&self, ops: Vec<RemoveOp>) -> Result<bool, MetaError> {
        let lid = MetaRaftApp::gene_lid();
        let (addrs, reqs) = self.compress_remove_ops(ops, lid).await?;
        let remove_n = reqs.len();
        let clients = MetaRaftApp::link_clients(addrs).await;
        if remove_n != clients.len() {
            todo!()
        }
        let rsps = MetaRaftApp::handle_remove_reqs(clients.clone(), reqs).await;
        if rsps.len() != remove_n {
            todo!()
        }
        let commit_req = CommitDataReq { lid };
        let rsps = MetaRaftApp::handle_commit_reqs(clients, commit_req).await;
        if rsps.len() != remove_n {
            todo!()
        }
        Ok(true)
    }

    async fn write_to_worker(
        &self,
        trans_ops: Vec<TransOp>,
        remove_ops: Vec<RemoveOp>,
        client_addr: String,
    ) -> Result<bool, MetaError> {
        let lid = MetaRaftApp::gene_lid();
        let (trans_addrs, trans_reqs) =
            self.compress_trans_ops(trans_ops, lid, client_addr).await?;
        let (remove_addrs, remove_reqs) = self.compress_remove_ops(remove_ops, lid).await?;
        let (addrs1, addrs2) = (trans_addrs.clone(), remove_addrs.clone());
        let transfer_handler = spawn(async move {
            let trans_n = trans_reqs.len();
            if trans_n == 0 {
                return Some(vec![]);
            }
            let trans_clients = MetaRaftApp::link_clients(trans_addrs).await;
            if trans_n != trans_clients.len() {
                todo!()
            }
            let rsps = MetaRaftApp::handle_trans_reqs(trans_clients.clone(), trans_reqs).await;
            if rsps.len() != trans_n {
                todo!()
            }
            info!("success all transfer");
            Some(trans_clients)
        });
        let remove_handler = spawn(async move {
            let remove_n = remove_reqs.len();
            if remove_n == 0 {
                return Some(vec![]);
            }
            let remove_clients = MetaRaftApp::link_clients(remove_addrs).await;
            if remove_clients.len() != remove_n {
                todo!()
            }
            let rsps = MetaRaftApp::handle_remove_reqs(remove_clients.clone(), remove_reqs).await;
            if rsps.len() != remove_n {
                todo!()
            }
            info!("success all remove");
            Some(remove_clients)
        });
        let rsp = join(transfer_handler, remove_handler).await;
        if rsp.0.is_err() || rsp.0.is_err() {
            todo!()
        }
        let clients1 = rsp.0.unwrap();
        let clients2 = rsp.1.unwrap();
        if clients1.is_none() || clients2.is_none() {
            info!("{:?} {:?}", clients1, clients2);
            todo!()
        }
        info!("start commit");
        let clients =
            MetaRaftApp::join_client(clients1.unwrap(), clients2.unwrap(), addrs1, addrs2);
        let commit_n = clients.len();
        let commit_req = CommitDataReq { lid };
        let rsp = MetaRaftApp::handle_commit_reqs(clients, commit_req).await;
        if rsp.len() != commit_n {
            todo!()
        }
        info!("success all commit!");
        Ok(true)
    }
}

impl MetaRaftApp {
    pub fn new(meta_path: String, _map_path: String) -> Self {
        Self {
            addr_map: RwLock::new(HashMap::new()),
            // chunk_map: sled::open(map_path).unwrap(),
            meta_db: RwLock::new(sled::open(meta_path).unwrap()),
            addrs: vec![
                "http://127.0.0.1:11112".to_string(),
                "http://127.0.0.1:44445".to_string(),
            ],
        }
    }

    pub async fn read(&self, req: ReadReq) -> Result<ReadRsp, MetaError> {
        info!("[read] {:?}", req);
        let meta_db = self.meta_db.read().await;
        let meta = meta_db.get(req.filename.as_bytes()).unwrap();
        if meta.is_none() {
            todo!();
        }
        let file_meta: FileMeta = deserialize(&meta.unwrap()).unwrap();
        let ops = file_meta.read(req.offset, req.size);
        drop(meta_db);
        let mut client_ops = vec![];
        for op in ops {
            let addr_map = self.addr_map.read().await;
            let addr = addr_map.get(&op.cid);
            if addr.is_none() {
                todo!()
            }
            client_ops.push(ReadOp {
                addr: addr.unwrap().clone(),
                cid: op.cid,
                offset: op.offset,
                size: op.size,
            });
        }
        info!("[read] finish");
        Ok(ReadRsp { ops: client_ops })
    }

    pub async fn create(&self, req: CreateReq) -> Result<CreateRsp, MetaError> {
        info!("[create] {:?}", req);
        let meta_db = self.meta_db.write().await;
        let prev = meta_db.get(&req.filename).unwrap();
        match prev {
            Some(_) => Ok(CreateRsp { exist: true }),
            None => {
                let init_meta = FileMeta::new();
                meta_db
                    .insert(&req.filename, serialize(&init_meta).unwrap())
                    .unwrap();
                Ok(CreateRsp { exist: false })
            }
        }
    }

    pub async fn remove(&self, req: RemoveReq) -> Result<RemoveRsp, MetaError> {
        info!("[remove] {:?}", req);
        let meta_db = self.meta_db.write().await;
        let meta = meta_db.get(&req.filename).unwrap();
        if meta.is_none() {
            return Ok(RemoveRsp { exist: false });
        }
        let meta: FileMeta = deserialize(&meta.unwrap()).unwrap();
        let ops = meta.remove();
        if ops.len() == 0 {
            meta_db.remove(&req.filename).unwrap();
            Ok(RemoveRsp { exist: true })
        } else {
            drop(meta_db);
            let ok = self.remove_to_worker(ops).await;
            if ok.is_err() {
                todo!()
            }
            let meta_db = self.meta_db.write().await;
            meta_db.remove(&req.filename).unwrap();
            Ok(RemoveRsp { exist: ok.unwrap() })
        }
    }

    pub async fn write(&self, req: WriteReq) -> Result<WriteRsp, MetaError> {
        info!("[write] {:?}", req);
        let meta_db = self.meta_db.read().await;
        let meta = meta_db.get(&req.filename).unwrap();
        if meta.is_none() {
            todo!()
        }
        let mut meta: FileMeta = deserialize(&meta.unwrap()).unwrap();
        drop(meta_db);
        let rsp = meta.write(req.offset, req.ins, req.del);
        if rsp.is_none() {
            todo!()
        }
        let (client_ops, mut trans_ops, remove_ops, meta_log) = rsp.unwrap();
        MetaRaftApp::add_trans_op(&mut trans_ops, client_ops);
        let rsp = self
            .write_to_worker(trans_ops, remove_ops, req.client_addr)
            .await;
        let rsp = match rsp {
            Ok(ok) => {
                meta.commit(meta_log);
                let meta_db = self.meta_db.read().await;
                meta_db
                    .insert(&req.filename, serialize(&meta).unwrap())
                    .unwrap();
                Ok(WriteRsp { ok })
            }
            Err(_) => todo!(),
        };
        rsp
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;

//     fn init_addr_map() -> HashMap<u64, String> {
//         [
//             (1, "1".to_string()),
//             (2, "2".to_string()),
//             (3, "1".to_string()),
//             (4, "2".to_string()),
//             (5, "1".to_string()),
//         ]
//         .iter()
//         .cloned()
//         .collect()
//     }

//     #[tokio::test]
//     async fn test_compress_remove_ops() {
//         println!("start test compress remove ops");
//         let addr_map = init_addr_map();
//         let remove_ops = vec![
//             RemoveOp { cid: 1 },
//             RemoveOp { cid: 3 },
//             RemoveOp { cid: 4 },
//         ];
//         let lid = 1;
//         let remove_reqs = MetaRaftApp::compress_remove_ops(&addr_map, remove_ops, lid).unwrap();
//         println!("results:{:?}", remove_reqs);
//         println!("")
//     }

//     #[tokio::test]
//     async fn test_compress_trans_ops() {
//         println!("start test compress trans ops");
//         let addr_map = init_addr_map();
//         let trans_ops = vec![
//             TransOp {
//                 from_cid: 1,
//                 to_cid: 1,
//                 from_off: 0,
//                 to_off: 0,
//                 size: 0,
//                 new: false,
//             },
//             TransOp {
//                 from_cid: 3,
//                 to_cid: 4,
//                 from_off: 0,
//                 to_off: 0,
//                 size: 0,
//                 new: false,
//             },
//             TransOp {
//                 from_cid: 2,
//                 to_cid: 3,
//                 from_off: 0,
//                 to_off: 0,
//                 size: 0,
//                 new: false,
//             },
//             TransOp {
//                 from_cid: 0,
//                 to_cid: 5,
//                 from_off: 0,
//                 to_off: 0,
//                 size: 0,
//                 new: false,
//             },
//         ];
//         let lid = 1;
//         let trans_reqs =
//             MetaRaftApp::compress_trans_ops(&addr_map, trans_ops, lid, "3".to_string()).unwrap();
//         println!("results:{:?}", trans_reqs);
//         println!("");
//     }
// }
