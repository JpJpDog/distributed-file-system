use crate::datapb::data_rpc_client::DataRpcClient;
use crate::datapb::{ReadDataReq, ReadDataRsp};
use crate::error::DataError;
use anyhow::Result;
use bincode::{deserialize, serialize};
use futures::executor::block_on;
use futures::future::join_all;
use log::{info, warn};
use myraft::async_trait::async_trait;
use myraft::{raft::RaftApp, AppData, AppDataResponse};
use serde::{Deserialize, Serialize};
use sled::Db;
use std::collections::HashMap;
use std::ffi::OsString;
use std::io::SeekFrom;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::{fs, spawn};
use tonic::Request;

#[derive(Serialize, Deserialize, Clone)]
pub struct TransOp {
    pub from_addr: String,
    pub to_cid: u64,
    pub to_offset: u64,
    pub from_cid: u64,
    pub from_offset: u64,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum WriteRequest {
    Remove { cids: Vec<u64>, lid: u64 },
    Commit { lid: u64 },
    Transfer { ops: Vec<TransOp>, lid: u64 },
}

impl AppData for WriteRequest {}

#[derive(Serialize, Deserialize, Clone)]
pub enum WriteResponse {
    Remove { ok: bool },
    Commit { ok: bool },
    Transfer { ok: bool },
}

impl AppDataResponse for WriteResponse {}

#[derive(Serialize, Deserialize)]
struct DataRaftAppSnapshot {
    db_map: HashMap<Vec<u8>, Vec<u8>>,
    file_map: HashMap<OsString, Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct TransStore {
    to_cid: u64,
    to_offset: u64,
    data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
enum DbStore {
    Remove { cids: Vec<u64> },
    Transfer { stores: Vec<TransStore> },
}

pub struct DataRaftApp {
    db: Db,
    file_path: String,
    my_addr: String,
}

impl DataRaftApp {
    pub async fn new(db_path: String, file_path: String, my_addr: String) -> Self {
        let db = sled::open(db_path).unwrap();
        fs::create_dir(&file_path).await.unwrap();
        Self {
            file_path,
            db,
            my_addr,
        }
    }

    #[inline]
    fn get_fname(&self, cid: u64) -> String {
        format!("{}/{}", self.file_path, cid.to_string())
    }

    pub async fn handle_read(&self, req: ReadDataReq) -> Result<ReadDataRsp> {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .open(self.get_fname(req.cid))
            .await?;
        file.seek(SeekFrom::Start(req.offset)).await?;
        let mut data = vec![0 as u8; req.size as usize];
        file.read_exact(data.as_mut_slice()).await?;
        Ok(ReadDataRsp { data })
    }

    async fn handle_trans_ops(&self, ops: Vec<TransOp>) -> Result<DbStore, DataError> {
        let reqs = ops
            .iter()
            .map(|op| ReadDataReq {
                cid: op.from_cid,
                offset: op.from_offset,
                size: op.size,
            })
            .collect::<Vec<_>>();
        let mut handlers = vec![];
        let mut self_store = None;
        let ops = ops
            .into_iter()
            .map_while(|op| {
                if op.from_addr == self.my_addr {
                    let data = block_on(self.handle_read(ReadDataReq {
                        cid: op.from_cid,
                        offset: op.from_offset,
                        size: op.size,
                    }));
                    if data.is_err() {
                        todo!()
                    }
                    let data = data.unwrap().data;
                    self_store = Some(TransStore {
                        to_cid: op.to_cid,
                        to_offset: op.to_offset,
                        data,
                    });
                    None
                } else {
                    let addr = op.from_addr.clone();
                    handlers.push(spawn(async move { DataRpcClient::connect(addr).await }));
                    Some(op)
                }
            })
            .collect::<Vec<_>>();
        let trans_n = ops.len();
        let clients = join_all(handlers).await;
        let clients = clients
            .into_iter()
            .map_while(|client| match client {
                Ok(client) => match client {
                    Ok(client) => Some(client),
                    Err(_) => None,
                },
                Err(_) => None,
            })
            .collect::<Vec<_>>();
        if clients.len() != trans_n {
            todo!()
        }
        let handlers = clients.into_iter().enumerate().map(|(idx, mut client)| {
            let req = reqs.get(idx).unwrap().clone();
            spawn(async move { client.read(Request::new(req)).await })
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
        if rsps.len() != trans_n {
            todo!()
        }
        let mut stores = rsps
            .into_iter()
            .enumerate()
            .map(|(idx, rsp)| {
                let op = ops.get(idx).unwrap();
                TransStore {
                    to_cid: op.to_cid,
                    to_offset: op.to_offset,
                    data: rsp.data,
                }
            })
            .collect::<Vec<_>>();
        if let Some(self_store) = self_store {
            stores.push(self_store);
        }
        let stores = DbStore::Transfer { stores };
        Ok(stores)
    }

    async fn handle_commit(&self, stores: Vec<DbStore>) -> Result<bool, DataError> {
        for store in stores {
            match store {
                DbStore::Remove { cids } => {
                    let futures = cids
                        .into_iter()
                        .map(|cid| {
                            self.get_fname(cid);
                            fs::remove_file(self.get_fname(cid))
                        })
                        .collect::<Vec<_>>();
                    let rsps = join_all(futures).await;
                    for rsp in rsps {
                        if rsp.is_err() {
                            // TODO:
                            // todo!()
                        }
                    }
                }
                DbStore::Transfer { stores } => {
                    let futures = stores
                        .into_iter()
                        .map(|store| {
                            let f_name = self.get_fname(store.to_cid);
                            spawn(async move {
                                let mut file = fs::OpenOptions::new()
                                    .write(true)
                                    .create(true)
                                    .open(f_name)
                                    .await
                                    .unwrap();
                                file.seek(SeekFrom::Start(store.to_offset)).await.unwrap();
                                file.write(store.data.as_slice()).await.unwrap();
                            })
                        })
                        .collect::<Vec<_>>();
                    let rsps = join_all(futures).await;
                    for rsp in rsps {
                        if rsp.is_err() {
                            todo!()
                        }
                    }
                }
            }
        }
        Ok(true)
    }
}

#[async_trait]
impl RaftApp for DataRaftApp {
    type WriteReq = WriteRequest;

    type WriteRsp = WriteResponse;

    async fn handle_write(&mut self, req: Self::WriteReq) -> Result<Self::WriteRsp> {
        match req {
            WriteRequest::Remove { cids, lid } => {
                let prev = self.db.get(lid.to_ne_bytes()).unwrap();
                match prev {
                    Some(prev) => {
                        let mut prev: Vec<DbStore> = deserialize(&prev).unwrap();
                        prev.push(DbStore::Remove { cids });
                        let bytes = serialize(&prev).unwrap();
                        self.db.insert(lid.to_ne_bytes(), bytes).unwrap();
                    }
                    None => {
                        let bytes = serialize(&(vec![DbStore::Remove { cids }])).unwrap();
                        self.db.insert(lid.to_ne_bytes(), bytes).unwrap();
                    }
                }
                Ok(WriteResponse::Remove { ok: true })
            }
            WriteRequest::Commit { lid } => {
                let store = self.db.remove(lid.to_ne_bytes()).unwrap();
                if store.is_none() {
                    todo!()
                }
                let stores: Vec<DbStore> = deserialize(&store.unwrap()).unwrap();
                let rsp = self.handle_commit(stores).await;
                match rsp {
                    Ok(ok) => Ok(WriteResponse::Commit { ok }),
                    Err(_) => todo!(),
                }
            }
            WriteRequest::Transfer { ops, lid } => match self.handle_trans_ops(ops).await {
                Ok(store) => {
                    let prev = self.db.get(lid.to_ne_bytes()).unwrap();
                    match prev {
                        Some(prev) => {
                            let mut prev: Vec<DbStore> = deserialize(&prev).unwrap();
                            prev.push(store);
                            let bytes = serialize(&prev).unwrap();
                            self.db.insert(lid.to_ne_bytes(), bytes).unwrap();
                        }
                        None => {
                            let bytes = serialize(&(vec![store])).unwrap();
                            self.db.insert(lid.to_ne_bytes(), bytes).unwrap();
                        }
                    }
                    Ok(WriteResponse::Transfer { ok: true })
                }
                Err(_) => todo!(),
            },
        }
    }

    async fn make_snapshot(&self) -> Result<Vec<u8>> {
        let mut file_map = HashMap::new();
        let mut dir = fs::read_dir(&self.file_path).await?;
        while let Some(entry) = dir.next_entry().await? {
            let fname = entry.file_name();
            let fpath = format!("{}/{}", self.file_path, fname.to_str().unwrap());
            let mut file = fs::OpenOptions::new().read(true).open(fpath).await?;
            let mut data = vec![];
            file.read_to_end(&mut data).await?;
            file_map.insert(fname, data).unwrap();
        }
        let mut db_map = HashMap::new();
        for kv in self.db.iter() {
            let kv = kv?;
            db_map.insert(kv.0.to_vec(), kv.1.to_vec());
        }
        let snap = DataRaftAppSnapshot { db_map, file_map };
        Ok(serialize(&snap)?)
    }

    async fn handle_snapshot(&self, snap: &Vec<u8>) -> Result<()> {
        let snap: DataRaftAppSnapshot = deserialize(snap)?;
        self.db.clear()?;
        let _ = fs::remove_dir(&self.file_path).await;
        for (fname, data) in snap.file_map {
            let fpath = format!("{}/{}", self.file_path, fname.to_str().unwrap());
            let mut file = fs::OpenOptions::new().write(true).open(fpath).await?;
            file.write_all(&data).await?;
        }
        for (key, value) in snap.db_map {
            self.db.insert(key, value)?;
        }
        Ok(())
    }
}
