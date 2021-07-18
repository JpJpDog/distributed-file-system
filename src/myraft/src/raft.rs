use crate::network::MyRaftRpc;
use crate::raftpb::raft_rpc_server::RaftRpcServer;
use crate::{network::MyRaftNetwork, storage::MyRaftStorage};
use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::raft::ClientWriteRequest;
use async_raft::{AppData, AppDataResponse};
use async_raft::{Config, NodeId, Raft};
use log::{error, info};
use std::collections::HashMap;
use std::env;
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::RwLock;
use tonic::transport::Server;
use zookeeper::{Acl, CreateMode, WatchedEvent, WatchedEventType, Watcher, ZkError, ZooKeeper};

#[async_trait]
pub trait RaftApp: Send + Sync + 'static {
    type WriteReq: AppData;
    type WriteRsp: AppDataResponse;

    async fn handle_write(&mut self, req: Self::WriteReq) -> Result<Self::WriteRsp>;
    async fn make_snapshot(&self) -> Result<Vec<u8>>;
    async fn handle_snapshot(&self, snap: &Vec<u8>) -> Result<()>;
}

fn zk_server_urls() -> String {
    let key = "ZOOKEEPER_SERVERS";
    match env::var(key) {
        Ok(val) => val,
        Err(_) => "localhost:2181".to_string(),
    }
}

struct NopWatcher;

impl Watcher for NopWatcher {
    fn handle(&self, e: WatchedEvent) {
        info!("{:?}", e)
    }
}

struct NodeWatcher {
    sender: Sender<()>,
}

impl Watcher for NodeWatcher {
    fn handle(&self, e: WatchedEvent) {
        info!("watcher get event {:?}", e);
        match e.event_type {
            WatchedEventType::NodeChildrenChanged => {
                self.sender.send(()).unwrap();
            }
            _ => todo!(),
        }
    }
}

pub struct MyRaft<T: RaftApp> {
    my_network: Arc<MyRaftNetwork<T>>,
    pub my_storage: Arc<MyRaftStorage<T>>,
    my_core: Arc<Raft<T::WriteReq, T::WriteRsp, MyRaftNetwork<T>, MyRaftStorage<T>>>,
    my_id: NodeId,
    my_addr: String,
}

impl<T: RaftApp> MyRaft<T> {
    pub async fn new(id: NodeId, raft_addr: String, sm: Arc<RwLock<T>>) -> Self {
        let my_network = Arc::new(MyRaftNetwork::<T>::new(id, raft_addr.clone()));
        let my_storage = Arc::new(MyRaftStorage::<T>::new(id, sm));
        let my_config = Arc::new(
            Config::build("test".into())
                .validate()
                .expect("failed to build Raft config"),
        );
        let my_core = Arc::new(Raft::new(
            id,
            my_config.clone(),
            my_network.clone(),
            my_storage.clone(),
        ));
        let raft_rpc = MyRaftRpc {
            core: my_core.clone(),
        };
        let addr = raft_addr.clone().parse().unwrap();
        info!("raft start listening at {}", addr);
        let _handler = spawn(async move {
            Server::builder()
                .add_service(RaftRpcServer::new(raft_rpc))
                .serve(addr)
                .await
                .unwrap();
        });
        Self {
            my_network,
            my_storage,
            my_core,
            my_id: id,
            my_addr: raft_addr,
        }
    }

    pub async fn join_cluster(&self, cluster_id: u64, as_init: bool) {
        if as_init {
            info!("start initialize");
            let _ = self
                .my_core
                .initialize([self.my_id].iter().cloned().collect())
                .await;
        }
        let zk_urls = zk_server_urls();
        let zk = ZooKeeper::connect(&zk_urls, Duration::from_secs(5), NopWatcher).unwrap();
        let zk = Arc::new(zk);
        let watch_path = format!("/raft/{}", &cluster_id.to_string());
        let node_path = format!("{}/{}", &watch_path, &self.my_id.to_string());
        while let Err(err) = zk.create(
            &node_path,
            self.my_addr.as_bytes().to_vec(),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        ) {
            match err {
                ZkError::NoNode => {
                    while let Err(err) = zk.create(
                        &watch_path,
                        vec![],
                        Acl::open_unsafe().clone(),
                        CreateMode::Persistent,
                    ) {
                        match err {
                            ZkError::NoNode => {
                                let p = zk
                                    .create(
                                        "/raft",
                                        vec![],
                                        Acl::open_unsafe().clone(),
                                        CreateMode::Persistent,
                                    )
                                    .unwrap();
                                info!("created zk node {}", p);
                            }
                            ZkError::NodeExists => break,
                            _ => {
                                error!("{:?}", err);
                                todo!()
                            }
                        }
                    }
                    info!("created zk node {}", watch_path);
                }
                ZkError::NodeExists => break,
                _ => {
                    error!("{:?}", err);
                    todo!()
                }
            }
        }
        info!("created zk node {}", node_path);
        let my_network = self.my_network.clone();
        let my_core = self.my_core.clone();
        let my_id = self.my_id;
        let _handler = spawn(async move {
            loop {
                let (sender, receiver) = channel();
                let nodes = zk
                    .get_children_w(&watch_path, NodeWatcher { sender })
                    .unwrap();
                info!("cluster change:{:?}", &nodes);
                let mut new_rt = HashMap::new();
                for node in nodes {
                    let id = node.parse().unwrap();
                    let path = format!("{}/{}", watch_path, node);
                    let data = zk.get_data(&path, false).unwrap().0;
                    let addr = String::from_utf8(data).unwrap();
                    new_rt.insert(id, addr);
                }
                let adds = my_network.update_rt(&new_rt).await;
                let members = new_rt.keys().cloned().collect();
                let metrics = my_core.metrics().borrow().clone();
                if let Some(leader_id) = metrics.current_leader {
                    info!("leader is {}", leader_id);
                    if leader_id == my_id {
                        for add in adds {
                            my_core.add_non_voter(add).await.unwrap();
                            info!("added non voter {}", add);
                        }
                        my_core.change_membership(members).await.unwrap();
                    }
                } else {
                    // panic!("no leader now!");
                }
                info!("watching {}", watch_path);
                let _ = receiver.recv().unwrap();
            }
        });
    }

    pub async fn client_write(&self, req: T::WriteReq) -> Result<T::WriteRsp> {
        let rsp = self
            .my_core
            .client_write(ClientWriteRequest::new(req))
            .await?;
        Ok(rsp.data)
    }
}
