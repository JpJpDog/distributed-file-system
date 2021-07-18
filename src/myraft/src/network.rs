mod raftpb {
    tonic::include_proto!("raftpb");
}

use crate::raft::RaftApp;
use crate::raftpb::raft_rpc_client::RaftRpcClient;
use crate::raftpb::raft_rpc_server::RaftRpc;
use crate::raftpb::{RawDataReq, RawDataRsp};
use crate::storage::MyRaftStorage;
use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use async_raft::{NodeId, Raft, RaftNetwork};
use bincode::{deserialize, serialize};
use log::info;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

pub struct MyRaftNetwork<T: RaftApp> {
    routing_table: RwLock<HashMap<NodeId, String>>,
    self_id: NodeId,
    app_type: PhantomData<T>,
}

impl<T: RaftApp> MyRaftNetwork<T> {
    pub fn new(id: u64, addr: String) -> Self {
        let mut routing_table = HashMap::new();
        routing_table.insert(id, addr);
        let routing_table = RwLock::new(routing_table);
        Self {
            self_id: id,
            routing_table,
            app_type: PhantomData,
        }
    }

    pub async fn update_rt(&self, new_rt: &HashMap<NodeId, String>) -> Vec<NodeId> {
        let mut rt = self.routing_table.write().await;
        let mut adds = vec![];
        for new_node in new_rt {
            let (new_id, new_addr) = new_node;
            if let Some(old_addr) = rt.get(&new_id) {
                if old_addr.eq(new_addr) {
                    continue;
                }
            }
            rt.insert(*new_id, new_addr.clone());
            if *new_node.0 != self.self_id {
                adds.push(*new_id);
            }
        }
        adds
    }
}

#[async_trait]
impl<T: RaftApp> RaftNetwork<T::WriteReq> for MyRaftNetwork<T> {
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<T::WriteReq>,
    ) -> Result<AppendEntriesResponse> {
        let rt = self.routing_table.read().await;
        let addr = rt
            .get(&target)
            .expect(&format!("no id {} in routing table", target))
            .clone();
        drop(rt);
        let mut client = RaftRpcClient::connect(format!("http://{}", addr)).await?;
        let req = Request::new(RawDataReq {
            data: serialize(&rpc)?,
        });
        let rsp = client.append_entries(req).await?;
        let rsp = deserialize(&rsp.get_ref().data)?;
        Ok(rsp)
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let rt = self.routing_table.read().await;
        let addr = rt
            .get(&target)
            .expect(&format!("no id {} in routing table", target))
            .clone();
        drop(rt);
        let mut client = RaftRpcClient::connect(format!("http://{}", addr)).await?;
        let req = Request::new(RawDataReq {
            data: serialize(&rpc)?,
        });
        let rsp = client.install_snapshot(req).await?;
        let rsp = deserialize(&rsp.get_ref().data)?;
        Ok(rsp)
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let rt = self.routing_table.read().await;
        let addr = rt
            .get(&target)
            .expect(&format!("no id {} in routing table", target))
            .clone();
        drop(rt);
        let mut client = RaftRpcClient::connect(format!("http://{}", addr)).await?;
        let req = Request::new(RawDataReq {
            data: serialize(&rpc)?,
        });
        let rsp = client.vote(req).await?;
        let rsp = deserialize(&rsp.get_ref().data)?;
        Ok(rsp)
    }
}

pub struct MyRaftRpc<T: RaftApp> {
    pub core: Arc<Raft<T::WriteReq, T::WriteRsp, MyRaftNetwork<T>, MyRaftStorage<T>>>,
}

#[async_trait]
impl<T: RaftApp> RaftRpc for MyRaftRpc<T> {
    async fn append_entries(
        &self,
        request: Request<RawDataReq>,
    ) -> Result<Response<RawDataRsp>, Status> {
        let req = deserialize(&request.get_ref().data).unwrap();
        let rsp = self.core.append_entries(req).await.unwrap();
        let rsp = Response::new(RawDataRsp {
            data: serialize(&rsp).unwrap(),
        });
        Ok(rsp)
    }
    async fn vote(&self, request: Request<RawDataReq>) -> Result<Response<RawDataRsp>, Status> {
        let req: VoteRequest = deserialize(&request.get_ref().data).unwrap();
        info!("recv vote from {}", req.candidate_id);
        let rsp = self.core.vote(req).await.unwrap();
        let rsp = Response::new(RawDataRsp {
            data: serialize(&rsp).unwrap(),
        });
        Ok(rsp)
    }
    async fn install_snapshot(
        &self,
        request: Request<RawDataReq>,
    ) -> Result<Response<RawDataRsp>, Status> {
        let req = deserialize(&request.get_ref().data).unwrap();
        let rsp = self.core.install_snapshot(req).await.unwrap();
        let rsp = Response::new(RawDataRsp {
            data: serialize(&rsp).unwrap(),
        });
        Ok(rsp)
    }
}
