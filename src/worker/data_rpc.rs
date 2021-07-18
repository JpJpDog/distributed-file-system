use crate::data_app::{DataRaftApp, TransOp, WriteRequest, WriteResponse};
use crate::datapb::data_rpc_server::DataRpc;
use crate::datapb::{
    CommitDataReq, CommitDataRsp, ReadDataReq, ReadDataRsp, RemoveDataReq, RemoveDataRsp,
    TransferReq, TransferRsp,
};
use core::panic;
// use log::{info, warn};
use myraft::async_trait::async_trait;
use myraft::raft::MyRaft;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Code, Request, Response, Status};

pub struct MyDataRpc {
    app: Arc<RwLock<DataRaftApp>>,
    core: MyRaft<DataRaftApp>,
}

impl MyDataRpc {
    pub fn new(app: Arc<RwLock<DataRaftApp>>, core: MyRaft<DataRaftApp>) -> Self {
        Self { app, core }
    }
}

#[async_trait]
impl DataRpc for MyDataRpc {
    async fn read(&self, request: Request<ReadDataReq>) -> Result<Response<ReadDataRsp>, Status> {
        match self
            .app
            .read()
            .await
            .handle_read(request.into_inner())
            .await
        {
            Ok(rsp) => Ok(Response::new(rsp)),
            Err(err) => Err(Status::new(Code::Unknown, err.to_string())),
        }
    }

    async fn remove(
        &self,
        request: Request<RemoveDataReq>,
    ) -> Result<Response<RemoveDataRsp>, Status> {
        let req = request.into_inner();
        let req = WriteRequest::Remove {
            cids: req.cids,
            lid: req.lid,
        };
        match self.core.client_write(req).await {
            Ok(rsp) => {
                let rsp = match rsp {
                    WriteResponse::Remove { ok } => RemoveDataRsp { ok },
                    _ => panic!(),
                };
                Ok(Response::new(rsp))
            }
            Err(err) => Err(Status::new(Code::Unknown, err.to_string())),
        }
    }

    async fn commit(
        &self,
        request: Request<CommitDataReq>,
    ) -> Result<Response<CommitDataRsp>, Status> {
        let req = request.into_inner();
        let req = WriteRequest::Commit { lid: req.lid };
        match self.core.client_write(req).await {
            Ok(rsp) => {
                let rsp = match rsp {
                    WriteResponse::Commit { ok } => CommitDataRsp { ok },
                    _ => panic!(),
                };
                Ok(Response::new(rsp))
            }
            Err(_) => todo!(),
        }
    }

    async fn transfer(
        &self,
        request: Request<TransferReq>,
    ) -> Result<Response<TransferRsp>, Status> {
        let req = request.into_inner();
        let req = WriteRequest::Transfer {
            ops: req
                .ops
                .into_iter()
                .map(|op| TransOp {
                    from_addr: op.from_addr,
                    to_cid: op.to_cid,
                    to_offset: op.to_offset,
                    from_cid: op.from_cid,
                    from_offset: op.from_offset,
                    size: op.size,
                })
                .collect(),
            lid: req.lid,
        };
        match self.core.client_write(req).await {
            Ok(rsp) => {
                let rsp = match rsp {
                    WriteResponse::Transfer { ok } => TransferRsp { ok },
                    _ => panic!(),
                };
                Ok(Response::new(rsp))
            }
            Err(err) => Err(Status::new(Code::Unknown, err.to_string())),
        }
    }
}
