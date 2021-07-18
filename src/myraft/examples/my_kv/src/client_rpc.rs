use crate::clientpb::client_rpc_server::ClientRpc;
use crate::clientpb::{ReadRpcReq, ReadRpcRsp, WriteRpcReq, WriteRpcRsp};
use crate::kv_app::{KvApp, ReadRequest, WriteRequest, WriteResponse};
use anyhow::Result;
use log::info;
use myraft::async_trait::async_trait;
use myraft::raft::MyRaft;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::Code;
use tonic::{Request, Response, Status};

pub type MyKvRaft = MyRaft<KvApp>;

pub struct MyClientRpc {
    pub core: MyKvRaft,
    pub storage: Arc<RwLock<KvApp>>,
}

#[async_trait]
impl ClientRpc for MyClientRpc {
    async fn read(&self, request: Request<ReadRpcReq>) -> Result<Response<ReadRpcRsp>, Status> {
        let req = request.into_inner();
        let req = ReadRequest { key: req.id };
        info!("read: {:?}", req);
        match self.storage.read().await.handle_read(req).await {
            Ok(rsp) => {
                let rsp = ReadRpcRsp {
                    found: rsp.data.is_some(),
                    data: rsp.data.unwrap_or("".to_string()),
                };
                Ok(Response::new(rsp))
            }
            Err(_) => Err(Status::new(Code::Unknown, "")),
        }
    }

    async fn write(&self, request: Request<WriteRpcReq>) -> Result<Response<WriteRpcRsp>, Status> {
        let req = request.into_inner();
        let req = if req.kind == 0 {
            WriteRequest::Insert {
                key: req.key,
                value: req.data,
            }
        } else {
            WriteRequest::Remove { key: req.key }
        };
        info!("write: {:?}", req);
        match self.core.client_write(req).await {
            Ok(rsp) => {
                let rsp = match rsp {
                    WriteResponse::Insert { prev } => WriteRpcRsp {
                        kind: 0,
                        found: prev.is_some(),
                        prev: prev.unwrap_or("".to_string()),
                    },
                    WriteResponse::Remove { prev } => WriteRpcRsp {
                        kind: 1,
                        found: prev.is_some(),
                        prev: prev.unwrap_or("".to_string()),
                    },
                };
                Ok(Response::new(rsp))
            }
            Err(err) => Err(Status::new(
                Code::Unknown,
                format!("call core write error: {}", err),
            )),
        }
    }
}
