use crate::meta_app::MetaRaftApp;
use crate::metapb::meta_rpc_server::MetaRpc;
use crate::metapb::{
    CreateReq, CreateRsp, ReadReq, ReadRsp, RemoveReq, RemoveRsp, WriteReq, WriteRsp,
};
use log::info;
use myraft::async_trait::async_trait;
use tonic::{Code, Request, Response, Status};

pub struct MyMetaRpc {
    app: MetaRaftApp,
}

impl MyMetaRpc {
    pub fn new(meta_path: String, map_path: String) -> Self {
        Self {
            app: MetaRaftApp::new(meta_path, map_path),
        }
    }
}

#[async_trait]
impl MetaRpc for MyMetaRpc {
    async fn create(&self, request: Request<CreateReq>) -> Result<Response<CreateRsp>, Status> {
        info!("create rev");
        match self.app.create(request.into_inner()).await {
            Ok(rsp) => Ok(Response::new(rsp)),
            Err(err) => Err(Status::new(Code::Unknown, err.to_string())),
        }
    }

    async fn read(&self, request: Request<ReadReq>) -> Result<Response<ReadRsp>, Status> {
        info!("read rev");
        match self.app.read(request.into_inner()).await {
            Ok(rsp) => Ok(Response::new(rsp)),
            Err(err) => Err(Status::new(Code::Unknown, err.to_string())),
        }
    }

    async fn write(&self, request: Request<WriteReq>) -> Result<Response<WriteRsp>, Status> {
        info!("write rev");
        match self.app.write(request.into_inner()).await {
            Ok(rsp) => Ok(Response::new(rsp)),
            Err(err) => Err(Status::new(Code::Unknown, err.inner)),
        }
    }

    async fn remove(&self, request: Request<RemoveReq>) -> Result<Response<RemoveRsp>, Status> {
        info!("remove rev");
        match self.app.remove(request.into_inner()).await {
            Ok(rsp) => Ok(Response::new(rsp)),
            Err(err) => Err(Status::new(Code::Unknown, err.inner)),
        }
    }
}
