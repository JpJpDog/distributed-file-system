#![feature(iter_map_while)]
// #![feature(async_closure)]

mod error;
mod file_meta;
mod meta_app;
mod meta_rpc;
mod metapb {
    tonic::include_proto!("metapb");
}
mod datapb {
    tonic::include_proto!("datapb");
}

use crate::{meta_rpc::MyMetaRpc, metapb::meta_rpc_server::MetaRpcServer};
use tonic::transport::Server;

#[tokio::main]
async fn main() {
    env_logger::init();
    let store_path = "store_meta";
    let meta_path = format!("{}/meta", store_path);
    let map_path = format!("{}/map", store_path);
    let meta_rpc = MyMetaRpc::new(meta_path, map_path);
    let addr = "127.0.0.1:12345".parse().unwrap();
    Server::builder()
        .add_service(MetaRpcServer::new(meta_rpc))
        .serve(addr)
        .await
        .unwrap();
}
