#![feature(iter_map_while)]

mod data_app;
mod data_rpc;
mod error;
mod datapb {
    tonic::include_proto!("datapb");
}

use crate::datapb::data_rpc_server::DataRpcServer;
use crate::{data_app::DataRaftApp, data_rpc::MyDataRpc};
use log::info;
use myraft::raft::MyRaft;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::RwLock;
use tonic::transport::Server;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long)]
    node_id: u64,
    #[structopt(short, long)]
    cluster_id: u64,
    #[structopt(short, long)]
    raft_addr: String,
    #[structopt(short, long)]
    out_addr: String,
    #[structopt(short, long)]
    as_init: bool,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let opt = Opt::from_args();
    let store_path = format!("store_data/node_{}", opt.node_id);
    let db_path = format!("{}/db", store_path);
    let file_path = format!("{}/file", store_path);
    let data_app = DataRaftApp::new(db_path, file_path, format!("http://{}", opt.out_addr)).await;
    let data_app = Arc::new(RwLock::new(data_app));
    let data_raft = MyRaft::<DataRaftApp>::new(opt.node_id, opt.raft_addr, data_app.clone()).await;
    data_raft.join_cluster(opt.cluster_id, opt.as_init).await;
    let out_addr = opt.out_addr.parse().unwrap();
    let data_rpc = MyDataRpc::new(data_app, data_raft);
    info!("listening out addr {:?}", out_addr);
    Server::builder()
        .add_service(DataRpcServer::new(data_rpc))
        .serve(out_addr)
        .await
        .unwrap();
}
