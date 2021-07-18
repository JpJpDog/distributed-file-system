mod clientpb {
    tonic::include_proto!("clientpb");
}
mod client_rpc;
mod kv_app;
use crate::client_rpc::{MyClientRpc, MyKvRaft};
use crate::kv_app::KvApp;
use anyhow::Result;
use clientpb::client_rpc_server::ClientRpcServer;
use log::info;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::spawn;
use tokio::sync::RwLock;
use tonic::transport::Server;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long)]
    id: u64,
    #[structopt(short, long)]
    raft_addr: String,
    #[structopt(short, long)]
    group_id: u64,
    #[structopt(short, long)]
    client_addr: Option<String>,
    #[structopt(short, long)]
    as_init: bool,
}

async fn start_client_service(
    raft: MyKvRaft,
    sm: Arc<RwLock<KvApp>>,
    client_addr: String,
) -> Result<()> {
    let client_rpc = MyClientRpc {
        core: raft,
        storage: sm,
    };
    let client_addr = client_addr.parse().unwrap();
    info!("listenning client addr: {:?}", client_addr);
    spawn(async move {
        Server::builder()
            .add_service(ClientRpcServer::new(client_rpc))
            .serve(client_addr)
            .await
            .unwrap();
    })
    .await
    .unwrap();
    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let opt = Opt::from_args();
    let kv_path = format!("kv_store/node_{}", opt.id.to_string());
    let kv_app = KvApp {
        db: sled::open(kv_path).unwrap(),
    };
    let kv_app = Arc::new(RwLock::new(kv_app));
    let my_raft = MyKvRaft::new(opt.id, opt.raft_addr, kv_app.clone()).await;
    my_raft.join_cluster(opt.group_id, opt.as_init).await;
    if let Some(client_addr) = opt.client_addr {
        start_client_service(my_raft, kv_app, client_addr)
            .await
            .unwrap();
    }
}
