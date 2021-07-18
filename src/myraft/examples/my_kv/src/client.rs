mod clientpb {
    tonic::include_proto!("clientpb");
}
use log::debug;
use clientpb::{client_rpc_client::ClientRpcClient, ReadRpcReq, WriteRpcReq};
use structopt::StructOpt;
use tonic::Request;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long)]
    client_addr: String,
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    debug!("connecting to {}", opt.client_addr);
    let mut client = ClientRpcClient::connect(opt.client_addr).await.unwrap();

    let req = Request::new(WriteRpcReq {
        kind: 0,
        key: 1,
        data: "ccc".to_string(),
    });
    let rsp = client.write(req).await.unwrap();
    println!("{:?}", rsp.into_inner());

    let req = Request::new(ReadRpcReq { id: 1 });
    let rsp = client.read(req).await.unwrap();
    println!("{:?}", rsp.into_inner());
}

