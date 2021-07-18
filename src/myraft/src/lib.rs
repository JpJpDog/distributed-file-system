mod network;
pub mod raft;
mod storage;
mod raftpb {
    tonic::include_proto!("raftpb");
}

pub use async_raft::async_trait;
// pub use async_raft::raft::ClientWriteRequest;
pub use async_raft::{AppData, AppDataResponse};
