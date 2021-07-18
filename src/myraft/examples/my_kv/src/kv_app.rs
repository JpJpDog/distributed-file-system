use anyhow::Result;
use bincode::{deserialize, serialize};
use myraft::{async_trait::async_trait, raft::RaftApp, AppData, AppDataResponse};
use serde::{Deserialize, Serialize};
use sled::Db;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReadRequest {
    pub key: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReadResponse {
    pub data: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteRequest {
    Insert { key: u64, value: String },
    Remove { key: u64 },
}

impl AppData for WriteRequest {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteResponse {
    Insert { prev: Option<String> },
    Remove { prev: Option<String> },
}

impl AppDataResponse for WriteResponse {}

pub struct KvApp {
    pub db: Db,
}

impl KvApp {
    pub async fn handle_read(&self, req: ReadRequest) -> Result<ReadResponse> {
        let rsp = self.db.get(&serialize(&req.key)?)?;
        let rsp = match rsp {
            Some(rsp) => Some(deserialize(&rsp)?),
            None => None,
        };
        Ok(ReadResponse { data: rsp })
    }
}

#[async_trait]
impl RaftApp for KvApp {
    async fn handle_write(&mut self, req: WriteRequest) -> Result<WriteResponse> {
        match req {
            WriteRequest::Insert { key, value } => {
                let prev = self.db.insert(serialize(&key)?, serialize(&value)?)?;
                self.db.flush_async().await?;
                Ok(WriteResponse::Insert {
                    prev: match prev {
                        Some(prev) => Some(deserialize(&prev)?),
                        None => None,
                    },
                })
            }
            WriteRequest::Remove { key } => {
                let prev = self.db.remove(serialize(&key)?)?;
                self.db.flush_async().await?;
                Ok(WriteResponse::Remove {
                    prev: match prev {
                        Some(prev) => Some(deserialize(&prev)?),
                        None => None,
                    },
                })
            }
        }
    }

    async fn make_snapshot(&self) -> Result<Vec<u8>> {
        let mut map = HashMap::new();
        for kv in self.db.into_iter() {
            let (k, v) = kv?;
            map.insert(k.to_vec(), v.to_vec());
        }
        Ok(serialize(&map)?)
    }

    async fn handle_snapshot(&self, snap: &Vec<u8>) -> Result<()> {
        let map: HashMap<Vec<u8>, Vec<u8>> = deserialize(snap)?;
        for (k, v) in map {
            self.db.insert(k, v)?;
        }
        self.db.flush_async().await?;
        Ok(())
    }

    type WriteReq = WriteRequest;
    type WriteRsp = WriteResponse;
}