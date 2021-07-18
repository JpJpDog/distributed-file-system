use anyhow::Result;
use async_raft::raft::{Entry, EntryPayload, MembershipConfig};
use async_raft::storage::{CurrentSnapshotData, HardState, InitialState};
use async_raft::RaftStorage;
use async_raft::{async_trait::async_trait, NodeId};
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use sled::{Db, IVec, Tree};
use std::io::Cursor;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::raft::RaftApp;

const STORE_DIR: &str = "store_raft";
const ERR_INCONSISTENT_LOG: &str =
    "a query was received which was expecting data to be in place which does not exist in the log";

#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    // #[error("unsafe storage error")]
// UnsafeStorageError,
}

#[derive(Serialize, Deserialize)]
struct MyStorageSnapshot {
    index: u64,
    term: u64,
    membership: MembershipConfig,
    data: Vec<u8>,
}

struct MyStorageState {
    last_applied_log: Vec<u8>,
    hs: Vec<u8>,
    current_snapshot: Vec<u8>,
    log_tree: String,
    state_tree: String,
    db: Db,
}

impl MyStorageState {
    fn new(state_path: &str) -> Result<Self> {
        let last_applied_log = "last_applied_log".as_bytes().to_vec();
        let hs = "hs".as_bytes().to_vec();
        let current_snapshot = "current_snapshot".as_bytes().to_vec();
        let log_tree_name = "log".to_string();
        let state_tree_name = "state".to_string();
        let db = sled::open(state_path)?;
        db.open_tree(&log_tree_name)?;
        let state_tree = db.open_tree(&state_tree_name)?;
        if state_tree.get(&last_applied_log)?.is_none() {
            state_tree.insert(&last_applied_log, &0u64.to_ne_bytes())?;
        }
        if state_tree.get(&hs)?.is_none() {
            let hs_none: Option<HardState> = None;
            state_tree.insert(&hs, serialize(&hs_none)?)?;
        }
        if state_tree.get(&current_snapshot)?.is_none() {
            let snap_none: Option<MyStorageSnapshot> = None;
            state_tree.insert(&current_snapshot, serialize(&snap_none)?)?;
        }
        Ok(Self {
            last_applied_log,
            hs,
            current_snapshot,
            log_tree: log_tree_name,
            state_tree: state_tree_name,
            db,
        })
    }

    #[inline]
    fn get_log_tree(&self) -> Result<Tree> {
        Ok(self.db.open_tree(&self.log_tree)?)
    }

    #[inline]
    fn get_hs(&self) -> Result<Option<HardState>> {
        let hs = self.db.open_tree(&self.state_tree)?.get(&self.hs)?.unwrap();
        Ok(deserialize(&hs)?)
    }

    #[inline]
    fn set_hs(&mut self, hs: HardState) -> Result<()> {
        let state_tree = self.db.open_tree(&self.state_tree)?;
        state_tree.insert(&self.hs, serialize(&Some(hs))?)?;
        Ok(())
    }

    #[inline]
    fn get_last_applied_log(&self) -> Result<u64> {
        let l = self
            .db
            .open_tree(&self.state_tree)?
            .get(&self.last_applied_log)?
            .unwrap();
        Ok(deserialize(&l)?)
    }

    #[inline]
    fn set_last_applied_log(&self, l: u64) -> Result<()> {
        self.db
            .open_tree(&self.state_tree)?
            .insert(&self.last_applied_log, serialize(&l)?)?;
        Ok(())
    }

    #[inline]
    fn get_current_snapshot(&self) -> Result<Option<MyStorageSnapshot>> {
        let snap = self
            .db
            .open_tree(&self.state_tree)?
            .get(&self.current_snapshot)?
            .unwrap();
        Ok(deserialize(&snap)?)
    }

    #[inline]
    fn set_current_snapshot(&mut self, snap: Option<MyStorageSnapshot>) -> Result<()> {
        self.db
            .open_tree(&self.state_tree)?
            .insert(&self.current_snapshot, serialize(&snap)?)?;
        todo!()
    }
}

pub struct MyRaftStorage<T: RaftApp> {
    id: NodeId,
    state: RwLock<MyStorageState>,
    sm: Arc<RwLock<T>>,
}

impl<T: RaftApp> MyRaftStorage<T> {
    pub fn new(id: NodeId, sm: Arc<RwLock<T>>) -> Self {
        let node_dir = format!("{}/node_{}", STORE_DIR, &id.to_string());
        let state_path = format!("{}/state", &node_dir);
        Self {
            id,
            state: RwLock::new(MyStorageState::new(&state_path).unwrap()),
            sm,
        }
    }

    // get the last applied memconfig request or get init one
    fn get_last_applied_membership_config(
        &self,
        log: &Tree,
        last_applied_log: u64,
    ) -> MembershipConfig {
        log.iter()
            .rev()
            .skip_while(|kv| {
                let (_, entry) =
                    MyRaftStorage::<T>::decode_log_entry(&kv.as_ref().unwrap()).unwrap();
                entry.index > last_applied_log
            })
            .find_map(|kv| {
                let (_, entry) = MyRaftStorage::<T>::decode_log_entry(&kv.unwrap()).unwrap();
                match entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                }
            })
            .unwrap_or_else(|| MembershipConfig::new_initial(self.id))
    }

    fn get_last_membership_config(&self, log: &Tree) -> MembershipConfig {
        let cfg_opt = log.iter().rev().find_map(|kv| {
            let (_, entry) = MyRaftStorage::<T>::decode_log_entry(&kv.ok()?).ok()?;
            match entry.payload {
                EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
                _ => None,
            }
        });
        match cfg_opt {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.id),
        }
    }

    #[inline]
    fn decode_log_entry(kv: &(IVec, IVec)) -> Result<(u64, Entry<T::WriteReq>)> {
        Ok((deserialize(&kv.0)?, deserialize(&kv.1)?))
    }
}

#[async_trait]
impl<T: RaftApp> RaftStorage<T::WriteReq, T::WriteRsp>
    for MyRaftStorage<T>
{
    type Snapshot = Cursor<Vec<u8>>;

    type ShutdownError = ShutdownError;

    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        let log = self.state.read().await.get_log_tree()?;
        Ok(self.get_last_membership_config(&log))
    }

    async fn get_initial_state(&self) -> Result<InitialState> {
        let mut state = self.state.write().await;
        let hs = state.get_hs()?;
        match hs {
            Some(hs) => {
                let log = state.get_log_tree()?;
                let membership = self.get_last_membership_config(&log);
                let log = state.get_log_tree()?;
                let (last_log_index, last_log_term) = match log.iter().rev().next() {
                    Some(kv) => {
                        let (_, entry) = MyRaftStorage::<T>::decode_log_entry(&kv?)?;
                        (entry.index, entry.term)
                    }
                    None => (0, 0),
                };
                let last_applied_log = state.get_last_applied_log()?;
                Ok(InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: hs.clone(),
                    membership,
                })
            }
            None => {
                let new = InitialState::new_initial(self.id);
                state.set_hs(new.hard_state.clone())?;
                Ok(new)
            }
        }
    }

    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        let mut state = self.state.write().await;
        state.set_hs(hs.clone())
    }

    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<T::WriteReq>>> {
        if start > stop {
            // TODO: log the error
            return Ok(vec![]);
        }
        let log = self.state.read().await.get_log_tree()?;
        let range = IVec::from(serialize(&start)?)..IVec::from(serialize(&stop)?);
        log.range(range)
            .map(|kv| {
                let (_, entry) = MyRaftStorage::<T>::decode_log_entry(&kv?)?;
                Ok(entry.clone())
            })
            .collect()
    }

    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> anyhow::Result<()> {
        let log = self.state.write().await.get_log_tree()?;
        let stop = match stop {
            Some(stop) => stop,
            None => match log.last()? {
                Some(kv) => MyRaftStorage::<T>::decode_log_entry(&kv)?.0 + 1,
                None => start,
            },
        };
        for key in start..stop {
            log.remove(serialize(&key)?)?;
        }
        Ok(())
    }

    async fn append_entry_to_log(&self, entry: &Entry<T::WriteReq>) -> anyhow::Result<()> {
        let log = self.state.write().await.get_log_tree()?;
        log.insert(serialize(&entry.index)?, serialize(entry)?)?;
        Ok(())
    }

    async fn replicate_to_log(&self, entries: &[Entry<T::WriteReq>]) -> anyhow::Result<()> {
        let log = self.state.write().await.get_log_tree()?;
        for entry in entries {
            log.insert(serialize(&entry.index)?, serialize(entry)?)?;
        }
        Ok(())
    }

    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &T::WriteReq,
    ) -> Result<T::WriteRsp> {
        let mut sm = self.sm.write().await;
        let rsp = sm.handle_write(data.clone()).await?;
        let state = self.state.write().await;
        state.set_last_applied_log(*index)?;
        Ok(rsp)
    }

    async fn replicate_to_state_machine(&self, entries: &[(&u64, &T::WriteReq)]) -> Result<()> {
        // let mut sm = self.sm.write().await;
        let mut sm = self.sm.write().await;
        let state = self.state.write().await;
        for (index, entry) in entries {
            sm.handle_write((*entry).clone()).await?;
            state.set_last_applied_log(**index)?;
        }
        Ok(())
    }

    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        //block the state
        let mut state = self.state.write().await;
        let sm = self.sm.read().await;
        let data = sm.make_snapshot().await?;
        let last_applied_log = state.get_last_applied_log()?;
        let log = state.get_log_tree()?;

        let membership = self.get_last_applied_membership_config(&log, last_applied_log);

        let term = log
            .get(serialize(&last_applied_log)?)?
            .map(|entry| {
                let entry: Entry<T::WriteReq> = deserialize(&entry).unwrap();
                entry.term
            })
            .ok_or_else(|| anyhow::anyhow!(ERR_INCONSISTENT_LOG))?;

        let snapshot = MyStorageSnapshot {
            index: last_applied_log,
            term,
            membership: membership.clone(),
            data,
        };
        let snapshot_bytes = serialize(&snapshot)?;
        state.set_current_snapshot(Some(snapshot))?;

        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: membership,
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        })
    }

    async fn create_snapshot(&self) -> anyhow::Result<(String, Box<Self::Snapshot>)> {
        Ok((String::from(""), Box::new(Cursor::new(Vec::new()))))
    }

    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> anyhow::Result<()> {
        {
            let log = self.state.read().await.get_log_tree()?;
            let membership = self.get_last_applied_membership_config(&log, index);
            match delete_through {
                Some(through) => {
                    for (_, entry) in log.iter().enumerate() {
                        let key = entry?.0;
                        let index: u64 = deserialize(&key)?;
                        if index > through {
                            break;
                        }
                        log.remove(&key)?;
                    }
                }
                None => log.clear()?,
            }
            let snap_entry: Entry<T::WriteReq> =
                Entry::new_snapshot_pointer(index, term, id, membership);
            log.insert(serialize(&index)?, serialize(&snap_entry)?)?;
        }
        let new_snapshot: MyStorageSnapshot = deserialize(snapshot.get_ref())?;
        {
            let sm = self.sm.write().await;
            sm.handle_snapshot(&new_snapshot.data).await?;
        }

        let mut state = self.state.write().await;
        state.set_current_snapshot(Some(new_snapshot))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &self,
    ) -> anyhow::Result<Option<async_raft::storage::CurrentSnapshotData<Self::Snapshot>>> {
        let state = self.state.read().await;
        match state.get_current_snapshot()? {
            Some(snapshot) => {
                let reader = serialize(&snapshot)?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(reader)),
                }))
            }
            None => Ok(None),
        }
    }
}
