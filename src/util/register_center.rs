use std::{env, sync::mpsc, thread, time::Duration};
use zookeeper::{Acl, CreateMode, WatchedEvent, WatchedEventType, Watcher, ZkError, ZooKeeper};

pub type HostAddr = (String, u16);

#[derive(Debug)]
pub enum HostAddrMsg {
    AddHost { addr: HostAddr },
    RemoveHost { addr: HostAddr },
}

fn zk_server_urls() -> String {
    let key = "ZOOKEEPER_SERVERS";
    match env::var(key) {
        Ok(val) => val,
        Err(_) => "127.0.0.1:2181".to_string(),
    }
}

fn parse_addr(s: &str) -> HostAddr {
    let dilli = s.find(':').unwrap();
    (
        s[0..dilli].to_string(),
        s[dilli + 1..].parse::<u16>().unwrap(),
    )
}

struct BasicWatcher;

impl Watcher for BasicWatcher {
    fn handle(&self, e: WatchedEvent) {
        println!("{:?}", e)
    }
}

struct LoggingWatcher {
    tx: mpsc::Sender<HostAddrMsg>,
    rtn_tx: mpsc::Sender<()>,
}

impl Watcher for LoggingWatcher {
    fn handle(&self, e: WatchedEvent) {
        match e.event_type {
            WatchedEventType::NodeCreated => {
                self.tx
                    .send(HostAddrMsg::AddHost {
                        addr: parse_addr(&e.path.unwrap()),
                    })
                    .unwrap();
            }
            WatchedEventType::NodeDeleted => self
                .tx
                .send(HostAddrMsg::RemoveHost {
                    addr: parse_addr(&e.path.unwrap()),
                })
                .unwrap(),
            
            _ => (),
        };
        self.rtn_tx.send(()).unwrap();
    }
}

pub fn register_w(me_addr: HostAddr, watch_dir: &'static str, host_ch: mpsc::Sender<HostAddrMsg>) {
    let zk_urls = zk_server_urls();
    let zk = ZooKeeper::connect(&zk_urls, Duration::from_secs(15), BasicWatcher).unwrap();
    if let Err(e) = zk.create(
        watch_dir,
        vec![],
        Acl::open_unsafe().clone(),
        CreateMode::Persistent,
    ) {
        if e != ZkError::NodeExists {
            panic!("{:?}", e);
        }
    }
    let me_path = format!("{}/{}:{}", watch_dir, me_addr.0, me_addr.1);
    zk.create(
        &me_path,
        vec![],
        Acl::open_unsafe().clone(),
        CreateMode::Ephemeral,
    )
    .unwrap();
    let childs = zk.get_children(watch_dir, false).unwrap();
    for child in childs {
        host_ch
            .send(HostAddrMsg::AddHost {
                addr: parse_addr(&child),
            })
            .unwrap();
    }
    thread::spawn(move || loop {
        let (c, p) = mpsc::channel();
        let watcher = LoggingWatcher {
            tx: host_ch.clone(),
            rtn_tx: c,
        };
        zk.get_children_w(watch_dir, watcher).unwrap();
        p.recv().unwrap();
    });
}

#[cfg(test)]
mod test {
    use std::{sync::mpsc::channel, thread::sleep};

    use super::*;

    #[test]
    fn test_register_center() {
        let mut hs = vec![];
        for i in 0..10 {
            hs.push(thread::spawn(move || {
                let (host_tx, host_rx) = channel();
                register_w(("127.0.0.1".to_string(), i), "/ttt", host_tx);
                while let Ok(host) = host_rx.recv() {
                    println!("{:?}", host);
                }
            }));
            sleep(Duration::from_secs(1));
        }
        for h in hs {
            h.join().unwrap();
        }
    }
}
