use log::{error, info};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    collections::{HashSet, VecDeque},
    usize,
};

const CHUNK_SIZE: u64 = 4 * 1024;

#[derive(Debug)]
pub struct ClientOp {
    pub cid: u64,
    pub offset: u64,
    pub size: u64,
    pub new: bool,
}

#[derive(Debug)]
pub struct TransOp {
    pub from_cid: u64,
    pub from_off: u64,
    pub to_cid: u64,
    pub to_off: u64,
    pub size: u64,
    pub new: bool,
}

#[derive(Debug)]
pub struct RemoveOp {
    pub cid: u64,
}

#[derive(Debug)]
pub struct MetaLog {
    remove: bool,
    new_chunks: Vec<Chunk>,
    old_start: usize,
    old_end: usize,
    new_fsize: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Chunk {
    cid: u64,
    size: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FileMeta {
    chunks: Vec<Chunk>,
    file_size: u64,
}

impl FileMeta {
    #[inline]
    fn gene_cid() -> u64 {
        rand::thread_rng().gen_range(0..u64::MAX)
    }

    fn remove_chunks(&self, meta_log: &MetaLog) -> Vec<RemoveOp> {
        let adds = meta_log.new_chunks.as_slice();
        let removes = self
            .chunks
            .get(meta_log.old_start..meta_log.old_end)
            .unwrap();
        let mut set = HashSet::new();
        for remove in removes {
            set.insert(remove.cid);
        }
        for add in adds {
            set.remove(&add.cid);
        }
        let mut ops = vec![];
        for cid in set {
            ops.push(RemoveOp { cid })
        }
        ops
    }

    pub fn new() -> Self {
        FileMeta {
            chunks: vec![],
            file_size: 0,
        }
    }

    pub fn get_size(&self) -> u64 {
        self.file_size
    }

    pub fn read(&self, offset: u64, mut read_size: u64) -> Vec<ClientOp> {
        if read_size == 0 {
            // return vec![];
            read_size = self.file_size;
        }
        let read_end = offset + read_size;
        let mut off = 0;
        let mut results = vec![];
        let mut start = false;
        for (_, Chunk { cid, size }) in self.chunks.iter().enumerate() {
            if *size == 0 {
                continue;
            }
            off += size;
            if !start && off >= offset {
                start = true;
                results.push(ClientOp {
                    cid: *cid,
                    offset: offset - (off - size),
                    size: min(off - offset, read_size),
                    new: false,
                });
            } else if start {
                results.push(ClientOp {
                    cid: *cid,
                    offset: 0,
                    size: size - max(off as i64 - read_end as i64, 0) as u64,
                    new: false,
                });
            }
            if off >= read_end {
                break;
            }
        }
        if off < read_end {
            error!("read too long");
        }
        return results;
    }

    pub fn write(
        &mut self,
        mut offset: u64,
        mut ins_size: u64,
        mut del_size: u64,
    ) -> Option<(Vec<ClientOp>, Vec<TransOp>, Vec<RemoveOp>, MetaLog)> {
        if ins_size == 0 && del_size == 0 {
            del_size = self.file_size;
            ins_size = offset;
            offset = 0;
            // return None;
        }
        if offset > self.file_size {
            error!("offset is too large");
            return None;
        }
        let del_end = offset + del_size;
        let ins_end = offset + ins_size;
        let mut unfilled_chunks = VecDeque::new();
        let mut off = 0;
        let mut client_ops = vec![];
        let mut trans_ops = vec![];
        let mut new_chunks = vec![];
        let mut last_info = None;
        let mut old_start = 0;
        let mut old_end = 0;
        let mut start = false;
        for (idx, Chunk { cid, size }) in self.chunks.iter().enumerate() {
            assert!(*size != 0 && *size <= CHUNK_SIZE);
            off += *size;
            if !start && off >= offset {
                start = true;
                old_start = idx;
                let used_size = offset - (off - *size);
                if used_size < CHUNK_SIZE {
                    unfilled_chunks.push_back(Chunk {
                        cid: *cid,
                        size: used_size,
                    });
                }
            }
            if start && off >= del_end {
                let trans_off = del_end - (off - *size);
                if trans_off < *size {
                    last_info = Some((*cid, trans_off, *size));
                }
                old_end = idx + 1;
                break;
            } else if start {
                unfilled_chunks.push_back(Chunk { cid: *cid, size: 0 });
            }
        }
        if off < del_end {
            error!("delete too long");
        }
        off = offset;
        while off < ins_end {
            if let Some(Chunk {
                cid: unfilled_cid,
                size: fill_off,
            }) = unfilled_chunks.front_mut()
            {
                let write_size = min(CHUNK_SIZE - *fill_off, ins_end - off);
                client_ops.push(ClientOp {
                    offset: *fill_off,
                    size: write_size,
                    cid: *unfilled_cid,
                    new: false,
                });
                *fill_off += write_size;
                off += write_size;
                if *fill_off == CHUNK_SIZE {
                    new_chunks.push(Chunk {
                        cid: unfilled_cid.clone(),
                        size: CHUNK_SIZE,
                    });
                    unfilled_chunks.pop_front();
                }
            } else {
                let new_cid = FileMeta::gene_cid();
                let write_size = min(CHUNK_SIZE, ins_end - off);
                client_ops.push(ClientOp {
                    cid: new_cid,
                    offset: 0,
                    size: write_size,
                    new: true,
                });
                if write_size < CHUNK_SIZE {
                    unfilled_chunks.push_back(Chunk {
                        cid: new_cid,
                        size: write_size,
                    });
                } else {
                    new_chunks.push(Chunk {
                        cid: new_cid,
                        size: write_size,
                    });
                }
                off += write_size;
            }
        }
        if let Some((last_cid, last_start, last_end)) = last_info {
            let mut trans_size = 0;
            let need_trans = last_end - last_start;
            if let Some(Chunk {
                cid: unfilled_cid,
                size: fill_off,
            }) = unfilled_chunks.pop_front()
            {
                trans_size = min(need_trans, CHUNK_SIZE - fill_off);
                trans_ops.push(TransOp {
                    from_cid: last_cid,
                    from_off: last_start,
                    to_cid: unfilled_cid,
                    to_off: fill_off,
                    size: trans_size,
                    new: false,
                });
                new_chunks.push(Chunk {
                    cid: unfilled_cid,
                    size: fill_off + trans_size,
                });
            }
            if trans_size < need_trans {
                let trans_size2 = need_trans - trans_size;
                let new_cid = FileMeta::gene_cid();
                trans_ops.push(TransOp {
                    from_cid: last_cid,
                    from_off: trans_size + last_start,
                    to_cid: new_cid,
                    to_off: 0,
                    size: trans_size2,
                    new: true,
                });
                new_chunks.push(Chunk {
                    cid: new_cid,
                    size: trans_size2,
                });
            }
        } else if let Some(Chunk { cid: _, size }) = unfilled_chunks.front() {
            if *size != 0 {
                new_chunks.push(unfilled_chunks.pop_front().unwrap());
            }
        }
        let old_fsize = self.file_size;
        let new_fsize = self.file_size - min(old_fsize - offset, del_size) + ins_size;
        let meta_log = MetaLog {
            remove: false,
            new_chunks,
            old_start,
            old_end,
            new_fsize,
        };
        let remove_ops = self.remove_chunks(&meta_log);
        Some((client_ops, trans_ops, remove_ops, meta_log))
    }

    pub fn compress(&mut self) -> Option<(Vec<TransOp>, Vec<RemoveOp>, MetaLog)> {
        let mut new_chunks = vec![];
        let mut trans_ops = vec![];
        let mut unfilled_chunks = VecDeque::new();
        let mut old_start = self.chunks.len();
        let mut start = false;
        for (idx, Chunk { cid, size }) in self.chunks.iter().enumerate() {
            if *size > CHUNK_SIZE || *size == 0 {
                panic!("chunk {} size is invalid {}", cid, *size);
            }
            if !start && *size < CHUNK_SIZE && idx + 1 != old_start {
                old_start = idx;
                start = true;
                unfilled_chunks.push_back(Chunk {
                    cid: *cid,
                    size: *size,
                });
            } else if start {
                unfilled_chunks.push_back(Chunk { cid: *cid, size: 0 });
                let Chunk {
                    cid: to_cids,
                    size: to_size,
                } = unfilled_chunks.front_mut().unwrap();
                let trans_size = min(CHUNK_SIZE - *to_size, *size);
                let after_size = *to_size + trans_size;
                trans_ops.push(TransOp {
                    from_cid: *cid,
                    from_off: 0,
                    to_cid: *to_cids,
                    to_off: *to_size,
                    size: trans_size,
                    new: false,
                });
                if after_size == CHUNK_SIZE {
                    new_chunks.push(Chunk {
                        cid: to_cids.clone(),
                        size: CHUNK_SIZE,
                    });
                    unfilled_chunks.pop_front().unwrap();
                } else {
                    *to_size = after_size;
                }
                if trans_size < *size {
                    let trans_size2 = *size - trans_size;
                    let Chunk {
                        cid: to_cid,
                        size: to_size,
                    } = unfilled_chunks.front_mut().unwrap();
                    assert!(*to_size == 0);
                    trans_ops.push(TransOp {
                        from_cid: *cid,
                        from_off: trans_size,
                        to_cid: *to_cid,
                        to_off: 0,
                        size: trans_size2,
                        new: false,
                    });
                    *to_size = trans_size2;
                }
            }
        }
        if let Some(Chunk { cid, size }) = unfilled_chunks.pop_front() {
            if size > 0 {
                new_chunks.push(Chunk { cid, size });
            }
        }
        let meta_log = MetaLog {
            remove: false,
            new_chunks,
            old_start,
            old_end: self.chunks.len(),
            new_fsize: self.file_size,
        };
        let remove_ops = self.remove_chunks(&meta_log);
        Some((trans_ops, remove_ops, meta_log))
    }

    pub fn remove(&self) -> Vec<RemoveOp> {
        let mut ops = vec![];
        for (_, Chunk { cid, size: _ }) in self.chunks.iter().enumerate() {
            ops.push(RemoveOp { cid: *cid });
        }
        ops
    }

    pub fn commit(&mut self, meta_log: MetaLog) {
        self.chunks
            .splice(meta_log.old_start..meta_log.old_end, meta_log.new_chunks);
        self.file_size = meta_log.new_fsize;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn print_chunks(file_meta: &FileMeta) -> u64 {
        let mut filesize = 0;
        for Chunk { cid, size } in &file_meta.chunks {
            filesize += size;
            println!("size: {}, cid: {}", size, cid);
        }
        filesize
    }

    fn init_chunks(c_sizes: Vec<u64>) -> Vec<Chunk> {
        let mut chunks = vec![];
        for (idx, size) in c_sizes.iter().enumerate() {
            chunks.push(Chunk {
                cid: idx as u64 + 1,
                size: *size,
            });
        }
        chunks
    }

    fn print_trans_ops(trans_ops: &[TransOp]) {
        for op in trans_ops {
            assert!(op.size != 0);
            println!(
                "size:{}, from_cid:{}, from_off:{}, to_cid:{}, to_off:{}",
                op.size, op.from_cid, op.from_off, op.to_cid, op.to_off,
            );
        }
    }

    fn print_client_op(client_ops: &[ClientOp]) -> u64 {
        let mut size = 0;
        for op in client_ops {
            assert!(op.size != 0);
            println!("size:{}, to_cid:{} , to_off:{}", op.size, op.cid, op.offset);
            size += op.size;
        }
        size
    }

    fn print_remove_op(remove_ops: &[RemoveOp]) {
        for op in remove_ops {
            println!("remove_cid:{}", op.cid);
        }
    }

    fn read_test(file_meta: &FileMeta, offsize: u64, read_size: u64) {
        println!("start read test!");
        let client_ops = file_meta.read(offsize, read_size);
        println!("client op:");
        let read_size1 = print_client_op(&client_ops);
        println!("read size: {}", read_size1);
        assert!(read_size == read_size1);
        println!("");
    }

    fn write_test(file_meta: &mut FileMeta, offset: u64, ins_size: u64, del_size: u64) {
        println!("write test start!");
        println!("offset:{}, ins:{}, del:{}", offset, ins_size, del_size);
        println!("origin:");
        let old_size = print_chunks(file_meta);

        let (client_ops, trans_ops, remove_ops, meta_log) =
            file_meta.write(offset, ins_size, del_size).unwrap();

        println!("trans op:");
        print_trans_ops(&trans_ops);
        println!("client op:");
        let write_size = print_client_op(&client_ops);
        println!("remove op:");
        print_remove_op(&remove_ops);
        println!("write size:{}", write_size);
        assert!(write_size == ins_size);
        file_meta.commit(meta_log);
        println!("after write commit:");
        let new_size = print_chunks(file_meta);
        println!("new size:{}, old size: {}", new_size, old_size);
        assert!(old_size + ins_size - del_size == new_size);
        assert!(file_meta.file_size == new_size);
        println!("--------------------------------------");
    }

    fn compress_test(file_meta: &mut FileMeta) {
        println!("start compress test!");
        println!("origin:");
        let old_size = print_chunks(file_meta);

        let (trans_ops, remove_ops, meta_log) = file_meta.compress().unwrap();

        println!("trans op:");
        print_trans_ops(&trans_ops);
        println!("remove op:");
        print_remove_op(&remove_ops);
        println!("after compress:");
        let new_size = print_chunks(file_meta);
        println!("new size {}, old size {}", new_size, old_size);
        assert!(old_size == new_size);

        let (trans_ops, remove_ops, _) = file_meta.compress().unwrap();

        println!("trans op:");
        print_trans_ops(&trans_ops);
        println!("remove op:");
        print_remove_op(&remove_ops);
        file_meta.commit(meta_log);
        println!("after compress commit:");
        let new_size2 = print_chunks(file_meta);
        println!("new size {}, old size {}", new_size2, new_size);
        assert!(new_size2 == new_size);
        println!("--------------------------------------");
    }

    fn compute_size(chunks: &Vec<Chunk>) -> u64 {
        let mut size = 0;
        for chunk in chunks {
            size += chunk.size;
        }
        size
    }

    #[test]
    fn basic_read_test() {
        let mut file_meta = FileMeta::new();
        let c_sizes = vec![4096, 4000, 4000, 100];
        let chunks = init_chunks(c_sizes);
        let fsize = compute_size(&chunks);
        file_meta.chunks = chunks;
        file_meta.file_size = fsize;
        read_test(&file_meta, 20, 10000);
        read_test(&file_meta, 2000, 1000);
    }

    #[test]
    fn basic_write_test() {
        let mut file_meta = FileMeta::new();
        let c_sizes = vec![4096, 4096, 4000, 4000, 4000, 4000, 4000, 4096, 4096, 100];
        let chunks = init_chunks(c_sizes);
        let fsize = compute_size(&chunks);
        file_meta.chunks = chunks;
        file_meta.file_size = fsize;
        write_test(&mut file_meta, 0, 1000, 0);
        write_test(&mut file_meta, 0, 0, 1000);
        write_test(&mut file_meta, 0, 2000, 1000);
        write_test(&mut file_meta, 0, 1000, 2000);
        write_test(&mut file_meta, 0, 10000, 0);
        write_test(&mut file_meta, 0, 0, 10000);
        write_test(&mut file_meta, 0, 20000, 10000);
        write_test(&mut file_meta, 0, 10000, 20000);

        write_test(&mut file_meta, 1000, 1000, 0);
        write_test(&mut file_meta, 2000, 0, 1000);
        write_test(&mut file_meta, 3000, 2000, 1000);
        write_test(&mut file_meta, 4000, 1000, 2000);
        write_test(&mut file_meta, 5000, 10000, 0);
        write_test(&mut file_meta, 6000, 0, 10000);
        write_test(&mut file_meta, 7000, 20000, 10000);
        write_test(&mut file_meta, 8000, 10000, 20000);
    }

    #[test]
    fn basic_compress_test() {
        let mut file_meta = FileMeta::new();
        let c_sizes = vec![4096, 4000, 96, 3900, 100, 96];
        let chunks = init_chunks(c_sizes);
        let fsize = compute_size(&chunks);
        file_meta.chunks = chunks;
        file_meta.file_size = fsize;
        compress_test(&mut file_meta);
    }
}
