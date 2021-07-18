RUST_LOG=info cargo run --bin worker -- --node-id=1 --raft-addr=127.0.0.1:11111 --out-addr=127.0.0.1:11112 --cluster-id=1 --as-init=true
RUST_LOG=info cargo run --bin worker -- --node-id=2 --raft-addr=127.0.0.1:22222 --out-addr=127.0.0.1:22223 --cluster-id=1
RUST_LOG=info cargo run --bin worker -- --node-id=3 --raft-addr=127.0.0.1:33333 --out-addr=127.0.0.1:33334 --cluster-id=1

RUST_LOG=info cargo run --bin worker -- --node-id=4 --raft-addr=127.0.0.1:44444 --out-addr=127.0.0.1:44445 --cluster-id=2 --as-init=true
RUST_LOG=info cargo run --bin worker -- --node-id=5 --raft-addr=127.0.0.1:55555 --out-addr=127.0.0.1:55556 --cluster-id=2
RUST_LOG=info cargo run --bin worker -- --node-id=6 --raft-addr=127.0.0.1:55666 --out-addr=127.0.0.1:55667 --cluster-id=2

RUST_LOG=info cargo run --bin master

RUST_LOG=info cargo run --bin client