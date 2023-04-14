cargo build --release

/home/rafibayer/maelstrom/maelstrom test -w broadcast --bin target/release/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100