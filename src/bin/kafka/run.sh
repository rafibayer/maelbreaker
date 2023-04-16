cargo build --release

/home/rafibayer/maelstrom/maelstrom test -w kafka --bin target/release/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
