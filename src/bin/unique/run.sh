cargo build --release

/home/rafibayer/maelstrom/maelstrom test -w unique-ids --bin target/release/unique --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition