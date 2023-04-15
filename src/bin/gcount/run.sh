cargo build --release

/home/rafibayer/maelstrom/maelstrom test -w g-counter --bin target/release/gcount --node-count 10 --rate 200 --time-limit 60 --nemesis partition