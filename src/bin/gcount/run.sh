cargo build --release

/home/rafibayer/maelstrom/maelstrom test -w g-counter --bin target/release/gcount --node-count 1 --rate 100 --time-limit 20

 # --nemesis partition