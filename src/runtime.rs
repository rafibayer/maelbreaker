use std::{
    io::{stdin, stdout, BufRead, Write},
    sync::mpsc::{channel, Receiver, Sender},
    thread,
};

const EOI: &str = "EOI";

use crate::{
    node::Node,
    types::{Init, Message, Payload, SyncTry, Try},
};

pub fn run<P, N>() -> Try
where
    P: Payload,
    N: Node<P>,
{
    let (stdin_tx, stdin_rx) = channel();
    let (stdout_tx, stdout_rx) = channel();

    let stdin_handle = thread::spawn(move || {
        let stdin = stdin().lock().lines();

        for line in stdin {
            let line = line.unwrap();
            stdin_tx.send(line).unwrap();
        }
    });

    let stdout_handle = thread::spawn(move || {
        let mut stdout = stdout().lock();

        for message in stdout_rx {
            writeln!(&mut stdout, "{message}").unwrap();
        }
    });

    run_internal::<P, N>(stdout_tx, stdin_rx)?;

    stdin_handle.join().unwrap();
    stdout_handle.join().unwrap();

    Ok(())
}

fn run_internal<P, N>(tx: Sender<String>, rx: Receiver<String>) -> Try
where
    P: Payload,
    N: Node<P>,
{
    eprintln!("Starting runtime...\nWaiting for init message");
    let init = &rx.recv()?;
    eprintln!("Got init: {init}");
    let init: Message<Init> = serde_json::from_str(init)?;
    let Init::Init { node_id, node_ids } = &init.body.payload else {
        return Err("expected init as first message")?;
    };

    let (node_sender, node_receiver) = channel();
    let mut node = N::from_init(node_sender, node_id.clone(), node_ids.clone());

    // we are using a msg_id here that might be used by the node,
    // which is against protocol, but maelstrom doesn't seem to mind
    let reply = init.into_reply(Init::InitOk);

    eprintln!("Starting outbound processing and sending init_ok");
    let outbound = thread::spawn::<_, SyncTry>(move || {
        // send the init_ok
        let mut json = serde_json::to_string(&reply)?;
        eprintln!("Writing init_ok: {json}");
        tx.send(json)?;

        // reply to other messages
        loop {
            let outbound = node_receiver.recv()?;
            json = serde_json::to_string(&outbound)?;
            eprintln!("Writing outbound message: {json}");
            tx.send(json)?;
        }
    });

    eprintln!("Starting inbound processing");
    process_input(rx, &mut node)?;

    eprintln!("Shutting down...");
    cleanup(node, outbound)
}

fn process_input<P, N>(rx: Receiver<String>, node: &mut N) -> Try
where
    P: Payload,
    N: Node<P>,
{
    for line in rx {
        if line == EOI {
            eprintln!("Got EOI");

            break;
        }

        eprintln!("Got message: {line}");
        let message: Message<P> = serde_json::from_str(&line)?;
        node.handle_message(message)?;
    }

    Ok(())
}

fn cleanup<P, N>(node: N, outbound: thread::JoinHandle<SyncTry>) -> Try
where
    P: Payload,
    N: Node<P>,
{
    drop(node);
    outbound
        .join()
        .map_err(|e| format!("outbound thread error: {e:#?}"))?
        .map_err(|_| "error processing error :~)")?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::{error::Error, sync::mpsc::Sender, thread::JoinHandle};

    use crate::{payload, types::Body};

    use super::*;

    payload!(
        enum Payload {
            Echo { echo: String },
            EchoOk { echo: String },
        }
    );

    struct EchoNode {
        network: Sender<Message<Payload>>,
        seq: usize,
    }

    impl Node<Payload> for EchoNode {
        fn from_init(
            network: Sender<Message<Payload>>,
            _node_id: String,
            _node_ids: Vec<String>,
        ) -> Self {
            EchoNode { network, seq: 0 }
        }

        fn handle_message(&mut self, msg: Message<Payload>) -> Try {
            let Payload::Echo { echo } = &msg.body.payload else {
                return Err("expected echo")?;
            };

            let echo = echo.clone();
            let reply = msg.into_reply(Payload::EchoOk { echo });

            self.seq += 1;
            self.network.send(reply)?;
            Ok(())
        }
    }

    #[test]
    fn test_basic_init() -> Result<(), Box<dyn Error>> {
        let (_, input, output) = run_node();

        let init = Message {
            src: "c2".into(),
            dest: "n1".into(),
            body: Body {
                msg_id: Some(3),
                in_reply_to: None,
                payload: Init::Init {
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            },
        };

        input.send(serde_json::to_string(&init)?)?;
        let _: Message<Init> = serde_json::from_str(&output.recv()?)?;
        Ok(())
    }

    #[test]
    fn test_basic_echo() -> Result<(), Box<dyn Error>> {
        let (_, input, output) = run_node();

        let init = Message {
            src: "c2".into(),
            dest: "n1".into(),
            body: Body {
                msg_id: Some(3),
                in_reply_to: None,
                payload: Init::Init {
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            },
        };

        input.send(serde_json::to_string(&init)?)?;
        let _: Message<Init> = serde_json::from_str(&output.recv()?)?;

        let echo = Message {
            src: "c2".into(),
            dest: "n1".into(),
            body: Body {
                msg_id: Some(3),
                in_reply_to: None,
                payload: Payload::Echo {
                    echo: "ding-dong!".into(),
                },
            },
        };

        input.send(serde_json::to_string(&echo)?)?;
        let _: Message<Payload> = serde_json::from_str(&output.recv()?)?;
        Ok(())
    }

    fn run_node() -> (JoinHandle<()>, Sender<String>, Receiver<String>) {
        let (stdout_tx, stdout_rx) = channel();
        let (stdin_tx, stdin_rx) = channel();

        let runtime = thread::spawn(move || {
            run_internal::<Payload, EchoNode>(stdout_tx, stdin_rx).unwrap();
        });

        (runtime, stdin_tx, stdout_rx)
    }
}
