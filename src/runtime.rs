use std::{
    error::Error,
    io::{stdin, stdout, BufRead, Write},
    sync::mpsc::{channel, Receiver, Sender},
    thread,
};

use serde::{de::DeserializeOwned, Serialize};

const EOI: &str = "EOI";

use crate::{
    node::Node,
    types::{Body, Init, Message, Try},
};

pub fn run<P, N>() -> Try
where
    P: std::fmt::Debug + Serialize + DeserializeOwned + Send + 'static,
    N: Node<P>,
{
    let (stdin_tx, stdin_rx) = channel();
    let (stdout_tx, stdout_rx) = channel::<String>();

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

    run_internal::<P, N>(stdout_tx, stdin_rx).unwrap();

    stdin_handle.join().unwrap();
    stdout_handle.join().unwrap();

    Ok(())
}

fn run_internal<P, N>(tx: Sender<String>, rx: Receiver<String>) -> Result<(), Box<dyn Error>>
where
    P: std::fmt::Debug + Serialize + DeserializeOwned + Send + 'static,
    N: Node<P>,
{
    eprintln!("Starting runtime...");

    eprintln!("Waiting for init message");

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
    let outbound = thread::spawn(move || {
        // send the init_ok
        let mut json = serde_json::to_string(&reply).unwrap();
        eprintln!("Writing init_ok: {json}");
        tx.send(json).unwrap();

        // reply to other messages
        loop {
            match node_receiver.recv() {
                Ok(outbound) => {
                    json = serde_json::to_string(&outbound).unwrap();
                    eprintln!("Writing outbound message: {json}");
                    tx.send(json).unwrap();
                }
                Err(e) => {
                    eprintln!("{:#?}", e);
                    return;
                }
            }
        }
    });

    eprintln!("Starting inbound processing");
    for line in rx {
        if line == EOI {
            eprintln!("Got EOI");

            break;
        }

        eprintln!("Got message: {line}");
        let message: Message<P> = serde_json::from_str(&line)?;
        node.handle_message(message)?;
    }

    eprintln!("Shutting down...");
    cleanup(node, outbound)
}

fn cleanup<P, N>(node: N, outbound: thread::JoinHandle<()>) -> Result<(), Box<dyn Error>>
where
    P: Serialize + DeserializeOwned + Send + 'static,
    N: Node<P>,
{
    drop(node);
    if let Err(outbound_err) = outbound.join() {
        eprintln!("{:#?}", outbound_err);
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::{sync::mpsc::Sender, thread::JoinHandle};

    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum EchoPayload {
        Echo { echo: String },
        EchoOk { echo: String },
    }

    struct EchoNode {
        network: Sender<Message<EchoPayload>>,
    }

    impl Node<EchoPayload> for EchoNode {
        fn from_init(
            network: Sender<Message<EchoPayload>>,
            _node_id: String,
            _node_ids: Vec<String>,
        ) -> Self {
            EchoNode { network }
        }

        fn handle_message(&mut self, msg: Message<EchoPayload>) -> Result<(), Box<dyn Error>> {
            let Message {
                src,
                dest,
                body:
                    Body {
                        msg_id,
                        in_reply_to: _,
                        payload: EchoPayload::Echo { echo },
                    },
            } = msg else {
                return Err("expected echo")?;
            };

            let reply = Message {
                src: dest,
                dest: src,
                body: Body {
                    in_reply_to: msg_id,
                    msg_id: None,
                    payload: EchoPayload::EchoOk { echo: echo },
                },
            };

            self.network
                .send(reply)
                .map_err(|_| "failed to send response")?;

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
                payload: EchoPayload::Echo {
                    echo: "ding-dong!".into(),
                },
            },
        };

        input.send(serde_json::to_string(&echo)?)?;
        let _: Message<EchoPayload> = serde_json::from_str(&output.recv()?)?;
        Ok(())
    }

    fn run_node() -> (JoinHandle<()>, Sender<String>, Receiver<String>) {
        let (stdout_tx, stdout_rx) = channel();
        let (stdin_tx, stdin_rx) = channel();

        let runtime = thread::spawn(move || {
            run_internal::<EchoPayload, EchoNode>(stdout_tx, stdin_rx).unwrap();
        });

        (runtime, stdin_tx, stdout_rx)
    }
}
