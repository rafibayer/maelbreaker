use std::{
    error::Error,
    io,
    ops::DerefMut,
    sync::{mpsc::channel, Arc, Mutex},
    thread,
};

use serde::{de::DeserializeOwned, Serialize};

const EOI: &'static str = "EOI";

use crate::{
    node::Node,
    types::{Body, Init, Message},
};

pub fn run<R, W, P, N>(input: R, output: W) -> Result<(), Box<dyn Error>>
where
    R: io::BufRead,
    W: io::Write + std::marker::Send + 'static,
    P: Serialize + DeserializeOwned + Send + 'static,
    N: Node<P>,
{
    let mut input = input.lines();
    let output = Arc::new(Mutex::new(output));

    let init: Message<Init> = serde_json::from_str(&input.next().ok_or("no init received")??)?;

    let Init::Init { node_id, node_ids } = init.body.payload else {
        return Err("expected init as first message")?;
    };

    let reply = Message {
        src: init.dest,
        dest: init.src,
        body: Body {
            msg_id: None,
            in_reply_to: init.body.msg_id,
            payload: Init::InitOk,
        },
    };

    let (tx, rx) = channel();

    let mut node = N::from_init(tx, node_id, node_ids);

    let outbound = thread::spawn(move || {
        // send the init_ok
        let mut locked = output.lock().unwrap();
        let mut deref = locked.deref_mut();
        write(&mut deref, &reply).unwrap();

        // reply to other messages
        loop {
            match rx.recv() {
                Ok(outbound) => {
                    write(deref, &outbound).unwrap();
                }
                Err(e) => {
                    eprintln!("{:#?}", e);
                    return;
                }
            }
        }
    });

    for line in input {
        let line = &line?;
        if line == EOI {
            break;
        }
        let message: Message<P> = serde_json::from_str(line)?;

        node.handle_message(message)?;
    }

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

fn write<W: io::Write, T: Serialize>(
    mut stdout: &mut W,
    message: &T,
) -> Result<(), Box<dyn Error>> {
    serde_json::to_writer(&mut stdout, message)?;
    stdout.write_all(b"\n").unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::{io::Write, sync::mpsc::Sender};

    use serde::Deserialize;

    use super::*;

    #[derive(Default, Clone)]
    struct TestWriter {
        data: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.data.lock().unwrap().write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.data.lock().unwrap().flush()
        }
    }

    impl ToString for TestWriter {
        fn to_string(&self) -> String {
            String::from_utf8_lossy(&self.data.lock().unwrap()).to_string()
        }
    }

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
    fn test_basic_init() {
        let (output, runtime) = run_node(vec![]);

        runtime.join().unwrap();
        let s = output.to_string();
        let init_ok: Message<Init> = serde_json::from_str(&s).unwrap();
        assert_eq!(&init_ok.src, "n1");
        assert_eq!(&init_ok.dest, "c1");
        match init_ok.body {
            Body {
                msg_id: None,
                in_reply_to: Some(1),
                payload: Init::InitOk,
            } => {}
            _ => panic!("invalid init_ok body"),
        };
    }

    #[test]
    fn test_basic_echo() {
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

        let echo_str = serde_json::to_string(&echo).unwrap();
        let (output, runtime) = run_node(vec![echo_str]);
        runtime.join().unwrap();
        let s = output.to_string();
        let mut lines = s.lines();

        let _: Message<Init> = serde_json::from_str(lines.next().unwrap()).unwrap();
        let _: Message<EchoPayload> = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(None, lines.next());
    }

    fn run_node(mut input: Vec<String>) -> (TestWriter, thread::JoinHandle<()>) {
        let init = Message {
            src: "c1".into(),
            dest: "n1".into(),
            body: Body {
                msg_id: Some(1),
                in_reply_to: None,
                payload: Init::Init {
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            },
        };

        let init_json = serde_json::to_string(&init).unwrap();
        let eoi = EOI.to_string();

        let mut input_strings = vec![init_json];
        input_strings.append(&mut input);
        input_strings.push(eoi);

        let cursor_string = input_strings.join("\n");

        let input = io::Cursor::new(cursor_string.as_bytes().to_vec());
        let output = TestWriter::default();
        let output_clone = output.clone();

        let runtime = thread::spawn(|| {
            run::<_, _, EchoPayload, EchoNode>(input, output).unwrap();
        });
        (output_clone, runtime)
    }
}
