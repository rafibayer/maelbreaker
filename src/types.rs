use std::{error::Error, fmt::Debug, sync::mpsc::Receiver};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::payload;

pub type Try = Result<(), Box<dyn Error>>;
pub type Rpc<P> = Result<Receiver<Message<P>>, Box<dyn Error>>;
pub type SyncTry = Result<(), Box<dyn Error + Send + Sync>>;

pub trait Payload: Clone + std::fmt::Debug + Serialize + DeserializeOwned + Send + 'static {}
impl<P: Clone + std::fmt::Debug + Serialize + DeserializeOwned + Send + 'static> Payload for P {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Body<Payload> {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message<Payload> {
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
}

impl<Payload> Message<Payload> {
    pub fn into_reply(self, payload: Payload) -> Message<Payload> {
        let next_id = self.body.msg_id.map(|id| id + 1);
        self.into_reply_with_id(payload, next_id)
    }

    pub fn into_reply_with_id(self, payload: Payload, msg_id: Option<usize>) -> Message<Payload> {
        Message {
            src: self.dest,
            dest: self.src,
            body: Body {
                msg_id,
                in_reply_to: self.body.msg_id,
                payload,
            },
        }
    }
}

payload!(
    pub enum Init {
        Init {
            node_id: String,
            node_ids: Vec<String>,
        },
        InitOk,
    }
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_init() {
        let init = Message {
            src: "c1".to_string(),
            dest: "n3".to_string(),
            body: Body {
                msg_id: Some(1),
                in_reply_to: None,
                payload: Init::Init {
                    node_id: "n3".to_string(),
                    node_ids: vec!["n1".to_string(), "n2".to_string(), "n3".to_string()],
                },
            },
        };

        let json = serde_json::to_string(&init).unwrap();
        assert_eq!(
            json,
            r#"{"src":"c1","dest":"n3","body":{"msg_id":1,"in_reply_to":null,"type":"init","node_id":"n3","node_ids":["n1","n2","n3"]}}"#
        );
    }

    #[test]
    fn test_deserialize_init() {
        let json = r#"{"src":"c1","dest":"n3","body":{"msg_id":1,"in_reply_to":null,"type":"init","node_id":"n3","node_ids":["n1","n2","n3"]}}"#;
        let init: Message<Init> = serde_json::from_str(&json).unwrap();
        assert_eq!(&init.src, "c1");
        assert_eq!(&init.dest, "n3");
        assert_eq!(init.body.msg_id, Some(1));
        assert_eq!(init.body.in_reply_to, None);
        match init.body.payload {
            Init::Init { node_id, node_ids } => {
                assert_eq!(node_id, "n3");
                assert_eq!(node_ids, vec!["n1", "n2", "n3"]);
            }
            _ => panic!("Unexpected message type"),
        }
    }
}
