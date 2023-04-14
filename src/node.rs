use std::sync::mpsc::Sender;

use crate::types::{Message, Try};

pub trait Node<Payload> {
    fn from_init(net: Sender<Message<Payload>>, node_id: String, node_ids: Vec<String>) -> Self;
    fn handle_message(&mut self, msg: Message<Payload>) -> Try;
}
