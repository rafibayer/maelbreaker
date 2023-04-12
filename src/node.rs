use std::{error::Error, sync::mpsc::Sender};

use crate::types::Message;

pub trait Node<Payload> {
    fn from_init(network: Sender<Message<Payload>>, node_id: String, node_ids: Vec<String>)
        -> Self;
    fn handle_message(&mut self, msg: Message<Payload>) -> Result<(), Box<dyn Error>>;
}
