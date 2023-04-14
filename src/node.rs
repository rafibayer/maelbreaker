use crate::{
    network::Network,
    types::{Message, Try},
};

pub trait Node<Payload> {
    fn from_init(net: Network<Payload>, node_id: String, node_ids: Vec<String>) -> Self;
    fn handle_message(&mut self, msg: Message<Payload>) -> Try;
}
