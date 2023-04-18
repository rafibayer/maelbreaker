//! Defines the Node trait

use crate::{
    network::Network,
    types::{Message, Try},
};

/// Maelstrom node
pub trait Node<Payload> {
    /// constructs a Node from the body of an init message.
    /// Also provides the Node a network to send future messages on.
    /// The runtime is responsible for sending init_ok after this message returns.
    fn from_init(network: Network<Payload>, node_id: String, node_ids: Vec<String>) -> Self;

    /// handles inbound messages to this node from clients or other nodes.
    fn handle_message(&mut self, msg: Message<Payload>) -> Try;
}
