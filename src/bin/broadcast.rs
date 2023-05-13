use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::Sender,
    time::Duration,
};

use anyhow::Context;
use fly_io_dist_sys::{process, Body, Event, Init, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: isize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<isize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        seen: HashSet<isize>,
    },
}

enum InternalMessage {
    Gossip,
}

struct BroadcastNode {
    id: String,
    message_id: usize,
    messages: HashSet<isize>,
    // nodes: Vec<String>,
    known: HashMap<String, HashSet<isize>>,

    neighbours: Vec<String>,
}

impl Node<Payload, InternalMessage> for BroadcastNode {
    fn from_init(
        intial_input: &Init,
        tx: Sender<Event<Payload, InternalMessage>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(300));
            if tx
                .send(Event::InternalMessage(InternalMessage::Gossip))
                .is_err()
            {
                // We have likley been shutdown so stop the loop.
                break;
            }
        });
        Ok(Self {
            id: intial_input.node_id.clone(),
            message_id: 0,
            messages: HashSet::new(),
            // nodes: intial_input.node_ids.clone(),
            known: intial_input
                .node_ids
                .iter()
                .map(|id| (id.clone(), HashSet::new()))
                .collect(),
            neighbours: Vec::new(),
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InternalMessage>,
        output: &mut impl std::io::Write,
    ) -> anyhow::Result<()> {
        match input {
            Event::ExternalMessage(input) => {
                let payload = match input.payload() {
                    Payload::Broadcast { message } => {
                        self.messages.insert(*message);
                        Some(Payload::BroadcastOk)
                    }
                    Payload::BroadcastOk => unreachable!(),
                    Payload::Read => Some(Payload::ReadOk {
                        messages: self.messages.iter().cloned().collect(),
                    }),
                    Payload::ReadOk { .. } => unreachable!(),
                    Payload::Topology { topology } => {
                        self.neighbours = topology.get(&self.id).cloned().unwrap_or_else(|| {
                            panic!("Unable to find node in topology: {}", self.id)
                        });
                        Some(Payload::TopologyOk)
                    }
                    Payload::TopologyOk => unreachable!(),
                    Payload::Gossip { seen } => {
                        let known = self.known.get_mut(&input.src).unwrap();
                        known.extend(seen.clone());
                        self.messages.extend(seen);
                        None
                    }
                };
                if let Some(payload) = payload {
                    let reply = input.into_reply(self.message_id, Some(payload));
                    serde_json::to_writer(&mut *output, &reply)?;
                    output.write_all(b"\n").context("write trailing newline")?;
                }
            }
            Event::InternalMessage(message) => {
                match message {
                    InternalMessage::Gossip => {
                        for node in &self.neighbours {
                            // Only send the messages that the other node has not sent to us.
                            let seen = self
                                .messages
                                .difference(self.known.get(node).unwrap())
                                .cloned()
                                .collect();
                            let payload = Payload::Gossip { seen };
                            let message_id = self.message_id;
                            self.message_id += 1;
                            let body = Body::new(Some(message_id), None, payload);
                            let message = Message::new(self.id.clone(), node.clone(), body);
                            serde_json::to_writer(&mut *output, &message)?;
                            output.write_all(b"\n").context("write trailing newline")?;
                        }
                    }
                }
            }
            Event::Shutdown => todo!(),
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    process::<BroadcastNode, Payload, InternalMessage>()
}
