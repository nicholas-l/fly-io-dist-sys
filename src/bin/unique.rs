use std::{io::Write, sync::mpsc::Sender};

use anyhow::Context;
use fly_io_dist_sys::{process, Event, Init, Node};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk { id: String },
}

struct UniqueNode {
    node_id: String,
    message_id: usize,
}

impl Node<Payload> for UniqueNode {
    fn from_init(intial_input: &Init, _tx: Sender<Event<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node_id: intial_input.node_id.clone(),
            message_id: 0,
        })
    }

    fn step(&mut self, input: Event<Payload>, output: &mut impl Write) -> anyhow::Result<()> {
        match input {
            Event::ExternalMessage(input) => {
                let payload = match input.payload() {
                    Payload::Generate => {
                        let message_id = self.message_id;
                        self.message_id += 1;
                        Payload::GenerateOk {
                            id: format!("{}-{}", self.node_id, message_id),
                        }
                    }
                    Payload::GenerateOk { .. } => unreachable!(),
                };
                let reply = input.into_reply(self.message_id, Some(payload));
                serde_json::to_writer(&mut *output, &reply)?;
                output.write_all(b"\n").context("write trailing newline")?;
            }
            Event::InternalMessage(_) => {
                panic!("We do not support internal messages in this node type.")
            }
            Event::Shutdown => todo!(),
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    process::<UniqueNode, Payload, _>()
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn test_step() {}
}
