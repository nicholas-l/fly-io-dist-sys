use std::io::Write;

use anyhow::Context;
use fly_io_dist_sys::{process, Init, Message, Node};
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
    fn from_init(intial_input: &Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node_id: intial_input.node_id.clone(),
            message_id: 0,
        })
    }

    fn step(&mut self, input: Message<Payload>, output: &mut impl Write) -> anyhow::Result<()> {
        let payload = match input.payload() {
            Payload::Generate => {
                let message_id = self.message_id;
                self.message_id += 1;
                Payload::GenerateOk {
                    id: format!("{}-{}", self.node_id, message_id),
                }
            }
            Payload::GenerateOk { .. } => {
                unreachable!()
            }
        };
        let reply = input.into_reply(self.message_id, Some(payload));
        dbg!(&reply);
        serde_json::to_writer(&mut *output, &reply)?;
        output.write_all(b"\n").context("write trailing newline")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    process::<UniqueNode, Payload>()
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn test_step() {}
}
