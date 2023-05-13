use std::{io::Write, sync::mpsc::Sender};

use anyhow::Context;
use fly_io_dist_sys::{process, Event, Init, Node};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    message_id: usize,
}

impl Node<Payload> for EchoNode {
    fn from_init(_initial_message: &Init, _tx: Sender<Event<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self { message_id: 0 })
    }

    fn step(&mut self, input: Event<Payload>, output: &mut impl Write) -> anyhow::Result<()> {
        match input {
            Event::ExternalMessage(input) => {
                let payload = match input.payload() {
                    Payload::Echo { echo } => Payload::EchoOk { echo: echo.into() },
                    Payload::EchoOk { .. } => {
                        unreachable!()
                    }
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
    process::<EchoNode, Payload, _>()
}

#[cfg(test)]
mod tests {
    use fly_io_dist_sys::{Body, Message};

    use super::*;

    #[test]
    fn test_step() {
        let mut node = EchoNode { message_id: 0 };
        let mut output = Vec::new();
        let body = Body::new(
            Some(0),
            None,
            Payload::Echo {
                echo: "hello".into(),
            },
        );
        let input = Message::new("testsrc".into(), "testdest".into(), body);

        let res = node.step(Event::ExternalMessage(input), &mut output);

        assert!(res.is_ok());

        let read: Message<Payload> = serde_json::from_slice(&output).unwrap();

        let expected = {
            let body = Body::new(
                Some(0),
                Some(0),
                Payload::EchoOk {
                    echo: "hello".into(),
                },
            );
            Message::new("testdest".into(), "testsrc".into(), body)
        };
        assert_eq!(read, expected);
    }
}
