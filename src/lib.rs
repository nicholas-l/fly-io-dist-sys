use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{BufRead, Write};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Message<T> {
    src: String,
    dest: String,
    pub body: Body<T>,
}

impl<T> Message<T> {
    pub fn new(src: String, dest: String, body: Body<T>) -> Self {
        Message {
            src,
            dest,
            body,
        }
    }

    pub fn into_reply(self, msg_id: usize, payload: Option<T>) -> Self {
        Message {
            src: self.dest,
            dest: self.src,
            body: Body {
                msg_id: Some(msg_id),
                in_reply_to: self.body.msg_id,
                payload: payload.unwrap_or(self.body.payload),
            },
        }
    }

    pub fn payload(&self) -> &T {
        &self.body.payload
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Body<T> {
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: T,
}

impl<T> Body<T> {
    pub fn new(msg_id: Option<usize>, in_reply_to: Option<usize>, payload: T) -> Self {
        Body {
            msg_id,
            in_reply_to,
            payload,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum PayloadInit {
    Init,
    InitOk,
}

pub trait Node<Payload, InjectedPayload = ()> {
    fn from_init() -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(&mut self, input: Message<Payload>, output: &mut impl Write) -> anyhow::Result<()>;
}

pub fn process<N, P>() -> anyhow::Result<()>
where
    P: DeserializeOwned + Send + 'static,
    N: Node<P>,
{
    let mut stdout = std::io::stdout().lock();

    let mut node = {
        let stdin = std::io::stdin();
        let mut stdin = stdin.lines();

        let init_msg: Message<PayloadInit> = serde_json::from_str(
            &stdin
                .next()
                .expect("no init message received")
                .context("failed to read init message from stdin")?,
        )
        .context("init message could not be deserialized")?;

        let reply = Message {
            src: init_msg.dest,
            dest: init_msg.src,
            body: Body {
                msg_id: Some(0),
                in_reply_to: init_msg.body.msg_id,
                payload: PayloadInit::InitOk,
            },
        };

        serde_json::to_writer(&mut stdout, &reply).context("serialize response to init")?;
        stdout.write_all(b"\n").context("write trailing newline")?;

        N::from_init()?
    };

    let (tx, rx) = std::sync::mpsc::channel();

    let jh = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for line in stdin.lines() {
            let line = line.context("Maelstrom input from STDIN could not be read")?;
            let input: Message<P> = serde_json::from_str(&line)
                .context("Maelstrom input from STDIN could not be deserialized")?;
            if let Err(_) = tx.send(input) {
                return Ok::<_, anyhow::Error>(());
            }
        }

        // let _ = tx.send(Event::EOF);
        Ok(())
    });

    for input in rx {
        node.step(input, &mut stdout)
            .context("Node step function failed")?;
    }

    jh.join()
        .expect("stdin thread panicked")
        .context("stdin thread err'd")?;

    Ok(())
}
