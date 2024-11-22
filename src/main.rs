use std::{collections::{HashMap, HashSet}, time::Duration};
use std::time::SystemTime;

use log::*;
use mqtt_async_client::client::{Client, QoS, ReadResult};
use tokio::{
    sync::mpsc::{self, Receiver, Sender}, task::JoinHandle, time::timeout
};
use anyhow::{Result, anyhow};
use serde::Serialize;
use serde_json;

#[derive(Debug)]
enum Power {
    On,
    Off,
}

type Topic = String;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    if std::env::args().count() > 1 {
        let argv0 = std::env::args().next().unwrap_or("mqtt-ha".into());
        eprintln!("Usage: {}", argv0);
        std::process::exit(2);
    }

    let (send, recv) = mpsc::channel(50);

    let (r0, r1) = tokio::join!(
        subscribe(send),
        adjuster(recv),
    );

    r0.and(r1)
}

async fn adjuster(mut action_sender: Receiver<Topic>) -> Result<()> {
    let mut pending = HashSet::<Topic>::new();
    let mut light_runs = HashMap::<Topic, JoinHandle<Result<_>>>::new();

    loop {
        let action = if pending.is_empty() {
            // no timeout
            action_sender.recv().await
        } else {
            match timeout(Duration::new(1, 0), action_sender.recv()).await {
                Ok(o) => o,
                Err(_) => {
                    // timeout
                    light_up_all(pending.drain(), &mut light_runs).await;
                    continue;
                }
            }
        };

        let Some(id) = action else {
            return Err(anyhow!("channel closed"));
        };

        info!("smooth light, queued: {id:?}");
        pending.insert(id);
    }
}

async fn light_up_all(
    topics: impl Iterator<Item = Topic>,
    light_runs: &mut HashMap::<Topic, JoinHandle<Result<()>>>,
) {
    for id in topics {
        use std::collections::hash_map::Entry;

        match light_runs.entry(id.clone()){
            Entry::Occupied(mut ent) => {
                // has the light run finished for this?

                let handle: &mut JoinHandle<_> = ent.get_mut();
                let ns = 25_000_000; // 25ms in ns


                let replace = match timeout(Duration::new(0, ns), handle).await {
                    Err(_) => false, // timeout, task still running
                    Ok(Ok(Ok(()))) => {
                        debug!("previous operation on \"{id}\" finished successfully");
                        true
                    },
                    Ok(Ok(Err(e))) => {
                        error!("joined previous operation (on \"{id}\"), found error: {e:?}");
                        true
                    }
                    Ok(Err(e)) => {
                        error!("error joining light-up task (on \"{id}\"): {e:?}");
                        true
                    }
                };

                if replace {
                    ent.insert(light_up(id).await);
                } else {
                    warn!(
                        "request to light-up \"{}\": skipping, previous task still running",
                        ent.key()
                    );
                }
            }
            Entry::Vacant(ent) => {
                ent.insert(light_up(id).await);
            }
        }
    }
}

async fn light_up(id: Topic) -> JoinHandle<Result<()>> {
    #[derive(Debug)]
    enum LightMsg {
        // { "brightness": <val>, "transition": <secs> }
        Brightness {
            level: u16,
            transition: u16,
        },
        // { "color_temp": <val>, "transition": <secs> }
        Colour {
            temp: Colour,
            transition: u16,
        }
        // { "state": <val> } // val is on/off
        // State(bool),
    }

    impl LightMsg {
        fn duration(&self) -> Duration {
            let secs = match *self {
                LightMsg::Brightness { transition, .. } => transition,
                LightMsg::Colour { transition, .. } => transition,
            };

            Duration::new(secs.into(), 0)
        }
    }

    #[derive(Debug)]
    enum Colour {
        //Value(u8),
        Str(&'static str),
    }

    impl Serialize for LightMsg {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer
        {
            use serde::ser::SerializeStruct;

            match self {
                Self::Brightness { level, transition } => {
                    let mut s = serializer.serialize_struct("msg", 2)?;
                    s.serialize_field("brightness", level)?;
                    s.serialize_field("transition", transition)?;
                    s
                }
                Self::Colour { temp, transition } => {
                    let mut s = serializer.serialize_struct("msg", 2)?;
                    s.serialize_field("color_temp", temp)?;
                    s.serialize_field("transition", transition)?;
                    s
                }
            }.end()
        }
    }

    impl Serialize for Colour {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer
        {
            match self {
                Self::Str(s) => serializer.serialize_str(s)
            }
        }
    }


    tokio::spawn(async move {
        let client = connected_client().await?;

        let msgs = [
            LightMsg::Brightness { level: 254, transition: 10 },
            LightMsg::Colour { temp: Colour::Str("coolest"), transition: 10 },
        ];

        let mut id = id;
        id.push_str("/set");

        for msg in msgs {
            use mqtt_async_client::client::Publish;

            let as_json = match serde_json::to_string(&msg) {
                Ok(x) => x,
                Err(e) => {
                    warn!("couldn't serialize {msg:?}: {e}");
                    continue
                }
            };

            debug!("publish \"{as_json}\" to \"{id}\"");

            client.publish(&Publish::new(
                    id.clone(),
                    as_json.into_bytes()
            )).await?;

            let dur = msg.duration();
            debug!("sleeping for {dur:?}");
            tokio::time::sleep(dur).await;
        }

        Result::<_>::Ok(())
    })
}

async fn subscribe(action_sender: Sender<String>) -> Result<()> {
    use mqtt_async_client::client::{Subscribe, SubscribeTopic};

    let mut client = connected_client().await?;

    let subopts = Subscribe::new(
        vec![
            SubscribeTopic {
                qos: QoS::AtLeastOnce,
                topic_path: "zigbee2mqtt/#".into(),
            },
        ],
    );

    trace!("subscribing");
    let subres = client.subscribe(subopts).await?;
    subres.any_failures().map_err(|e| {
        error!("e: {e}");
        e
    })?;

    trace!("subscribed");

    let mut light_state = HashMap::new();

    loop {
        use mqtt_async_client::Error as MqttError;

        let resp = client.read_subscriptions().await;

        let msg = match resp {
            Err(e @ MqttError::Disconnected) => return Err(e.into()),
            Err(e) => {
                error!("{e}");
                continue;
            }
            Ok(m) => m,
        };

        trace!(
            "got {}-byte message on topic {}",
            msg.payload().len(),
            msg.topic()
        );

        let parts: Vec<_> = msg.topic().split('/').collect();
        let [_namespace, area, ref rest @ .., last] = *parts else { continue };

        trace!("got message in area \"{area}\", rest = {rest:?}, last = {last}");

        if last == "action" {
            let id = msg
                .topic()
                .rsplitn(2, '/')
                .nth(1)
                .expect("unreachable due to above parts check");

            handle_action(
                area,
                &rest[..rest.len() - 1],
                &msg,
                &mut light_state,
                id,
                &action_sender,
            ).await?;
        }
    }
}

async fn connected_client() -> Result<Client> {
    let mut client = Client::builder()
        .set_url_string("mqtt://localhost:1883")?
        .set_client_id(Some("smooth-lights".into()))
        .set_max_packet_len(0x20000)
        .build()?;

    client.connect().await?;

    Ok(client)
}

async fn handle_action(
    area: &str,
    params: &[&str], // ["upstairs", "light1"]
    msg: &ReadResult,
    light_state: &mut HashMap<Topic, SystemTime>,
    id: &str,
    action_sender: &Sender<Topic>,
) -> Result<()> {
    let Ok(s) = std::str::from_utf8(msg.payload()) else {
        warn!("unparsable action in \"{area}\", params = {params:?}");
        return Ok(())
    };

    debug!("got action in \"{area}\", params = {params:?}, payload = {s}");

    let Ok(Power::On) = s.parse() else { return Ok(()) };

    let now = SystemTime::now();
    let old = now - Duration::new(60 * 60 * 7, 0); // TODO: 7 hours hack

    let last_on = light_state.get(id).unwrap_or(&old /* &now */);

    let (should_act, duration) = match now.duration_since(*last_on) {
        Ok(diff) => {
            (
                diff > Duration::new(60 * 60 * 6, 0),
                Some(diff),
            )
        }
        Err(_) => {
            // clock may have moved, assume ass
            (
                true,
                None,
            )
        }
    };

    debug!("should_act = {should_act}");

    if should_act {
        match duration {
            Some(d) => {
                let hrs = d.as_secs() / 60 / 60;
                info!("{id} turned on after {hrs} hours, resetting");
            }
            None => {
                info!("{id} turned on (after unknown time), resetting");
            }
        }

        action_sender.send(id.to_string()).await?;
    }

    light_state.insert(id.to_string(), now);

    Ok(())
}

impl std::str::FromStr for Power {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "on" => Power::On,
            "off" => Power::Off,
            _ => return Err(()),
        })
    }
}
