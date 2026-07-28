#![allow(unused)]
use std::{collections::HashMap, default, sync::atomic::{self, AtomicUsize}};
use rocket::State;
use rocket::{figment::value::Num::USize, futures::{SinkExt, StreamExt, stream::SplitSink}, tokio::io::DuplexStream};
use rocket_ws::{Channel, Message, WebSocket};

static USER_ID_COUNTER: AtomicUsize = AtomicUsize::new(0); 

#[derive(Default)]
struct ChatRoom 
{
    connections: HashMap<usize, SplitSink<DuplexStream, Message>>
}


#[rocket::get("/")]
fn chat(ws: WebSocket, state: &State<ChatRoom>) -> Channel<'static>
{
    ws.channel(move | stream |  Box::pin(async move {
        let user_id = USER_ID_COUNTER.fetch_add(1, atomic::Ordering::Relaxed);
        let (mut ws_sink, mut ws_stream) = stream.split();
        while let  Some(message) = ws_stream.next().await {

        }

        Ok(())
    }))
}

#[rocket::main]
async fn main() {
    let _ = rocket::build()
        .mount("/", rocket::routes![
            chat
        ])
        .manage(ChatRoom::default())
        .launch()
        .await;
}
