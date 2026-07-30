#![allow(unused)]
use std::{collections::HashMap, default, sync::atomic::{self, AtomicUsize}};
use rocket::State;
use rocket::tokio::sync::Mutex;
use rocket::{figment::value::Num::USize, futures::{SinkExt, StreamExt, stream::SplitSink}};
use rocket_ws::{Channel, Message, WebSocket, stream::DuplexStream};

static USER_ID_COUNTER: AtomicUsize = AtomicUsize::new(0); 

#[derive(Default)]
struct ChatRoom 
{
    connections: Mutex<HashMap<usize, SplitSink<DuplexStream, Message>>>
}


#[rocket::get("/")]
fn chat<'r>(ws: WebSocket, state: &'r State<ChatRoom>) -> Channel<'r>
{
    ws.channel(move | stream |  Box::pin(async move {
        let user_id = USER_ID_COUNTER.fetch_add(1, atomic::Ordering::Relaxed);
        let (ws_sink, mut ws_stream) = stream.split();
        {
            let mut conns = state.connections.lock().await;

            conns.insert(user_id, ws_sink);
        }
        while let  Some(message) = ws_stream.next().await {
            
            {
                let mut conns = state.connections.lock().await;
                let msg = message?;
                for (_id, sink) in conns.iter_mut()
                {
                    let _ = sink.send(msg.clone()).await;   
                }

            }
        }

        {
            let mut conns = state.connections.lock().await;
            conns.remove(&user_id);
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
