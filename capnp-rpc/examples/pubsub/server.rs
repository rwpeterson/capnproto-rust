// Copyright (c) 2013-2016 Sandstorm Development Group, Inc. and contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};
use crate::pubsub_capnp::{publisher, subscriber, subscription, qux, baz};

use capnp::capability::Promise;

use futures::{AsyncReadExt, FutureExt, StreamExt};

struct SubscriberHandle {
    client: subscriber::Client<qux::Owned>,
    requests_in_flight: i32,
}

struct SubscriberMap {
    subscribers: HashMap<u64, SubscriberHandle>,
}

impl SubscriberMap {
    fn new() -> SubscriberMap {
        SubscriberMap { subscribers: HashMap::new() }
    }
}

struct SubscriptionImpl {
    id: u64,
    subscribers: Rc<RefCell<SubscriberMap>>,
}

impl SubscriptionImpl {
    fn new(id: u64, subscribers: Rc<RefCell<SubscriberMap>>) -> SubscriptionImpl {
        SubscriptionImpl { id: id, subscribers: subscribers }
    }
}

impl Drop for SubscriptionImpl {
    fn drop(&mut self) {
        println!("subscription dropped");
        self.subscribers.borrow_mut().subscribers.remove(&self.id);
    }
}

impl subscription::Server for SubscriptionImpl {}

struct PublisherImpl {
    next_id: u64,
    subscribers: Rc<RefCell<SubscriberMap>>,
}

impl PublisherImpl {
    pub fn new() -> (PublisherImpl, Rc<RefCell<SubscriberMap>>) {
        let subscribers = Rc::new(RefCell::new(SubscriberMap::new()));
        (PublisherImpl { next_id: 0, subscribers: subscribers.clone() },
         subscribers.clone())
    }
}

impl publisher::Server<qux::Owned> for PublisherImpl {
    fn subscribe(&mut self,
                 params: publisher::SubscribeParams<qux::Owned>,
                 mut results: publisher::SubscribeResults<qux::Owned>,)
                 -> Promise<(), ::capnp::Error>
    {
        println!("subscribe");
        self.subscribers.borrow_mut().subscribers.insert(
            self.next_id,
            SubscriberHandle {
                client: pry!(pry!(params.get()).get_subscriber()),
                requests_in_flight: 0,
            }
        );

        results.get().set_subscription(capnp_rpc::new_client(
            SubscriptionImpl::new(self.next_id, self.subscribers.clone())));

        self.next_id += 1;
        Promise::ok(())
    }
}

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::net::ToSocketAddrs;
    let args: Vec<String> = ::std::env::args().collect();
    if args.len() != 3 {
        println!("usage: {} server HOST:PORT", args[0]);
        return Ok(());
    }

    let addr = args[2].to_socket_addrs().unwrap().next().expect("could not parse address");

    // Trigger sending as fast as possible
    let (tx, rx) = flume::bounded(1);
    std::thread::spawn(move || {
        use parking_lot::RwLock;
        use std::sync::Arc;
        let data = Arc::new(
            RwLock::new(
                vec![(42u64, 42u64); 64 * 1024 * 1024]
            )
        );
        loop {
            let (respond_to, response) = flume::bounded(1);
            if let Err(_) = tx.send((data.clone(), respond_to)) {
                break;
            }
            if let Err(_) = response.recv() {
                break;
            }
        }
    });


    tokio::task::LocalSet::new().run_until(async move {
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        let (publisher_impl, subscribers) = PublisherImpl::new();
        let publisher: publisher::Client<_> = capnp_rpc::new_client(publisher_impl);

        let handle_incoming = async move {
            loop {
                let (stream, _) = listener.accept().await?;
                stream.set_nodelay(true)?;
                let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
                let network =
                    twoparty::VatNetwork::new(reader, writer,
                                              rpc_twoparty_capnp::Side::Server, Default::default());
                let rpc_system = RpcSystem::new(Box::new(network), Some(publisher.clone().client));

                tokio::task::spawn_local(Box::pin(rpc_system.map(|_| ())));
            }
        };

        let send_to_subscribers = async move {
            let mut buf = capnp::Word::allocate_zeroed_vec(1 << 28);
            let mut alloc = capnp::message::ScratchSpaceHeapAllocator::new(
                capnp::Word::words_to_bytes_mut(&mut buf));
            while let Ok((datam, response_to)) = rx.recv_async().await {
                let data = datam.read();
                let mut msg = capnp::message::Builder::new(&mut alloc);
                let msg_bdr = msg.init_root::<baz::Builder>();

                // figure out the sizes of outer and inner lists
                let n = 2usize.pow(27);
                let full_lists = (data.len() / n) as u32;
                let remainder = (data.len() % n) as u32;
                let lists = if remainder > 0 {
                    full_lists + 1
                } else {
                    full_lists
                };

                let mut outer_bdr = msg_bdr.init_baz(lists);
                for (i, chunk) in data.chunks(n).enumerate() {
                   let mut inner_bdr = outer_bdr.reborrow().init(i as u32, chunk.len() as u32);
                    for (j, &(foo, bar)) in chunk.iter().enumerate() {
                        let mut qux_bdr = inner_bdr.reborrow().get(j as u32);
                        qux_bdr.set_foo(foo);
                        qux_bdr.set_bar(bar);
                    }
                }

                let subscribers1 = subscribers.clone();
                let subs = &mut subscribers.borrow_mut().subscribers;
                for (&idx, mut subscriber) in subs.iter_mut() {
                    if subscriber.requests_in_flight < 5 {
                        subscriber.requests_in_flight += 1;
                        let mut request = subscriber.client.push_message_request();
                        request.get().set_message(msg.get_root_as_reader()?)?;
                        let subscribers2 = subscribers1.clone();
                        tokio::task::spawn_local(Box::pin(
                            request.send().promise.map(move |r| {
                                match r {
                                    Ok(_) => {
                                        subscribers2.borrow_mut().subscribers.get_mut(&idx).map(|ref mut s| {
                                            s.requests_in_flight -= 1;
                                        });
                                    }
                                    Err(e) => {
                                        println!("Got error: {:?}. Dropping subscriber.", e);
                                        subscribers2.borrow_mut().subscribers.remove(&idx);
                                    }
                                }
                            })));
                    }
                }
                response_to.send(()).unwrap();
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        };

        let _ : ((), ()) = futures::future::try_join(handle_incoming, send_to_subscribers).await?;
        Ok(())
    }).await
}
