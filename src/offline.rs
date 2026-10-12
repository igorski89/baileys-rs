//! Shared FIFO processing for offline stanzas, with source-compatible batch yields.
use crate::binary::BinaryNode;
use futures_util::future::BoxFuture;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

pub type Handler<E> = Arc<dyn Fn(BinaryNode) -> BoxFuture<'static, Result<(), E>> + Send + Sync>;
pub type Yield<E> = Arc<dyn Fn() -> BoxFuture<'static, Result<(), E>> + Send + Sync>;
#[derive(Debug)]
pub enum Failure<E> {
    UnknownType(String),
    Error(E),
}
pub type ErrorReporter<E> = Arc<dyn Fn(Failure<E>, &str) + Send + Sync>;
pub struct Dependencies<E> {
    pub is_ws_open: Arc<dyn Fn() -> bool + Send + Sync>,
    pub on_unexpected_error: ErrorReporter<E>,
    pub yield_to_event_loop: Yield<E>,
}
impl<E: Send + 'static> Dependencies<E> {
    pub fn new(
        is_ws_open: impl Fn() -> bool + Send + Sync + 'static,
        on_error: impl Fn(Failure<E>, &str) + Send + Sync + 'static,
    ) -> Self {
        Self {
            is_ws_open: Arc::new(is_ws_open),
            on_unexpected_error: Arc::new(on_error),
            yield_to_event_loop: Arc::new(|| {
                Box::pin(async {
                    tokio::task::yield_now().await;
                    Ok(())
                })
            }),
        }
    }
}
struct State {
    nodes: VecDeque<(String, BinaryNode)>,
    processing: bool,
}
struct Shared<E> {
    state: Mutex<State>,
    handlers: HashMap<String, Handler<E>>,
    deps: Dependencies<E>,
    batch_size: usize,
}
pub struct Processor<E>(Arc<Shared<E>>);
impl<E> Clone for Processor<E> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<E: Send + 'static> Processor<E> {
    pub fn with_batch_size(
        handlers: HashMap<String, Handler<E>>,
        deps: Dependencies<E>,
        batch_size: usize,
    ) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                nodes: VecDeque::new(),
                processing: false,
            }),
            handlers,
            deps,
            batch_size,
        }))
    }
    /// Use the upstream default of ten handled nodes per yield.
    pub fn new(handlers: HashMap<String, Handler<E>>, deps: Dependencies<E>) -> Self {
        Self::with_batch_size(handlers, deps, 10)
    }
    /// Enqueueing restarts a stopped processor; reopening alone does not.
    pub fn enqueue(&self, kind: impl Into<String>, node: BinaryNode) {
        let mut state = self.0.state.lock().expect("offline queue mutex");
        state.nodes.push_back((kind.into(), node));
        if state.processing {
            return;
        }
        state.processing = true;
        drop(state);
        let shared = self.0.clone();
        tokio::spawn(async move {
            shared.run().await;
        });
    }
    pub fn queued_len(&self) -> usize {
        self.0
            .state
            .lock()
            .expect("offline queue mutex")
            .nodes
            .len()
    }
    pub fn is_processing(&self) -> bool {
        self.0.state.lock().expect("offline queue mutex").processing
    }
}
impl<E: Send + 'static> Shared<E> {
    async fn run(&self) {
        let mut processed = 0;
        loop {
            {
                let mut state = self.state.lock().expect("offline queue mutex");
                if state.nodes.is_empty() {
                    state.processing = false;
                    return;
                }
            }
            if !(self.deps.is_ws_open)() {
                self.state.lock().expect("offline queue mutex").processing = false;
                return;
            }
            let (kind, node) = self
                .state
                .lock()
                .expect("offline queue mutex")
                .nodes
                .pop_front()
                .expect("single offline processor");
            let Some(handler) = self.handlers.get(&kind) else {
                (self.deps.on_unexpected_error)(
                    Failure::UnknownType(format!("unknown offline node type: {kind}")),
                    "processing offline node",
                );
                continue;
            };
            if let Err(error) = handler(node).await {
                (self.deps.on_unexpected_error)(
                    Failure::Error(error),
                    &format!("processing offline {kind}"),
                );
            }
            processed += 1;
            if processed >= self.batch_size {
                processed = 0;
                if let Err(error) = (self.deps.yield_to_event_loop)().await {
                    // The source's outer catch leaves its processing guard set on a failed yield.
                    (self.deps.on_unexpected_error)(
                        Failure::Error(error),
                        "processing offline nodes",
                    );
                    return;
                }
            }
        }
    }
}
