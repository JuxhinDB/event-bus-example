use anyhow::Result;
use event_bus::EventBus;
use logger_module::Logger;
use module::{Module, ModuleCtx};
use network_module::Network;

#[tokio::main]
async fn main() -> Result<()> {
    let event_bus = EventBus::new();

    let logger_ctx = ModuleCtx::new("logger", &event_bus);
    let mut logger = Logger::new(logger_ctx);

    let network_ctx = ModuleCtx::new("network", &event_bus);
    let mut network = Network::new(network_ctx);

    tokio::join!(network.run(), logger.run()).0?;

    Ok(())
}

mod event_bus {
    use tokio::sync::broadcast;

    #[derive(Clone, Debug)]
    pub struct Event {
        pub module: String,
        pub inner: EventKind,
    }

    #[derive(Clone, Debug)]
    pub enum EventKind {
        StubEvent(String),
    }

    #[derive(Debug)]
    pub struct EventBus {
        pub sender: broadcast::Sender<Event>,
        pub receiver: broadcast::Receiver<Event>,
    }

    impl Clone for EventBus {
        fn clone(&self) -> Self {
            Self {
                sender: self.sender.clone(),
                receiver: self.sender.subscribe(),
            }
        }
    }

    impl Default for EventBus {
        fn default() -> Self {
            Self::new()
        }
    }

    impl EventBus {
        pub fn new() -> Self {
            let (sender, receiver) = broadcast::channel(100);
            EventBus { sender, receiver }
        }

        pub fn subscribe(&self) -> broadcast::Receiver<Event> {
            self.sender.subscribe()
        }
    }
}

mod module {
    use super::event_bus::{Event, EventBus};

    use anyhow::Result;
    use async_trait::async_trait;
    use tokio::sync::broadcast;

    #[async_trait]
    pub trait Module {
        fn new(ctx: ModuleCtx) -> Self;
        async fn run(&mut self) -> Result<()>;
    }

    #[derive(Debug)]
    pub struct ModuleCtx {
        pub name: String,
        pub sender: broadcast::Sender<Event>,
        pub receiver: broadcast::Receiver<Event>,
    }

    impl ModuleCtx {
        pub fn new(name: &str, bus: &EventBus) -> Self {
            let sender = bus.sender.clone();
            let receiver = bus.subscribe();

            ModuleCtx {
                name: name.to_string(),
                sender,
                receiver,
            }
        }
    }
}

mod logger_module {
    use super::event_bus::EventKind;
    use super::module::{Module, ModuleCtx};
    use anyhow::Result;
    use async_trait::async_trait;

    pub struct Logger {
        ctx: ModuleCtx,
    }

    #[async_trait]
    impl Module for Logger {
        fn new(ctx: ModuleCtx) -> Self {
            Logger { ctx }
        }

        async fn run(&mut self) -> Result<()> {
            loop {
                tokio::select! {
                    e = self.ctx.receiver.recv() => {
                        match e {
                            Ok(event) => {
                                match event.inner {
                                    EventKind::StubEvent(message) => println!("{}: received event: {}", &self.ctx.name, message),
                                }
                            },
                            Err(e) => println!("Error: {}", e),
                        }
                    },
                }
            }
        }
    }
}

mod network_module {
    use super::event_bus::{Event, EventKind};
    use super::module::{Module, ModuleCtx};
    use anyhow::Result;
    use async_trait::async_trait;

    pub struct Network {
        ctx: ModuleCtx,
    }

    #[async_trait]
    impl Module for Network {
        fn new(ctx: ModuleCtx) -> Self {
            Network { ctx }
        }

        async fn run(&mut self) -> Result<()> {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

            loop {
                tokio::select! {
                _ = interval.tick() => {

                    let event = Event {
                        module: self.ctx.name.to_string(),
                        inner: EventKind::StubEvent("Completed some work".to_string()),
                    };
                    self.ctx.sender
                        .send(event)
                        .unwrap();
                },
                }
            }
        }
    }
}
