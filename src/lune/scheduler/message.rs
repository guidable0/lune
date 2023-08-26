use std::sync::{MutexGuard, TryLockError};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use super::state::SchedulerState;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) enum SchedulerMessage {
    ExitCodeSet,
    PushedLuaThread,
    FutureSpawned,
    FutureCompleted,
}

/**
    A message sender for the scheduler.

    As long as this sender is not dropped, the scheduler
    will be kept alive, waiting for more messages to arrive.
*/
pub(crate) struct SchedulerMessageSender(UnboundedSender<SchedulerMessage>);

impl SchedulerMessageSender {
    /**
        Creates a new message sender for the scheduler.
    */
    pub fn new(state: &SchedulerState) -> Self {
        Self(
            state
                .message_sender
                .lock()
                .expect("Scheduler state was poisoned")
                .clone(),
        )
    }

    pub fn send_exit_code_set(&self) {
        self.0.send(SchedulerMessage::ExitCodeSet).ok();
    }

    pub fn send_pushed_lua_thread(&self) {
        self.0.send(SchedulerMessage::PushedLuaThread).ok();
    }

    pub fn send_future_spawned(&self) {
        self.0.send(SchedulerMessage::FutureSpawned).ok();
    }

    pub fn send_future_completed(&self) {
        self.0.send(SchedulerMessage::FutureCompleted).ok();
    }
}

/**
    A message receiver for the scheduler.

    Only one message receiver may exist per scheduler.
*/
pub(crate) struct SchedulerMessageReceiver<'a>(MutexGuard<'a, UnboundedReceiver<SchedulerMessage>>);

impl<'a> SchedulerMessageReceiver<'a> {
    /**
        Creates a new message receiver for the scheduler.

        Panics if the message receiver is already being used.
    */
    pub fn new(state: &'a SchedulerState) -> Self {
        Self(match state.message_receiver.try_lock() {
            Err(TryLockError::Poisoned(_)) => panic!("Sheduler state was poisoned"),
            Err(TryLockError::WouldBlock) => {
                panic!("Message receiver may only be borrowed once at a time")
            }
            Ok(guard) => guard,
        })
    }

    // NOTE: Holding this lock across await points is fine, since we
    // can only ever create lock exactly one SchedulerMessageReceiver
    // See above constructor for details on this
    #[allow(clippy::await_holding_lock)]
    pub async fn recv(&mut self) -> Option<SchedulerMessage> {
        self.0.recv().await
    }
}
