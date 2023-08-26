use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use mlua::prelude::*;
use tokio::{runtime::Runtime, task::LocalSet};

mod message;
mod state;
mod thread;
mod traits;

mod impl_async;
mod impl_runner;
mod impl_threads;

pub use self::thread::SchedulerThreadId;
pub use self::traits::*;

use self::{
    state::SchedulerState,
    thread::{SchedulerThread, SchedulerThreadSender},
};

/**
    Scheduler for Lua threads and futures.
*/
#[derive(Debug)]
pub(crate) struct Scheduler {
    runtime: Runtime,
    local_set: LocalSet,
    state: SchedulerState,
    threads: Arc<Mutex<VecDeque<SchedulerThread>>>,
    thread_senders: Arc<Mutex<HashMap<SchedulerThreadId, SchedulerThreadSender>>>,
}

impl Scheduler {
    /**
        Creates a new scheduler.
    */
    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("lune-threadpool-{}", id)
            })
            .build()
            .expect("Failed to create runtime");
        let local_set = LocalSet::new();

        Self {
            runtime,
            local_set,
            state: SchedulerState::new(),
            threads: Arc::new(Mutex::new(VecDeque::new())),
            thread_senders: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /**
        Sets the luau interrupt for this scheduler.

        This will propagate errors from any lua-spawned
        futures back to the lua threads that spawned them.
    */
    pub fn set_interrupt_for(&self, lua: &Lua) {
        // Propagate errors given to the scheduler back to their lua threads
        // FUTURE: Do profiling and anything else we need inside of this interrupt
        let state = self.state.clone();
        lua.set_interrupt(move |_| {
            if let Some(id) = state.get_current_thread_id() {
                if let Some(err) = state.get_thread_error(id) {
                    return Err(err);
                }
            }
            Ok(LuaVmState::Continue)
        });
    }

    /**
        Sets the exit code for the scheduler.

        This will stop the scheduler from resuming any more lua threads or futures.

        Panics if the exit code is set more than once.
    */
    pub fn set_exit_code(&self, code: impl Into<u8>) {
        assert!(
            self.state.exit_code().is_none(),
            "Exit code may only be set exactly once"
        );
        self.state.set_exit_code(code.into());
    }
}
