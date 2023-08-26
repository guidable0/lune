use std::{process::ExitCode, sync::Arc};

use mlua::prelude::*;

use tracing::debug;

use crate::lune::util::traits::LuaEmitErrorExt;

use super::Scheduler;

impl Scheduler {
    /**
        Runs all lua threads to completion.
    */
    fn run_lua_threads(&self, lua: &Lua) {
        if self.state.has_exit_code() {
            return;
        }

        let mut count = 0;

        // Pop threads from the scheduler until there are none left
        while let Some(thread) = self
            .pop_thread()
            .expect("Failed to pop thread from scheduler")
        {
            // Deconstruct the scheduler thread into its parts
            let thread_id = thread.id();
            let (thread, args) = thread.into_inner(lua);

            // Make sure this thread is still resumable, it might have
            // been resumed somewhere else or even have been cancelled
            if thread.status() != LuaThreadStatus::Resumable {
                continue;
            }

            // Resume the thread, ensuring that the schedulers
            // current thread id is set correctly for error catching
            self.state.set_current_thread_id(Some(thread_id));
            let res = thread.resume::<_, LuaMultiValue>(args);
            self.state.set_current_thread_id(None);

            count += 1;

            // If we got any resumption (lua-side) error, increment
            // the error count of the scheduler so we can exit with
            // a non-zero exit code, and print it out to stderr
            if let Err(err) = &res {
                self.state.increment_error_count();
                lua.emit_error(err.clone());
            }

            // If the thread has finished running completely,
            // send results of final resume to any listeners
            if thread.status() != LuaThreadStatus::Resumable {
                // NOTE: Threads that were spawned to resume
                // with an error will not have a result sender
                if let Some(sender) = self
                    .thread_senders
                    .try_lock()
                    .expect("Failed to get thread senders")
                    .remove(&thread_id)
                {
                    if sender.receiver_count() > 0 {
                        let stored = match res {
                            Err(e) => Err(e),
                            Ok(v) => Ok(Arc::new(lua.create_registry_value(v.into_vec()).expect(
                                "Failed to store thread results in registry - out of memory",
                            ))),
                        };
                        sender
                            .send(stored)
                            .expect("Failed to broadcast thread results");
                    }
                }
            }

            if self.state.has_exit_code() {
                break;
            }
        }

        if count > 0 {
            debug! {
                %count,
                "resumed lua"
            }
        }
    }

    /**
        Runs the scheduler to completion in a [`LocalSet`],
        both normal lua threads and futures, prioritizing
        lua threads over completion of any pending futures.

        Will emit lua output and errors to stdout and stderr.
    */
    pub fn run_to_completion(&self, lua: &Lua) -> ExitCode {
        let state = self.state.clone();
        if let Some(code) = state.exit_code() {
            return ExitCode::from(code);
        }

        let _rt_guard = self.runtime.enter();
        let _set_guard = self.local_set.enter();

        loop {
            // 1. Run lua threads until exit or there are none left
            self.run_lua_threads(lua);

            // 2. If we got a manual exit code from lua we should
            // not try to wait for any pending futures to complete
            if state.has_exit_code() {
                break;
            }

            // 3. Keep resuming futures until there are no futures left to
            // resume, or until we manually break out of resumption for any
            // reason, this may be because a future spawned a new lua thread
            if state.has_futures() {
                let mut rx = self.state.message_receiver();
                self.runtime.block_on(self.local_set.run_until(async {
                    while let Some(_msg) = rx.recv().await {
                        if self.has_thread() || !state.has_futures() || state.has_exit_code() {
                            break;
                        }
                    }
                }));
            }

            // 4. If we have an exit code, or if we don't have any lua
            // threads or futures left, we have now run to completion
            if state.has_exit_code() || (!self.has_thread() && !state.has_futures()) {
                break;
            }
        }

        if let Some(code) = state.exit_code() {
            debug! {
                %code,
                "scheduler ran to completion"
            };
            ExitCode::from(code)
        } else if state.has_errored() {
            debug!("scheduler ran to completion, with failure");
            ExitCode::FAILURE
        } else {
            debug!("scheduler ran to completion, with success");
            ExitCode::SUCCESS
        }
    }
}
