use std::pin::Pin;

use futures_util::Future;
use mlua::prelude::*;
use tokio::task::{self, JoinHandle};

use super::{IntoLuaThread, Scheduler};

type FutureSend<'fut, O> = Pin<Box<dyn Future<Output = O> + Send + 'fut>>;
type FutureNonSend<'fut, O> = Pin<Box<dyn Future<Output = O> + 'fut>>;

impl Scheduler {
    pub fn spawn<'fut, F>(&self, fut: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'fut,
        F::Output: Send + 'static,
    {
        /*
            SAFETY: The scheduler struct manages the tokio runtime,
            when the scheduler gets dropped so does the runtime, hence
            it is guaranteed that no futures live longer than scheduler
        */
        let box_fut: FutureSend<'fut, F::Output> = Box::pin(fut);
        let box_fut_2: FutureSend<'static, F::Output> = unsafe { std::mem::transmute(box_fut) };

        let state = self.state.clone();
        let sender = state.message_sender();
        state.increment_future_count();
        sender.send_future_spawned();
        task::spawn(async move {
            let result = box_fut_2.await;
            state.decrement_future_count();
            sender.send_future_completed();
            result
        })
    }

    pub fn spawn_local<'fut, F>(&self, fut: F) -> JoinHandle<F::Output>
    where
        F: Future + 'fut,
        F::Output: 'static,
    {
        /*
            SAFETY: Same as above
        */
        let box_fut: FutureNonSend<'fut, F::Output> = Box::pin(fut);
        let box_fut_2: FutureNonSend<'static, F::Output> = unsafe { std::mem::transmute(box_fut) };

        let state = self.state.clone();
        let sender = state.message_sender();
        state.increment_future_count();
        sender.send_future_spawned();
        task::spawn_local(async move {
            let result = box_fut_2.await;
            state.decrement_future_count();
            sender.send_future_completed();
            result
        })
    }

    /**
        Schedules the given `thread` to run when the given `fut` completes.

        If the given future returns a [`LuaError`], that error will be passed to the given `thread`.
    */
    pub fn spawn_thread<'fut, 'lua, F, FR>(
        &'fut self,
        lua: &'lua Lua,
        thread: impl IntoLuaThread<'lua>,
        fut: F,
    ) -> LuaResult<()>
    where
        'lua: 'fut,
        FR: IntoLuaMulti<'lua>,
        F: Future<Output = LuaResult<FR>> + 'fut,
    {
        let thread = thread.into_lua_thread(lua)?;

        self.spawn_local(async move {
            match fut.await.and_then(|rets| rets.into_lua_multi(lua)) {
                Err(e) => {
                    self.push_err(lua, thread, e)
                        .expect("Failed to schedule future err thread");
                }
                Ok(v) => {
                    self.push_back(lua, thread, v)
                        .expect("Failed to schedule future thread");
                }
            }
        });

        // NOTE: We might be resuming background futures, need to signal that a
        // new background future is ready to break out of futures resumption
        self.state.message_sender().send_future_spawned();

        Ok(())
    }
}
