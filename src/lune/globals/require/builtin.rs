use mlua::prelude::*;

use super::context::*;

pub(super) async fn require<'lua>(
    lua: &'lua Lua,
    ctx: &RequireContext,
    name: &str,
) -> LuaResult<LuaMultiValue<'lua>> {
    ctx.load_builtin(lua, name)
}
