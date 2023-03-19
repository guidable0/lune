use std::{
    fmt,
    sync::{Arc, RwLock},
};

use mlua::prelude::*;
use rbx_dom_weak::{
    types::{Ref as DomRef, Variant as DomValue},
    Instance as DomInstance, InstanceBuilder as DomInstanceBuilder, WeakDom,
};

use crate::{
    datatypes::{
        conversion::{DomValueToLua, LuaToDomValue},
        types::EnumItem,
        userdata_impl_eq, userdata_impl_to_string,
    },
    shared::instance::{class_exists, class_is_a, find_property_info},
};

#[derive(Debug, Clone)]
pub struct Instance {
    pub(crate) dom: Arc<RwLock<WeakDom>>,
    pub(crate) dom_ref: DomRef,
    pub(crate) class_name: String,
    pub(crate) is_root: bool,
    pub(crate) is_destroyed: bool,
}

impl Instance {
    /**
        Creates a new `Instance` from a document and dom object ref.
    */
    pub fn new(dom: &Arc<RwLock<WeakDom>>, dom_ref: DomRef) -> Self {
        let reader = dom.read().expect("Failed to get read access to document");
        let instance = reader
            .get_by_ref(dom_ref)
            .expect("Failed to find instance in document");
        Self {
            dom: Arc::clone(dom),
            dom_ref,
            class_name: instance.class.clone(),
            is_root: dom_ref == reader.root_ref(),
            is_destroyed: false,
        }
    }

    /**
        Creates a new orphaned `Instance` with a given class name.

        An orphaned instance does not belong to any particular document and
        is instead part of the internal weak dom for orphaned lua instances,
        it can however be re-parented to a "real" document and weak dom.
    */
    pub fn new_orphaned(lua: &Lua, class_name: impl AsRef<str>) -> Self {
        let dom_lua = lua
            .app_data_mut::<Arc<RwLock<WeakDom>>>()
            .expect("Failed to find internal lua weak dom");
        let mut dom = dom_lua
            .write()
            .expect("Failed to get write access to document");

        let class_name = class_name.as_ref();
        let dom_root = dom.root_ref();
        let dom_ref = dom.insert(dom_root, DomInstanceBuilder::new(class_name.to_string()));

        Self {
            dom: Arc::clone(&dom_lua),
            dom_ref,
            class_name: class_name.to_string(),
            is_root: false,
            is_destroyed: false,
        }
    }

    /**
        Clones the instance and all of its descendants, and orphans it.

        To then save the new instance it must be re-parented,
        which matches the exact behavior of Roblox's instances.
    */
    pub fn clone_instance(&self, lua: &Lua) -> Instance {
        // NOTE: We create a new scope here to avoid deadlocking since
        // our clone implementation must have exclusive write access
        let parent_ref = {
            self.dom
                .read()
                .expect("Failed to get read access to document")
                .get_by_ref(self.dom_ref)
                .expect("Failed to find instance in document")
                .parent()
        };
        let new_ref = Self::clone_inner(lua, self.dom_ref, parent_ref);
        let new_inst = Self::new(&self.dom, new_ref);
        new_inst.set_parent_to_nil(lua);
        new_inst
    }

    pub fn clone_inner(lua: &Lua, dom_ref: DomRef, parent_ref: DomRef) -> DomRef {
        // NOTE: We create a new scope here to avoid deadlocking since
        // our clone implementation must have exclusive write access
        let (new_ref, child_refs) = {
            let dom_lua = lua
                .app_data_mut::<Arc<RwLock<WeakDom>>>()
                .expect("Failed to find internal lua weak dom");
            let mut dom = dom_lua
                .try_write()
                .expect("Failed to get write access to document");

            let (new_class, new_name, new_props, child_refs) = {
                let instance = dom
                    .get_by_ref(dom_ref)
                    .expect("Failed to find instance in document");
                (
                    instance.class.to_string(),
                    instance.name.to_string(),
                    instance.properties.clone(),
                    instance.children().to_vec(),
                )
            };

            let new_ref = dom.insert(
                parent_ref,
                DomInstanceBuilder::new(new_class)
                    .with_name(new_name)
                    .with_properties(new_props),
            );

            (new_ref, child_refs)
        };

        for child_ref in child_refs {
            Self::clone_inner(lua, child_ref, new_ref);
        }

        new_ref
    }

    /**
        Destroys the instance, unless it is the root instance, removing
        it completely from the weak dom with no way of recovering it.

        All member methods will throw errors when called from lua and panic
        when called from rust after the instance has been destroyed.

        Returns `true` if destroyed successfully, `false` if already destroyed.
    */
    pub fn destroy(&mut self) -> bool {
        if self.is_root || self.is_destroyed {
            false
        } else {
            let mut dom = self
                .dom
                .try_write()
                .expect("Failed to get write access to document");
            dom.destroy(self.dom_ref);
            self.is_destroyed = true;
            true
        }
    }

    fn ensure_not_destroyed(&self) -> LuaResult<()> {
        if self.is_destroyed {
            Err(LuaError::RuntimeError(format!(
                "Tried to access destroyed instance '{}'",
                self
            )))
        } else {
            Ok(())
        }
    }

    /**
        Checks if the instance matches or inherits a given class name.
    */
    pub fn is_a(&self, class_name: impl AsRef<str>) -> bool {
        class_is_a(&self.class_name, class_name).unwrap_or(false)
    }

    /**
        Gets the name of the instance, if it exists.
    */
    pub fn get_name(&self) -> String {
        let dom = self
            .dom
            .read()
            .expect("Failed to get read access to document");
        dom.get_by_ref(self.dom_ref)
            .expect("Failed to find instance in document")
            .name
            .clone()
    }

    /**
        Sets the name of the instance, if it exists.
    */
    pub fn set_name(&self, name: impl Into<String>) {
        let mut dom = self
            .dom
            .write()
            .expect("Failed to get write access to document");
        dom.get_by_ref_mut(self.dom_ref)
            .expect("Failed to find instance in document")
            .name = name.into()
    }

    /**
        Gets the parent of the instance, if it exists.
    */
    pub fn get_parent(&self) -> Option<Instance> {
        let dom = self
            .dom
            .read()
            .expect("Failed to get read access to document");
        let parent_ref = dom
            .get_by_ref(self.dom_ref)
            .expect("Failed to find instance in document")
            .parent();
        if parent_ref == dom.root_ref() {
            None
        } else {
            Some(Self::new(&self.dom, parent_ref))
        }
    }

    /**
        Sets the parent of the instance, if it exists.

        Note that this can transfer between different weak doms,
        and assumes that separate doms always have unique root referents.

        If doms do not have unique root referents then this operation may panic.
    */
    pub fn set_parent(&self, parent: Instance) {
        let mut dom_source = self
            .dom
            .write()
            .expect("Failed to get read access to source document");
        let dom_target = parent
            .dom
            .read()
            .expect("Failed to get read access to target document");
        let target_ref = dom_target
            .get_by_ref(parent.dom_ref)
            .expect("Failed to find instance in target document")
            .parent();
        if dom_source.root_ref() == dom_target.root_ref() {
            dom_source.transfer_within(self.dom_ref, target_ref);
        } else {
            // NOTE: We must drop the previous dom_target read handle here first so
            // that we can get exclusive write access for transferring across doms
            drop(dom_target);
            let mut dom_target = parent
                .dom
                .try_write()
                .expect("Failed to get write access to target document");
            dom_source.transfer(self.dom_ref, &mut dom_target, target_ref)
        }
    }

    /**
        Sets the parent of the instance, if it exists, to nil, making it orphaned.

        An orphaned instance does not belong to any particular document and
        is instead part of the internal weak dom for orphaned lua instances,
        it can however be re-parented to a "real" document and weak dom.
    */
    pub fn set_parent_to_nil(&self, lua: &Lua) {
        let mut dom_source = self
            .dom
            .write()
            .expect("Failed to get read access to source document");
        let dom_lua = lua
            .app_data_mut::<Arc<RwLock<WeakDom>>>()
            .expect("Failed to find internal lua weak dom");
        let mut dom_target = dom_lua
            .write()
            .expect("Failed to get write access to target document");
        let target_ref = dom_target.root_ref();
        dom_source.transfer(self.dom_ref, &mut dom_target, target_ref)
    }

    /**
        Gets a property for the instance, if it exists.
    */
    pub fn get_property(&self, name: impl AsRef<str>) -> Option<DomValue> {
        self.dom
            .read()
            .expect("Failed to get read access to document")
            .get_by_ref(self.dom_ref)
            .expect("Failed to find instance in document")
            .properties
            .get(name.as_ref())
            .cloned()
    }

    /**
        Sets a property for the instance.

        Note that setting a property here will not fail even if the
        property does not actually exist for the instance class.
    */
    pub fn set_property(&self, name: impl AsRef<str>, value: DomValue) {
        self.dom
            .write()
            .expect("Failed to get read access to document")
            .get_by_ref_mut(self.dom_ref)
            .expect("Failed to find instance in document")
            .properties
            .insert(name.as_ref().to_string(), value);
    }

    /**
        Finds a child of the instance using the given predicate callback.
    */
    pub fn find_child<F>(&self, predicate: F) -> Option<Instance>
    where
        F: Fn(&DomInstance) -> bool,
    {
        let dom = self
            .dom
            .read()
            .expect("Failed to get read access to document");
        let children = dom
            .get_by_ref(self.dom_ref)
            .expect("Failed to find instance in document")
            .children();
        children.iter().find_map(|child_ref| {
            if let Some(child_inst) = dom.get_by_ref(*child_ref) {
                if predicate(child_inst) {
                    Some(Self::new(&self.dom, *child_ref))
                } else {
                    None
                }
            } else {
                None
            }
        })
    }

    /**
        Finds an ancestor of the instance using the given predicate callback.
    */
    pub fn find_ancestor<F>(&self, predicate: F) -> Option<Instance>
    where
        F: Fn(&DomInstance) -> bool,
    {
        let dom = self
            .dom
            .read()
            .expect("Failed to get read access to document");
        let mut ancestor_ref = dom
            .get_by_ref(self.dom_ref)
            .expect("Failed to find instance in document")
            .parent();
        while let Some(ancestor) = dom.get_by_ref(ancestor_ref) {
            if predicate(ancestor) {
                return Some(Self::new(&self.dom, ancestor_ref));
            } else {
                ancestor_ref = ancestor.parent();
            }
        }
        None
    }
}

impl Instance {
    pub(crate) fn make_table(lua: &Lua, datatype_table: &LuaTable) -> LuaResult<()> {
        datatype_table.set(
            "new",
            lua.create_function(|lua, class_name: String| {
                if class_exists(&class_name) {
                    Instance::new_orphaned(lua, class_name).to_lua(lua)
                } else {
                    Err(LuaError::RuntimeError(format!(
                        "{} is not a valid class name",
                        class_name
                    )))
                }
            })?,
        )
    }
}

impl LuaUserData for Instance {
    fn add_methods<'lua, M: LuaUserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(LuaMetaMethod::ToString, userdata_impl_to_string);
        methods.add_meta_method(LuaMetaMethod::Eq, userdata_impl_eq);
        /*
            Getting a value does the following:

            1. Check if it is a special property like "ClassName", "Name" or "Parent"
            2. Check if a property exists for the wanted name
                2a. Get an existing instance property OR
                2b. Get a property from a known default value
            3. Get a current child of the instance
            4. No valid property or instance found, throw error
        */
        methods.add_meta_method(LuaMetaMethod::Index, |lua, this, prop_name: String| {
            this.ensure_not_destroyed()?;

            match prop_name.as_str() {
                "ClassName" => return this.class_name.clone().to_lua(lua),
                "Name" => {
                    return this.get_name().to_lua(lua);
                }
                "Parent" => {
                    return this.get_parent().to_lua(lua);
                }
                _ => {}
            }

            if let Some(info) = find_property_info(&this.class_name, &prop_name) {
                if let Some(prop) = this.get_property(&prop_name) {
                    if let DomValue::Enum(enum_value) = prop {
                        let enum_name = info.enum_name.ok_or_else(|| {
                            LuaError::RuntimeError(format!(
                                "Failed to get property '{}' - encountered unknown enum",
                                prop_name
                            ))
                        })?;
                        EnumItem::from_enum_name_and_value(&enum_name, enum_value.to_u32())
                            .ok_or_else(|| {
                                LuaError::RuntimeError(format!(
                                    "Failed to get property '{}' - Enum.{} does not contain numeric value {}",
                                    prop_name, enum_name, enum_value.to_u32()
                                ))
                            })?
                            .to_lua(lua)
                    } else {
                        Ok(LuaValue::dom_value_to_lua(lua, &prop)?)
                    }
                } else if let (Some(enum_name), Some(enum_value)) = (info.enum_name, info.enum_default) {
                    EnumItem::from_enum_name_and_value(&enum_name, enum_value)
                        .ok_or_else(|| {
                            LuaError::RuntimeError(format!(
                                "Failed to get property '{}' - Enum.{} does not contain numeric value {}",
                                prop_name, enum_name, enum_value
                            ))
                        })?
                        .to_lua(lua)
                } else if let Some(prop_default) = info.value_default {
                    Ok(LuaValue::dom_value_to_lua(lua, prop_default)?)
                } else {
                    Err(LuaError::RuntimeError(format!(
                        "Failed to get property '{}' - malformed property info",
                        prop_name
                    )))
                }
            } else if let Some(inst) = this.find_child(|inst| inst.name == prop_name) {
                Ok(LuaValue::UserData(lua.create_userdata(inst)?))
            } else {
                Err(LuaError::RuntimeError(format!(
                    "{} is not a valid member of {}",
                    prop_name, this
                )))
            }
        });
        /*
            Setting a value does the following:

            1. Check if it is a special property like "ClassName", "Name" or "Parent"
            2. Check if a property exists for the wanted name
                2a. Set a strict enum from a given EnumItem OR
                2b. Set a normal property from a given value
        */
        methods.add_meta_method_mut(
            LuaMetaMethod::NewIndex,
            |lua, this, (prop_name, prop_value): (String, LuaValue)| {
                this.ensure_not_destroyed()?;

                match prop_name.as_str() {
                    "ClassName" => {
                        return Err(LuaError::RuntimeError(
                            "ClassName can not be written to".to_string(),
                        ))
                    }
                    "Name" => {
                        let name = String::from_lua(prop_value, lua)?;
                        this.set_name(name);
                        return Ok(());
                    }
                    "Parent" => {
                        type Parent = Option<Instance>;
                        match Parent::from_lua(prop_value, lua)? {
                            Some(parent) => this.set_parent(parent),
                            None => this.set_parent_to_nil(lua),
                        }
                        return Ok(());
                    }
                    _ => {}
                }

                let info = match find_property_info(&this.class_name, &prop_name) {
                    Some(b) => b,
                    None => {
                        return Err(LuaError::RuntimeError(format!(
                            "{} is not a valid member of {}",
                            prop_name, this
                        )))
                    }
                };

                if let Some(enum_name) = info.enum_name {
                    match EnumItem::from_lua(prop_value, lua) {
                        Ok(given_enum) if given_enum.name == enum_name => {
                            this.set_property(prop_name, DomValue::Enum(given_enum.into()));
                            Ok(())
                        }
                        Ok(given_enum) => Err(LuaError::RuntimeError(format!(
                            "Failed to set property '{}' - expected Enum.{}, got Enum.{}",
                            prop_name, enum_name, given_enum.name
                        ))),
                        Err(e) => Err(e),
                    }
                } else if let Some(dom_type) = info.value_type {
                    match prop_value.lua_to_dom_value(lua, dom_type) {
                        Ok(dom_value) => {
                            this.set_property(prop_name, dom_value);
                            Ok(())
                        }
                        Err(e) => Err(e.into()),
                    }
                } else {
                    Err(LuaError::RuntimeError(format!(
                        "Failed to set property '{}' - malformed property info",
                        prop_name
                    )))
                }
            },
        );
        /*
            Implementations of base methods on the Instance class

            Currently implemented:

            * Clone
            * Destroy

            * FindFirstAncestor
            * FindFirstAncestorOfClass
            * FindFirstAncestorWhichIsA
            * FindFirstChild
            * FindFirstChildOfClass
            * FindFirstChildWhichIsA

            * IsAncestorOf
            * IsDescendantOf

            Not yet implemented, but planned:

            * FindFirstDescendant
            * GetChildren
            * GetDescendants
            * GetFullName
            * GetAttribute
            * GetAttributes
            * SetAttribute
        */
        methods.add_method("Clone", |lua, this, ()| {
            this.ensure_not_destroyed()?;
            this.clone_instance(lua).to_lua(lua)
        });
        methods.add_method_mut("Destroy", |_, this, ()| {
            this.destroy();
            Ok(())
        });
        methods.add_method("FindFirstAncestor", |lua, this, name: String| {
            this.ensure_not_destroyed()?;
            this.find_ancestor(|child| child.name == name).to_lua(lua)
        });
        methods.add_method(
            "FindFirstAncestorOfClass",
            |lua, this, class_name: String| {
                this.ensure_not_destroyed()?;
                this.find_ancestor(|child| child.class == class_name)
                    .to_lua(lua)
            },
        );
        methods.add_method(
            "FindFirstAncestorWhichIsA",
            |lua, this, class_name: String| {
                this.ensure_not_destroyed()?;
                this.find_ancestor(|child| class_is_a(&child.class, &class_name).unwrap_or(false))
                    .to_lua(lua)
            },
        );
        methods.add_method("FindFirstChild", |lua, this, name: String| {
            this.ensure_not_destroyed()?;
            this.find_child(|child| child.name == name).to_lua(lua)
        });
        methods.add_method("FindFirstChildOfClass", |lua, this, class_name: String| {
            this.ensure_not_destroyed()?;
            this.find_child(|child| child.class == class_name)
                .to_lua(lua)
        });
        methods.add_method("FindFirstChildWhichIsA", |lua, this, class_name: String| {
            this.ensure_not_destroyed()?;
            this.find_child(|child| class_is_a(&child.class, &class_name).unwrap_or(false))
                .to_lua(lua)
        });
        methods.add_method("IsAncestorOf", |_, this, instance: Instance| {
            this.ensure_not_destroyed()?;
            Ok(instance
                .find_ancestor(|ancestor| ancestor.referent() == this.dom_ref)
                .is_some())
        });
        methods.add_method("IsDescendantOf", |_, this, instance: Instance| {
            this.ensure_not_destroyed()?;
            Ok(this
                .find_ancestor(|ancestor| ancestor.referent() == instance.dom_ref)
                .is_some())
        });
        // FUTURE: We could pass the "methods" struct to some other functions
        // here to add inheritance-like behavior and class-specific methods
    }
}

impl fmt::Display for Instance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get_name())
    }
}

impl PartialEq for Instance {
    fn eq(&self, other: &Self) -> bool {
        self.dom_ref == other.dom_ref
    }
}
