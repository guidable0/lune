use std::collections::HashMap;
use std::fs::{create_dir_all, write};

use anyhow::{Context, Result};
use directories::UserDirs;
use include_dir::Dir;

pub fn generate_typedef_files_from_definitions(dir: &Dir<'_>) -> Result<String> {
    let contents = read_typedefs_dir_contents(dir);
    write_typedef_files(contents)
}

fn read_typedefs_dir_contents(dir: &Dir<'_>) -> HashMap<String, Vec<u8>> {
    let mut definitions = HashMap::new();

    for entry in dir.find("*.luau").unwrap() {
        let entry_file = entry.as_file().unwrap();
        let entry_name = entry_file.path().file_name().unwrap().to_string_lossy();

        let typedef_name = entry_name.trim_end_matches(".luau");
        let typedef_contents = entry_file.contents().to_vec();

        definitions.insert(typedef_name.to_string(), typedef_contents);
    }

    definitions
}

fn write_typedef_files(typedef_files: HashMap<String, Vec<u8>>) -> Result<String> {
    let version_string = env!("CARGO_PKG_VERSION");
    let mut dirs_to_write = Vec::new();
    let mut files_to_write = Vec::new();
    // Create the typedefs dir in the users cache dir
    let cache_dir = UserDirs::new()
        .context("Failed to find user home directory")?
        .home_dir()
        .join(".lune")
        .join(".typedefs")
        .join(version_string);
    dirs_to_write.push(cache_dir.clone());
    // Make typedef files
    for (builtin_name, builtin_typedef) in typedef_files {
        let path = cache_dir
            .join(builtin_name.to_ascii_lowercase())
            .with_extension("luau");
        files_to_write.push((builtin_name.to_lowercase(), path, builtin_typedef));
    }
    // Write all dirs and files only when we know generation was successful
    for dir in dirs_to_write {
        create_dir_all(dir)?;
    }
    for (_, path, contents) in files_to_write {
        write(path, contents)?;
    }
    Ok(version_string.to_string())
}
