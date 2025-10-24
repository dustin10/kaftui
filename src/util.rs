use anyhow::Context;

/// Attempts to read an environment variable with the given key returning the value as a
/// [`String`]. If no value is present for the environment variable or there was an error
/// during the attempt to read it, [`None`] is returned.
pub fn try_read_env(key: impl AsRef<str>) -> Option<String> {
    match std::env::var(key.as_ref()) {
        Ok(v) => Some(v),
        Err(std::env::VarError::NotPresent) => {
            tracing::debug!("{} is not set", key.as_ref());
            None
        }
        Err(e) => {
            tracing::error!("error reading {}: {}", key.as_ref(), e);
            None
        }
    }
}

/// Reads an environment variable with the given key returning the value as a [`String`]. If no
/// value is present for the environment variable, then the default will be returned.
pub fn read_env_or<K>(key: K, default: String) -> String
where
    K: AsRef<str>,
{
    read_env_or_else(key, || default)
}

/// Reads an environment variable with the given key returning the value as a [`String`]. If no
/// value is present for the environment variable, then the [`FnOnce`] will be invoked to provide a
/// default value.
pub fn read_env_or_else<K, D>(key: K, default: D) -> String
where
    K: AsRef<str>,
    D: FnOnce() -> String,
{
    try_read_env(key).unwrap_or_else(default)
}

/// Reads an environment variable with the given key and then invokes the given [`FnOnce`] to
/// transform it to a different type. If no value is present for the environment variable, then
/// the default value will be returned.
pub fn read_env_transformed_or<K, T, V>(key: K, transform: T, default: V) -> V
where
    K: AsRef<str>,
    T: FnOnce(String) -> V,
{
    read_env_transformed_or_else(key, transform, || default)
}

/// Reads an environment variable with the given key and then invokes the given [`FnOnce`] to
/// transform it to a different type. If no value is present for the environment variable, then
/// the default [`FnOnce`] parameter will be invoked to provide a default value.
pub fn read_env_transformed_or_else<K, T, D, V>(key: K, transform: T, default: D) -> V
where
    K: AsRef<str>,
    D: FnOnce() -> V,
    T: FnOnce(String) -> V,
{
    match try_read_env(key) {
        Some(v) => transform(v),
        None => default(),
    }
}

/// Recursively finds all files with the given extension in the specified directory and its
/// subdirectories, returning their contents as a vector of strings.
pub fn read_files_recursive(
    dir: impl AsRef<str>,
    target_ext: impl AsRef<str>,
) -> anyhow::Result<Vec<String>> {
    let entries = std::fs::read_dir(dir.as_ref())
        .context(format!("read files from directory {}", dir.as_ref()))?;

    let mut contents: Vec<String> = Vec::new();

    for entry in entries {
        match entry {
            Ok(e) => {
                let path = e.path();

                if path.is_dir() {
                    let child_dir = match path.into_os_string().into_string() {
                        Ok(child_dir) => child_dir,
                        Err(_) => anyhow::bail!("unable to convert proto dir path to string"),
                    };

                    let child_contents = read_files_recursive(child_dir, target_ext.as_ref())
                        .context("recursive find files")?;

                    contents.extend(child_contents);
                } else if let Some(file_ext) = path.extension()
                    && file_ext.to_str() == Some(target_ext.as_ref())
                {
                    match std::fs::read_to_string(path) {
                        Ok(content) => contents.push(content),
                        Err(e) => anyhow::bail!("unable to read proto file: {}", e),
                    }
                }
            }
            Err(e) => anyhow::bail!("unable to read proto file entry: {}", e),
        }
    }

    Ok(contents)
}
