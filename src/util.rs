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
/// value is present for the environment variable, then the [`FnOnce`] will be invoked to provide a
/// default value.
pub fn read_env<K, D>(key: K, default: D) -> String
where
    K: AsRef<str>,
    D: FnOnce() -> String,
{
    try_read_env(key).unwrap_or_else(default)
}

/// Reads an environment variable with the given key and then invokes the given [`FnOnce`] to
/// transform it to a different type. If no value is present for the environment variable, then
/// the default [`FnOnce`] parameter will be invoked to provide a default value.
pub fn read_env_transformed<K, T, D, V>(key: K, transform: T, default: D) -> V
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
