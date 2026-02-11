use std::num::NonZeroUsize;
use std::sync::Mutex;

use lru::LruCache;
use serde_json::{json, Value};
use sui_types::base_types::ObjectID;
use sui_types::object::Object;

const DEFAULT_CACHE_CAPACITY: usize = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VersionQuery {
    AtCheckpoint(u64),
    ExactVersion(u64),
    RootVersion(u64),
}

#[derive(Debug)]
pub struct GraphqlClient {
    client: reqwest::Client,
    url: String,
    cache: Mutex<LruCache<(ObjectID, VersionQuery), Option<Object>>>,
}

impl GraphqlClient {
    pub fn new(url: String, cache_capacity: usize) -> Self {
        let capacity = if cache_capacity == 0 {
            DEFAULT_CACHE_CAPACITY
        } else {
            cache_capacity
        };
        Self {
            client: reqwest::Client::new(),
            url,
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).expect("cache capacity must be > 0"),
            )),
        }
    }

    /// Execute a GraphQL query synchronously. Uses a scoped thread with a
    /// separate current-thread tokio runtime to bridge async reqwest into the
    /// synchronous Sui storage trait methods. This avoids deadlocks from nested
    /// calls and works regardless of the caller's async context.
    fn query_graphql(&self, query: &str, variables: Value) -> Result<Value, GraphqlError> {
        let body = json!({
            "query": query,
            "variables": variables,
        });

        let fut = async {
            let resp = self
                .client
                .post(&self.url)
                .json(&body)
                .send()
                .await
                .map_err(GraphqlError::Network)?;

            let status = resp.status();
            let json: Value = resp.json().await.map_err(GraphqlError::Network)?;

            if let Some(errors) = json.get("errors") {
                return Err(GraphqlError::Api(format!(
                    "GraphQL errors (HTTP {}): {}",
                    status, errors
                )));
            }

            json.get("data")
                .cloned()
                .ok_or_else(|| GraphqlError::Api("missing 'data' field in response".into()))
        };

        block_on(fut)
    }

    fn decode_object_bcs(value: &Value) -> Result<Option<Object>, GraphqlError> {
        let obj = match value {
            Value::Null => return Ok(None),
            Value::Object(obj) => obj,
            _ => return Err(GraphqlError::Api("expected object or null".into())),
        };

        let bcs_b64 = obj
            .get("objectBcs")
            .and_then(|v| v.as_str())
            .ok_or_else(|| GraphqlError::Api("missing objectBcs field".into()))?;

        use fastcrypto::encoding::{Base64, Encoding};
        let bcs_bytes = Base64::decode(bcs_b64)
            .map_err(|e| GraphqlError::Api(format!("base64 decode error: {}", e)))?;

        let object: Object = bcs::from_bytes(&bcs_bytes)
            .map_err(|e| GraphqlError::Api(format!("BCS decode error: {}", e)))?;

        Ok(Some(object))
    }

    fn cached_fetch(
        &self,
        id: ObjectID,
        vq: VersionQuery,
        fetch: impl FnOnce() -> Result<Option<Object>, GraphqlError>,
    ) -> Result<Option<Object>, GraphqlError> {
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some(cached) = cache.get(&(id, vq)) {
                return Ok(cached.clone());
            }
        }

        let result = fetch()?;

        {
            let mut cache = self.cache.lock().unwrap();
            cache.put((id, vq), result.clone());
        }

        Ok(result)
    }

    pub fn fetch_object_at_checkpoint(
        &self,
        id: ObjectID,
        checkpoint: u64,
    ) -> Result<Option<Object>, GraphqlError> {
        let vq = VersionQuery::AtCheckpoint(checkpoint);
        self.cached_fetch(id, vq, || {
            let data = self.query_graphql(
                "query ($addr: SuiAddress!, $cp: UInt53) {
                    object(address: $addr, atCheckpoint: $cp) { objectBcs }
                }",
                json!({ "addr": id.to_hex_uncompressed(), "cp": checkpoint }),
            )?;
            Self::decode_object_bcs(&data["object"])
        })
    }

    pub fn fetch_object_at_version(
        &self,
        id: ObjectID,
        version: u64,
    ) -> Result<Option<Object>, GraphqlError> {
        let vq = VersionQuery::ExactVersion(version);
        self.cached_fetch(id, vq, || {
            let data = self.query_graphql(
                "query ($addr: SuiAddress!, $ver: UInt53) {
                    object(address: $addr, version: $ver) { objectBcs }
                }",
                json!({ "addr": id.to_hex_uncompressed(), "ver": version }),
            )?;
            Self::decode_object_bcs(&data["object"])
        })
    }

    pub fn fetch_object_at_root_version(
        &self,
        id: ObjectID,
        root_version: u64,
    ) -> Result<Option<Object>, GraphqlError> {
        let vq = VersionQuery::RootVersion(root_version);
        self.cached_fetch(id, vq, || {
            let data = self.query_graphql(
                "query ($addr: SuiAddress!, $rv: UInt53) {
                    object(address: $addr, rootVersion: $rv) { objectBcs }
                }",
                json!({ "addr": id.to_hex_uncompressed(), "rv": root_version }),
            )?;
            Self::decode_object_bcs(&data["object"])
        })
    }

    pub fn fetch_objects_batch(
        &self,
        keys: &[(ObjectID, u64)],
    ) -> Result<Vec<Option<Object>>, GraphqlError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let gql_keys: Vec<Value> = keys
            .iter()
            .map(|(id, version)| {
                json!({
                    "address": id.to_hex_uncompressed(),
                    "version": version,
                })
            })
            .collect();

        let data = self.query_graphql(
            "query ($keys: [ObjectKey!]!) {
                multiGetObjects(keys: $keys) { objectBcs }
            }",
            json!({ "keys": gql_keys }),
        )?;

        let arr = data["multiGetObjects"]
            .as_array()
            .ok_or_else(|| GraphqlError::Api("multiGetObjects not an array".into()))?;

        let mut results = Vec::with_capacity(arr.len());
        for (i, val) in arr.iter().enumerate() {
            let obj = Self::decode_object_bcs(val)?;
            // Cache exact-version results from batch
            if let Some(ref o) = obj {
                let vq = VersionQuery::ExactVersion(keys[i].1);
                let mut cache = self.cache.lock().unwrap();
                cache.put((keys[i].0, vq), Some(o.clone()));
            }
            results.push(obj);
        }

        Ok(results)
    }
}

#[derive(Debug)]
pub enum GraphqlError {
    Network(reqwest::Error),
    Api(String),
}

impl std::fmt::Display for GraphqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphqlError::Network(e) => write!(f, "GraphQL network error: {}", e),
            GraphqlError::Api(msg) => write!(f, "GraphQL API error: {}", msg),
        }
    }
}

impl std::error::Error for GraphqlError {}

/// Run a future to completion synchronously. If called from within a tokio
/// runtime, spawns a scoped thread with its own current-thread runtime to
/// avoid deadlocks from nested runtimes. Otherwise creates a runtime directly.
/// Adapted from the Sui forking tool's approach.
fn block_on<F>(fut: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build tokio runtime");
                    rt.block_on(fut)
                })
                .join()
                .expect("failed to join scoped thread")
        })
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime");
        rt.block_on(fut)
    }
}
