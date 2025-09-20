use anyhow::{Context, Result};
use object_store::path::Path as ObjPath;
use object_store::DynObjectStore;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::Arc;
use tracing::debug;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageOptions {
    pub concurrency: Option<usize>,
    pub timeout_secs: Option<u64>,
    pub profile: Option<String>,
    pub role_arn: Option<String>,
    pub region: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ParsedUri {
    pub url: Url,
    pub root: ObjPath,
}

pub fn parse_uri(uri: &str) -> Result<ParsedUri> {
    let url = if uri.starts_with("s3://") || uri.starts_with("gs://") || uri.starts_with("az://") || uri.starts_with("abfs://") || uri.starts_with("file://") {
        Url::parse(uri)?
    } else if uri.starts_with('/') || uri.chars().nth(1) == Some(':') {
        Url::from_file_path(uri).map_err(|_| anyhow::anyhow!("invalid file path"))?
    } else {
        Url::from_file_path(uri).map_err(|_| anyhow::anyhow!("invalid file path"))?
    };
    let root_path = match url.scheme() {
        "s3" | "gs" | "az" | "abfs" | "file" => {
            let p = url.path().trim_start_matches('/');
            ObjPath::from(p)
        }
        _ => ObjPath::from(url.path().trim_start_matches('/')),
    };
    Ok(ParsedUri { url, root: root_path })
}

pub async fn make_object_store(uri: &str, opts: &StorageOptions) -> Result<Arc<DynObjectStore>> {
    let parsed = parse_uri(uri)?;
    let store: Arc<DynObjectStore> = match parsed.url.scheme() {
        "s3" => {
            #[cfg(feature = "s3")]
            {
                use object_store::aws::AmazonS3Builder;
                let mut builder = AmazonS3Builder::from_env();
                if let Some(region) = &opts.region { builder = builder.with_region(region); }
                if let Some(profile) = &opts.profile { std::env::set_var("AWS_PROFILE", profile); }
                if let Some(role) = &opts.role_arn { std::env::set_var("AWS_ROLE_ARN", role); }
                let base = builder.build()?;
                Arc::new(base)
            }
            #[cfg(not(feature = "s3"))]
            {
                anyhow::bail!("s3 feature not enabled")
            }
        }
        "gs" => {
            #[cfg(feature = "gcs")]
            {
                use object_store::gcp::GoogleCloudStorageBuilder;
                Arc::new(GoogleCloudStorageBuilder::from_env().build()?)
            }
            #[cfg(not(feature = "gcs"))]
            {
                anyhow::bail!("gcs feature not enabled")
            }
        }
        "az" | "abfs" => {
            #[cfg(feature = "azure")]
            {
                use object_store::azure::MicrosoftAzureBuilder;
                Arc::new(MicrosoftAzureBuilder::from_env().build()?)
            }
            #[cfg(not(feature = "azure"))]
            {
                anyhow::bail!("azure feature not enabled")
            }
        }
        "file" => {
            Arc::new(object_store::local::LocalFileSystem::new())
        }
        _ => {
            debug!(scheme = parsed.url.scheme(), "assuming local filesystem for unknown scheme");
            Arc::new(object_store::local::LocalFileSystem::new())
        }
    };
    Ok(store)
}

pub async fn list_recursively(
    store: Arc<DynObjectStore>,
    prefix: &ObjPath,
) -> Result<Vec<object_store::ObjectMeta>> {
    use futures::StreamExt;
    let mut entries = Vec::new();
    let mut stream = store.list(Some(prefix));
    while let Some(item) = stream.next().await {
        let meta = item?;
        if meta.location.as_ref().ends_with('/') {
            continue;
        }
        entries.push(meta);
    }
    Ok(entries)
}

pub async fn head_range(
    store: Arc<DynObjectStore>,
    location: &ObjPath,
    range: Range<usize>,
) -> Result<bytes::Bytes> {
    let data = store.get_range(location, range).await?;
    Ok(data)
}

pub fn object_path_from_url(url: &Url) -> ObjPath {
    let p = url.path().trim_start_matches('/');
    ObjPath::from(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_file_uri() {
        let p = parse_uri("/tmp/table").unwrap();
        assert_eq!(p.url.scheme(), "file");
        assert_eq!(p.root.as_ref(), "tmp/table");
    }

    #[test]
    fn test_parse_s3_uri() {
        let p = parse_uri("s3://bucket/path/to/table").unwrap();
        assert_eq!(p.url.scheme(), "s3");
        assert_eq!(p.root.as_ref(), "path/to/table");
    }
}


