use std::{fmt, path::PathBuf, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::fs;
use xlake_ast::{Object, PlanArguments, PlanKind};
use xlake_core::{models::hash::Hash, PipeNodeFactory, PipeNodeImpl, PipeStore};

#[derive(Copy, Clone, Debug, Default)]
pub struct LocalStoreFactory;

impl fmt::Display for LocalStoreFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeFactory for LocalStoreFactory {
    fn kind(&self) -> PlanKind {
        PlanKind::Store { name: self.name() }
    }

    fn name(&self) -> String {
        "local".into()
    }

    async fn build(&self, args: &PlanArguments) -> Result<PipeNodeImpl> {
        let imp: LocalStore = args.to()?;
        imp.init().await?;
        Ok(PipeNodeImpl::Store(Arc::new(imp)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LocalStore {
    #[serde(default = "LocalStore::default_path")]
    path: PathBuf,
}

impl LocalStore {
    fn default_path() -> PathBuf {
        "caches".parse().unwrap()
    }
}

impl Default for LocalStore {
    fn default() -> Self {
        Self {
            path: Self::default_path(),
        }
    }
}

impl LocalStore {
    async fn init(&self) -> Result<()> {
        let Self { path } = self;
        fs::create_dir_all(path).await?;
        Ok(())
    }

    fn path(&self, hash: &Hash) -> PathBuf {
        self.path.join(format!("{hash}.json"))
    }
}

#[async_trait]
impl PipeStore for LocalStore {
    async fn contains(&self, hash: &Hash) -> Result<bool> {
        fs::try_exists(self.path(hash)).await.map_err(Into::into)
    }

    async fn read_item(&self, hash: &Hash) -> Result<Object> {
        let buf = fs::read(self.path(hash)).await?;
        ::serde_json::from_slice(&buf).map_err(Into::into)
    }

    async fn write_item(&self, hash: &Hash, object: &Object) -> Result<()> {
        let contents = object.to_vec()?;
        fs::write(self.path(hash), contents)
            .await
            .map_err(Into::into)
    }
}
