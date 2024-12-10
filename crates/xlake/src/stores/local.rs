use std::{fmt, path::PathBuf};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::fs;
use xlake_ast::{Object, PlanArguments, PlanKind};
use xlake_core::{PipeNodeBuilder, PipeNodeImpl, PipeStore};

#[derive(Copy, Clone, Debug, Default)]
pub struct LocalStoreBuilder;

impl fmt::Display for LocalStoreBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeBuilder for LocalStoreBuilder {
    fn kind(&self) -> PlanKind {
        PlanKind::Store {
            name: "local".into(),
        }
    }

    async fn build(&self, args: &PlanArguments) -> Result<PipeNodeImpl> {
        let imp: LocalStore = args.to()?;
        imp.init().await?;
        Ok(PipeNodeImpl::Store(Box::new(imp)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LocalStore {
    path: PathBuf,
}

impl LocalStore {
    async fn init(&self) -> Result<()> {
        let Self { path } = self;
        fs::create_dir_all(path).await?;
        Ok(())
    }

    fn path(&self, hash: &str) -> PathBuf {
        self.path.join(format!("{hash}.json"))
    }
}

#[async_trait]
impl PipeStore for LocalStore {
    async fn contains(&self, hash: &str) -> Result<bool> {
        fs::try_exists(self.path(hash)).await.map_err(Into::into)
    }

    async fn read_item(&self, hash: &str) -> Result<Object> {
        let buf = fs::read(self.path(hash)).await?;
        ::serde_json::from_slice(&buf).map_err(Into::into)
    }

    async fn write_item(&self, hash: &str, object: &Object) -> Result<()> {
        let contents = object.to_vec()?;
        fs::write(self.path(hash), contents)
            .await
            .map_err(Into::into)
    }
}
