use std::{fmt, path::PathBuf};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::fs;
use xlake_ast::{PlanArguments, PlanKind};
use xlake_core::{
    models::hash::HashModelView, PipeChannel, PipeEdge, PipeNodeBuilder, PipeNodeImpl, PipeSrc,
};

use crate::models::{binary::BinaryModelObject, file::FileModelView};

#[derive(Copy, Clone, Debug, Default)]
pub struct FileSrcBuilder;

impl fmt::Display for FileSrcBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeBuilder for FileSrcBuilder {
    fn kind(&self) -> PlanKind {
        PlanKind::Src {
            name: "file".into(),
        }
    }

    fn output(&self) -> PipeEdge {
        PipeEdge {
            format: Some("stream".into()),
            model: Some(vec!["binary".into(), "file".into(), "hash".into()]),
        }
    }

    async fn build(&self, args: &PlanArguments) -> Result<PipeNodeImpl> {
        let imp: FileSrc = args.to()?;
        Ok(PipeNodeImpl::Src(Box::new(imp)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileSrc {
    #[serde(default)]
    cache: FileCacheType,
    path: PathBuf,
}

#[async_trait]
impl PipeSrc for FileSrc {
    async fn call(&self) -> Result<PipeChannel> {
        let Self { cache, path } = self;
        let path = fs::canonicalize(path).await?;
        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or_default();

        let content = fs::read(&path).await?.into();

        let item = BinaryModelObject { content };
        let item = match cache {
            FileCacheType::Content => HashModelView::try_from(item)?,
            FileCacheType::Path => HashModelView::try_new(item, &path)?,
        };
        let item = FileModelView::new(item, extension.into());
        PipeChannel::stream_unit(item)
    }
}

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FileCacheType {
    #[default]
    Content,
    Path,
}
