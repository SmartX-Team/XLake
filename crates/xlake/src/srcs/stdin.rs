use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncReadExt};
use xlake_ast::{PlanArguments, PlanKind};
use xlake_core::{
    models::hash::HashModelView, PipeChannel, PipeEdge, PipeNodeBuilder, PipeNodeImpl, PipeSrc,
};

use crate::models::builtins::doc::DocModelObject;

#[derive(Copy, Clone, Debug, Default)]
pub struct StdinSrcBuilder;

impl fmt::Display for StdinSrcBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeBuilder for StdinSrcBuilder {
    fn kind(&self) -> PlanKind {
        PlanKind::Src {
            name: "stdin".into(),
        }
    }

    fn output(&self) -> PipeEdge {
        PipeEdge {
            format: Some("stream".into()),
            model: Some(vec!["doc".into(), "hash".into()]),
        }
    }

    async fn build(&self, _args: &PlanArguments) -> Result<PipeNodeImpl> {
        Ok(PipeNodeImpl::Src(Box::new(StdinSrc)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StdinSrc;

#[async_trait]
impl PipeSrc for StdinSrc {
    async fn call(&self) -> Result<PipeChannel> {
        let mut document = String::new();
        io::stdin().read_to_string(&mut document).await?;

        let item = DocModelObject { document };
        let item = HashModelView::try_from(item)?;
        PipeChannel::stream_unit(item)
    }
}
