use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncReadExt};
use xlake_ast::{PlanArguments, PlanKind};
use xlake_core::{
    models::hash::HashModelView, PipeChannel, PipeEdge, PipeNodeFactory, PipeNodeImpl, PipeSrc,
};

use crate::models::builtins::doc::DocModelObject;

#[derive(Copy, Clone, Debug, Default)]
pub struct StdinSrcFactory;

impl fmt::Display for StdinSrcFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeFactory for StdinSrcFactory {
    fn kind(&self) -> PlanKind {
        PlanKind::Src { name: self.name() }
    }

    fn name(&self) -> String {
        "stdin".into()
    }

    fn output(&self) -> PipeEdge {
        PipeEdge {
            model: Some(vec!["doc".into(), "hash".into()]),
            ..Default::default()
        }
    }

    async fn build(&self, args: &PlanArguments) -> Result<PipeNodeImpl> {
        let imp: StdinSrc = args.to()?;
        Ok(PipeNodeImpl::Src(Box::new(imp)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StdinSrc {}

#[async_trait]
impl PipeSrc for StdinSrc {
    async fn call(&self) -> Result<PipeChannel> {
        let mut document = String::new();
        io::stdin().read_to_string(&mut document).await?;

        let item = DocModelObject { document };
        let item = HashModelView::try_from(item)?;
        Ok(PipeChannel::from_unit(item))
    }
}
