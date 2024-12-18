use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use xlake_ast::{PlanArguments, PlanKind};
use xlake_core::{PipeChannel, PipeEdge, PipeFunc, PipeNodeBuilder, PipeNodeImpl};

use super::DocModelView;

#[derive(Copy, Clone, Debug, Default)]
pub struct SplitBuilder;

impl fmt::Display for SplitBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeBuilder for SplitBuilder {
    fn kind(&self) -> PlanKind {
        PlanKind::Func {
            model_name: super::consts::NAME.into(),
            func: self.name(),
        }
    }

    fn name(&self) -> String {
        "split".into()
    }

    fn input(&self) -> PipeEdge {
        PipeEdge {
            model: Some(vec![super::consts::NAME.into()]),
            ..Default::default()
        }
    }

    fn output(&self) -> PipeEdge {
        PipeEdge {
            model: Some(vec![super::consts::NAME.into()]),
            ..Default::default()
        }
    }

    async fn build(&self, args: &PlanArguments) -> Result<PipeNodeImpl> {
        let imp: SplitFunc = args.to()?;
        Ok(PipeNodeImpl::Func(Box::new(imp)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SplitFunc {}

#[async_trait]
impl PipeFunc for SplitFunc {
    async fn call(&self, channel: PipeChannel) -> Result<PipeChannel> {
        let mut iter = channel.into_stream::<DocModelView>().await?;
        while let Some(item) = iter.next().await {}
        todo!()
    }
}
