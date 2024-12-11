use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use xlake_ast::{PlanArguments, PlanKind};
use xlake_core::{
    object::LazyObject, PipeChannel, PipeModelOwnedExt, PipeNodeBuilder, PipeNodeImpl, PipeSink,
};

use crate::models::{binary::BinaryModelView, doc::DocModelView};

#[derive(Copy, Clone, Debug, Default)]
pub struct StdoutSinkBuilder;

impl fmt::Display for StdoutSinkBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeBuilder for StdoutSinkBuilder {
    fn kind(&self) -> PlanKind {
        PlanKind::Sink {
            name: "stdout".into(),
        }
    }

    async fn build(&self, _args: &PlanArguments) -> Result<PipeNodeImpl> {
        Ok(PipeNodeImpl::Sink(Box::new(StdoutSink)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StdoutSink;

#[async_trait]
impl PipeSink for StdoutSink {
    async fn call(&self, channel: PipeChannel) -> Result<()> {
        let mut iter = channel.async_iter::<LazyObject>();
        while let Some(item) = iter.next().await {
            let item = item.flatten().await?;
            let item = match item.view::<DocModelView>() {
                Ok(item) => {
                    println!("{item}");
                    continue;
                }
                Err(item) => item,
            };
            let item = match item.view::<BinaryModelView>() {
                Ok(mut item) => {
                    let _ = item.content();
                    println!("{item}");
                    continue;
                }
                Err(item) => item,
            };
            dbg!(item);
        }
        Ok(())
    }
}
