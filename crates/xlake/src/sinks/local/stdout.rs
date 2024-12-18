use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use xlake_ast::{PlanArguments, PlanKind};
use xlake_core::{
    object::LazyObject, PipeChannel, PipeModelOwnedExt, PipeNodeBuilder, PipeNodeImpl, PipeSink,
};

use crate::models::builtins::{binary::BinaryModelView, doc::DocModelView};

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
        PlanKind::Sink { name: self.name() }
    }

    fn name(&self) -> String {
        "stdout".into()
    }

    async fn build(&self, args: &PlanArguments) -> Result<PipeNodeImpl> {
        let imp: StdoutSink = args.to()?;
        Ok(PipeNodeImpl::Sink(Box::new(imp)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StdoutSink {}

#[async_trait]
impl PipeSink for StdoutSink {
    async fn call(&self, channel: PipeChannel) -> Result<()> {
        let mut iter = channel.into_stream::<LazyObject>().await?;
        while let Some(item) = iter.try_next().await? {
            let item = item.flatten().await?;
            let item = match item.view::<DocModelView>() {
                Ok(mut item) => {
                    println!("{}", item.document());
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
            println!("{}", item.to_string_pretty()?);
        }
        Ok(())
    }
}
