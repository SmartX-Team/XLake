pub mod models;
pub mod sinks;
pub mod srcs;
pub mod stores;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, iter,
};

use anyhow::{anyhow, bail, Context, Result};
use tracing::debug;
use xlake_ast::{Plan, PlanKind};
use xlake_core::{PipeEdge, PipeNode, PipeNodeFactory, PipeNodeImpl, PipeStoreExt};
use xlake_parser::SeqParser;

#[derive(Debug)]
pub struct PipeSession {
    factories: BTreeMap<PlanKind, Box<dyn PipeNodeFactory>>,
    parser: SeqParser,
}

impl Default for PipeSession {
    fn default() -> Self {
        let mut session = Self::empty();
        session.add_builtin_factories();
        session
    }
}

impl PipeSession {
    pub fn empty() -> Self {
        Self {
            factories: Default::default(),
            parser: Default::default(),
        }
    }

    fn add_builtin_factories(&mut self) {
        // Defaults
        self.insert_factory(Box::new(::xlake_core::batch::DefaultBatchFactory::default()));
        self.insert_factory(Box::new(
            ::xlake_core::stream::DefaultStreamFactory::default(),
        ));

        #[cfg(feature = "libreoffice")]
        self.insert_factory(Box::new(self::models::builtins::binary::pdf::PdfFactory));
        #[cfg(feature = "io-std")]
        self.insert_factory(Box::new(self::sinks::local::stdout::StdoutSinkFactory));
        self.insert_factory(Box::new(self::srcs::local::csv::CsvSrcFactory));
        #[cfg(feature = "fs")]
        self.insert_factory(Box::new(self::srcs::local::file::FileSrcFactory));
        #[cfg(feature = "io-std")]
        self.insert_factory(Box::new(self::srcs::local::stdin::StdinSrcFactory));
        #[cfg(feature = "fs")]
        self.insert_factory(Box::new(self::stores::local::LocalStoreFactory));
    }

    pub async fn call(&self, input: &str) -> Result<()> {
        let plans = self
            .parser
            .parse(input)
            .map_err(|error| anyhow!("Failed to parse command: {error}"))?;
        self.call_with(plans).await
    }

    pub async fn call_with(&self, plans: Vec<Plan>) -> Result<()> {
        let mut input_batch = ::xlake_core::batch::NAME.to_string();
        let mut input_model = BTreeSet::default();
        let mut input_stream = ::xlake_core::stream::NAME.to_string();
        let mut nodes = Vec::default();
        let mut term_input = None;
        let mut term_output = None;

        debug!("Begin initializing {} plans", plans.len());
        for (index, Plan { kind, args }) in plans.into_iter().enumerate() {
            debug!("Initialize index {index} @ plan {kind}");
            let type_name = kind.type_name();

            let factory = match self.factories.get(&kind) {
                Some(factory) => factory,
                None => bail!("No such {type_name}: '{kind}'"),
            };

            let PipeEdge {
                batch: output_batch,
                model: output_model,
                stream: output_stream,
            } = factory.input();

            debug!("sequence.{index}.{kind}.pre: '{args:?}'");
            {
                debug!("sequence.{index}.{kind}.pre.batch: '{input_batch:?}'");
                let inputs = iter::once(&input_batch);
                let outputs = iter::once(&output_batch);
                let type_name = ValidatableTypeName::Batch;
                self.validate_types(inputs, outputs, type_name)?
            }
            if let Some(output_model) = output_model {
                if input_model.is_empty() {
                    bail!("Implicit model is not allowed");
                }
                debug!("sequence.{index}.{kind}.pre.model: '{input_model:?}'");
                let inputs = input_model.iter();
                let outputs = output_model.iter();
                let type_name = ValidatableTypeName::Model;
                self.validate_types(inputs, outputs, type_name)?
            }
            {
                debug!("sequence.{index}.{kind}.pre.stream: '{input_stream:?}'");
                let inputs = iter::once(&input_stream);
                let outputs = iter::once(&output_stream);
                let type_name = ValidatableTypeName::Stream;
                self.validate_types(inputs, outputs, type_name)?
            }

            let PipeEdge {
                batch: output_batch,
                model: output_model,
                stream: output_stream,
            } = factory.output();

            {
                debug!("sequence.{index}.{kind}.post.batch: {output_batch:?}");
                input_batch = output_batch;
            }
            if let Some(output_model) = output_model {
                debug!("sequence.{index}.{kind}.post.model: {output_model:?}");
                input_model.extend(output_model);
            }
            {
                debug!("sequence.{index}.{kind}.post.stream: {output_stream:?}");
                input_stream = output_stream;
            }

            let imp = factory.build(&args).await?;
            let imp_type_name = imp.type_name();
            if imp_type_name != type_name {
                bail!("Unexpected node: expected {type_name:?}, but given {imp_type_name:?}")
            }

            if matches!(&kind, PlanKind::Src { .. }) {
                if let Some(term) = term_input {
                    bail!("Duplicated src; '{term}' then '{kind}'")
                }
                term_input = Some(kind.clone());
            } else if term_input.is_none() {
                bail!("Cannot link before src: '{kind}'")
            }
            if matches!(&kind, PlanKind::Sink { .. }) {
                if let Some(term) = term_output {
                    bail!("Duplicated sink; '{term}' then '{kind}'")
                }
                term_output = Some(kind.clone());
            } else if term_output.is_some() {
                bail!("Cannot link after sink: '{kind}'")
            }

            let node = PipeNode { kind, args, imp };
            nodes.push(node)
        }

        if term_input.is_none() {
            bail!("No src")
        }
        if term_output.is_none() {
            bail!("No sink")
        }
        debug!("Initialized {} plans", nodes.len());

        // TODO: Detach SequencePlan from `[call_with]`
        drop(input_batch);
        drop(input_model);
        drop(input_stream);
        drop(term_input);
        drop(term_output);

        debug!("Begin executing {} plans", nodes.len());
        let mut channel = None;
        for (index, node) in nodes.into_iter().enumerate() {
            debug!("Execute index {index} @ plan {}", &node.kind);
            let next_channel = match node.imp {
                // TODO: to be implemented
                PipeNodeImpl::Batch(imp) => todo!(),
                // TODO: to be implemented
                PipeNodeImpl::Func(imp) => imp.call(channel.unwrap()).await?,
                // TODO: to be implemented
                PipeNodeImpl::Sink(imp) => {
                    imp.call(channel.unwrap()).await?;
                    break;
                }
                PipeNodeImpl::Src(imp) => imp.call().await?,
                PipeNodeImpl::Store(imp) => match channel.take() {
                    Some(channel) => imp.save(channel).await?,
                    // TODO: to be implemented (load)
                    None => todo!(),
                },
                // TODO: to be implemented
                PipeNodeImpl::Stream(imp) => todo!(),
            };
            channel = Some(next_channel);
        }
        debug!("Finalizing plans");
        Ok(())
    }

    fn collect_factories<'a>(
        &self,
        iter: impl Iterator<Item = &'a String>,
        type_name: ValidatableTypeName,
    ) -> Result<Vec<&dyn PipeNodeFactory>> {
        iter.cloned()
            .map(|name| match type_name {
                ValidatableTypeName::Batch => PlanKind::Batch { name },
                ValidatableTypeName::Model => PlanKind::Model { name },
                ValidatableTypeName::Stream => PlanKind::Stream { name },
            })
            .map(|ref kind| {
                self.factories
                    .get(kind)
                    .map(|factory| &**factory)
                    .with_context(|| format!("No such {type_name}: {kind}"))
            })
            .collect()
    }

    pub fn insert_factory(
        &mut self,
        factory: Box<dyn PipeNodeFactory>,
    ) -> Option<Box<dyn PipeNodeFactory>> {
        self.factories.insert(factory.kind(), factory)
    }

    fn validate_types<'a>(
        &self,
        inputs: impl Iterator<Item = &'a String>,
        outputs: impl Iterator<Item = &'a String>,
        type_name: ValidatableTypeName,
    ) -> Result<()> {
        // let input_factories: Vec<_> = self.collect_factories(inputs, type_name)?;
        // let outputs_factories: Vec<_> = self.collect_factories(outputs, type_name)?;
        // TODO: to be implemented (validate)
        Ok(())
    }
}

#[derive(Copy, Clone)]
enum ValidatableTypeName {
    Batch,
    Model,
    Stream,
}

impl fmt::Display for ValidatableTypeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Batch => "batch".fmt(f),
            Self::Model => "model".fmt(f),
            Self::Stream => "stream".fmt(f),
        }
    }
}
