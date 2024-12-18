use std::{fmt, ops, pin::Pin};

use anyhow::{bail, Result};
use arrow_json::JsonSerializable;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{cast, ArrayRef, ArrowPrimitiveType, AsArray, RecordBatch},
        datatypes::{self, DataType},
    },
    prelude::SessionContext,
};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use xlake_ast::{Object, PlanArguments, PlanKind, Value};

use crate::{object::ObjectLayer, stream::DefaultStream, PipeEdge, PipeNodeBuilder, PipeNodeImpl};

pub type DefaultBatchBuilder = DataFusionBatchBuilder;
pub type DefaultBatch = DataFusionBatch;

pub const DEFAULT_TABLE_REF: &str = "default";
pub const NAME: &str = "datafusion";

#[async_trait]
pub trait PipeBatch: Send + fmt::Debug {
    async fn to_default(&mut self) -> Result<DefaultBatch>;

    async fn to_stream(&mut self) -> Result<DefaultStream>;
}

#[derive(Copy, Clone, Debug, Default)]
pub struct DataFusionBatchBuilder;

impl fmt::Display for DataFusionBatchBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeBuilder for DataFusionBatchBuilder {
    fn kind(&self) -> PlanKind {
        PlanKind::Batch { name: self.name() }
    }

    fn name(&self) -> String {
        NAME.into()
    }

    fn input(&self) -> PipeEdge {
        PipeEdge {
            model: Some(vec![self.name()]),
            ..Default::default()
        }
    }

    fn output(&self) -> PipeEdge {
        PipeEdge {
            batch: self.name(),
            model: Some(vec![self.name()]),
            ..Default::default()
        }
    }

    async fn build(&self, args: &PlanArguments) -> Result<PipeNodeImpl> {
        let args: BatchFormatArgs = args.to()?;
        let imp = DataFusionBatch::new(args);
        Ok(PipeNodeImpl::Batch(Box::new(imp)))
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BatchFormatArgs {}

#[derive(Default)]
pub struct DataFusionBatch {
    args: BatchFormatArgs,
    ctx: SessionContext,
}

impl DataFusionBatch {
    fn new(args: BatchFormatArgs) -> Self {
        Self {
            args,
            ctx: Default::default(),
        }
    }
}

impl fmt::Debug for DataFusionBatch {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.args.fmt(f)
    }
}

impl ops::Deref for DataFusionBatch {
    type Target = SessionContext;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl ops::DerefMut for DataFusionBatch {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ctx
    }
}

#[async_trait]
impl PipeBatch for DataFusionBatch {
    async fn to_default(&mut self) -> Result<Self> {
        let Self { args, ctx } = self;
        Ok(Self {
            args: args.clone(),
            ctx: ctx.clone(),
        })
    }

    async fn to_stream(&mut self) -> Result<DefaultStream> {
        let df = self.ctx.table(DEFAULT_TABLE_REF).await?;
        let stream = df.execute_stream().await?;
        let stream = stream
            .map_err(Into::into)
            .map(record_batches_to_async_rows)
            .flatten()
            .map_ok(ObjectLayer::from_object_dyn)
            .map_ok(Into::into)
            .boxed();
        Ok(DefaultStream::from_stream(stream))
    }
}

fn record_batches_to_async_rows(
    batch: Result<RecordBatch>,
) -> Pin<Box<dyn Send + Stream<Item = Result<Object>>>> {
    match batch.and_then(|ref batch| record_batches_to_rows(batch)) {
        Ok(rows) => stream::iter(rows.into_iter().map(Ok)).boxed(),
        Err(error) => stream::iter(vec![Err(error)]).boxed(),
    }
}

fn record_batches_to_rows(batch: &RecordBatch) -> Result<Vec<Object>> {
    let mut rows = vec![Object::default(); batch.num_rows()];

    let schema = batch.schema();
    for (j, col) in batch.columns().iter().enumerate() {
        let col_name = schema.field(j).name();
        let explicit_nulls = false;
        set_column_for_object_rows(&mut rows, col, col_name, explicit_nulls)?
    }
    Ok(rows)
}

fn set_column_for_object_rows(
    rows: &mut [Object],
    array: &ArrayRef,
    col_name: &str,
    explicit_nulls: bool,
) -> Result<()> {
    macro_rules! set_column_by_array_type {
        ($cast_fn:expr, $col_name:tt, $rows:tt, $array:tt, $explicit_nulls:tt$(,)?) => {{
            let array = $cast_fn($array);
            $rows
                .iter_mut()
                .zip(array.iter())
                .for_each(|(row, maybe_value)| match maybe_value.map(Into::into) {
                    Some(value) => {
                        row.insert(col_name.into(), value);
                    }
                    None => {
                        if explicit_nulls {
                            row.insert(col_name.into(), Value::Null);
                        }
                    }
                })
        }};
    }

    match array.data_type() {
        DataType::Null => {
            if explicit_nulls {
                rows.iter_mut().for_each(|row| {
                    row.insert(col_name.into(), Value::Null);
                })
            }
        }
        DataType::Int8 => set_column_by_primitive_type::<datatypes::Int8Type>(
            rows,
            array,
            col_name,
            explicit_nulls,
        ),
        DataType::Int16 => set_column_by_primitive_type::<datatypes::Int16Type>(
            rows,
            array,
            col_name,
            explicit_nulls,
        ),
        DataType::Int32 => set_column_by_primitive_type::<datatypes::Int32Type>(
            rows,
            array,
            col_name,
            explicit_nulls,
        ),
        DataType::Int64 => set_column_by_primitive_type::<datatypes::Int64Type>(
            rows,
            array,
            col_name,
            explicit_nulls,
        ),
        DataType::UInt8 => set_column_by_primitive_type::<datatypes::UInt8Type>(
            rows,
            array,
            col_name,
            explicit_nulls,
        ),
        DataType::UInt16 => set_column_by_primitive_type::<datatypes::UInt16Type>(
            rows,
            array,
            col_name,
            explicit_nulls,
        ),
        DataType::UInt32 => set_column_by_primitive_type::<datatypes::UInt32Type>(
            rows,
            array,
            col_name,
            explicit_nulls,
        ),
        DataType::UInt64 => set_column_by_primitive_type::<datatypes::UInt64Type>(
            rows,
            array,
            col_name,
            explicit_nulls,
        ),
        DataType::Float16 => set_column_by_primitive_type::<datatypes::Float16Type>(
            rows,
            array,
            col_name,
            explicit_nulls,
        ),
        DataType::Float32 => set_column_by_primitive_type::<datatypes::Float32Type>(
            rows,
            array,
            col_name,
            explicit_nulls,
        ),
        DataType::Float64 => set_column_by_primitive_type::<datatypes::Float64Type>(
            rows,
            array,
            col_name,
            explicit_nulls,
        ),
        DataType::Boolean => {
            set_column_by_array_type!(
                cast::as_boolean_array,
                col_name,
                rows,
                array,
                explicit_nulls,
            );
        }
        DataType::Utf8 => {
            set_column_by_array_type!(cast::as_string_array, col_name, rows, array, explicit_nulls)
        }
        DataType::LargeUtf8 => {
            set_column_by_array_type!(
                cast::as_largestring_array,
                col_name,
                rows,
                array,
                explicit_nulls,
            )
        }
        _ => {
            bail!("Data type {:?} not supported", array.data_type())
        }
    }
    Ok(())
}

fn set_column_by_primitive_type<T>(
    rows: &mut [Object],
    array: &ArrayRef,
    col_name: &str,
    explicit_nulls: bool,
) where
    T: ArrowPrimitiveType,
    T::Native: JsonSerializable,
{
    let array = array.as_primitive::<T>();
    rows.iter_mut()
        .zip(array.iter().map(|value| {
            value
                .and_then(|value| value.into_json_value())
                .and_then(|value| value.try_into().ok())
        }))
        .for_each(|(row, maybe_value)| match maybe_value {
            Some(value) => {
                row.insert(col_name.into(), value);
            }
            None => {
                if explicit_nulls {
                    row.insert(col_name.into(), Value::Null);
                }
            }
        });
}
