use std::{collections::VecDeque, fmt, mem, ops, pin::Pin};

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

use crate::{
    object::{LazyObject, ObjectLayer},
    PipeEdge, PipeFormat, PipeNodeBuilder, PipeNodeImpl,
};

use super::stream::StreamFormat;

#[derive(Copy, Clone, Debug, Default)]
pub struct BatchBuilder;

impl fmt::Display for BatchBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeBuilder for BatchBuilder {
    fn kind(&self) -> PlanKind {
        PlanKind::Format {
            name: "datafusion".into(),
        }
    }

    fn input(&self) -> PipeEdge {
        PipeEdge {
            format: Some("stream".into()),
            model: Some(vec!["batch".into(), "stream".into()]),
        }
    }

    fn output(&self) -> PipeEdge {
        PipeEdge {
            format: Some("batch".into()),
            model: Some(vec!["batch".into(), "stream".into()]),
            ..Default::default()
        }
    }

    async fn build(&self, args: &PlanArguments) -> Result<PipeNodeImpl> {
        let args: BatchFormatArgs = args.to()?;
        let imp = BatchFormat::new(args);
        Ok(PipeNodeImpl::Format(Box::new(imp)))
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BatchFormatArgs {}

#[derive(Default)]
pub struct BatchFormat {
    args: BatchFormatArgs,
    ctx: SessionContext,
    new: VecDeque<LazyObject>,
}

impl BatchFormat {
    pub const DEFAULT_TABLE_REF: &str = "default";

    fn new(args: BatchFormatArgs) -> Self {
        Self {
            args,
            ctx: Default::default(),
            new: Default::default(),
        }
    }
}

impl fmt::Debug for BatchFormat {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.args.fmt(f)
    }
}

impl ops::Deref for BatchFormat {
    type Target = SessionContext;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl ops::DerefMut for BatchFormat {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ctx
    }
}

#[async_trait]
impl PipeFormat for BatchFormat {
    #[inline]
    fn extend_one(&mut self, item: LazyObject) {
        self.new.push_back(item)
    }

    async fn batch(&mut self) -> Result<Self> {
        let Self { args, ctx, new } = self;
        Ok(Self {
            args: args.clone(),
            ctx: ctx.clone(),
            new: {
                let mut buf = Default::default();
                mem::swap(&mut buf, new);
                buf
            },
        })
    }

    async fn stream(&mut self) -> Result<StreamFormat> {
        let df = self.ctx.table(Self::DEFAULT_TABLE_REF).await?;
        let stream = df.execute_stream().await?;
        let stream = stream
            .map_err(Into::into)
            .map(record_batches_to_async_rows)
            .flatten()
            .map_ok(ObjectLayer::from_object_dyn)
            .map_ok(Into::into)
            .boxed();
        Ok(StreamFormat::new(stream, &mut self.new))
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
