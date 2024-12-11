use std::{
    collections::VecDeque,
    fmt, mem,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Result;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use xlake_ast::{PlanArguments, PlanKind};

use crate::{object::LazyObject, PipeFormat, PipeNodeBuilder, PipeNodeImpl};

#[derive(Copy, Clone, Debug, Default)]
pub struct StreamFormatBuilder;

impl fmt::Display for StreamFormatBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeBuilder for StreamFormatBuilder {
    fn kind(&self) -> PlanKind {
        PlanKind::Format {
            name: "stdout".into(),
        }
    }

    async fn build(&self, _args: &PlanArguments) -> Result<PipeNodeImpl> {
        Ok(PipeNodeImpl::Format(Box::new(StreamFormat::default())))
    }
}

#[derive(Default)]
pub struct StreamFormat {
    inner: Option<Pin<Box<dyn Send + Stream<Item = LazyObject>>>>,
    new: VecDeque<LazyObject>,
}

impl fmt::Debug for StreamFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamFormat")
            .field("inner", &"...")
            .field("new", &self.new)
            .finish()
    }
}

impl StreamFormat {
    pub fn from_stream(stream: Pin<Box<dyn Send + Stream<Item = LazyObject>>>) -> Self {
        Self {
            inner: Some(stream),
            new: Default::default(),
        }
    }

    pub fn from_unit(item: LazyObject) -> Self {
        let mut format = Self::default();
        format.new.push_back(item);
        format
    }
}

#[async_trait]
impl PipeFormat for StreamFormat {
    fn extend_one(&mut self, item: LazyObject) {
        self.new.push_back(item)
    }

    fn stream(&mut self) -> Self {
        let Self { inner, new } = self;
        Self {
            inner: inner.take(),
            new: {
                let mut buf = VecDeque::default();
                mem::swap(&mut buf, new);
                buf
            },
        }
    }
}

impl Stream for StreamFormat {
    type Item = LazyObject;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Self {
            ref mut inner,
            ref mut new,
        } = self.get_mut();

        match inner.as_mut().map(|inner| inner.poll_next_unpin(cx)) {
            Some(Poll::Ready(None)) | None => Poll::Ready(new.pop_front()),
            Some(polled) => polled,
        }
    }
}
