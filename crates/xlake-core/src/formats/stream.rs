use std::{
    collections::VecDeque,
    fmt, mem,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::{bail, Result};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use xlake_ast::{PlanArguments, PlanKind};

use crate::{object::LazyObject, PipeEdge, PipeFormat, PipeNodeBuilder, PipeNodeImpl};

use super::batch::BatchFormat;

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
            name: "stream".into(),
        }
    }

    fn input(&self) -> PipeEdge {
        PipeEdge {
            format: Some("stream".into()),
            model: Some(vec!["stream".into()]),
        }
    }

    fn output(&self) -> PipeEdge {
        PipeEdge {
            format: Some("stream".into()),
            model: Some(vec!["stream".into()]),
        }
    }

    async fn build(&self, _args: &PlanArguments) -> Result<PipeNodeImpl> {
        Ok(PipeNodeImpl::Format(Box::new(StreamFormat::default())))
    }
}

#[derive(Default)]
pub struct StreamFormat {
    stream: Option<Pin<Box<dyn Send + Stream<Item = Result<LazyObject>>>>>,
    new: VecDeque<LazyObject>,
}

impl fmt::Debug for StreamFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamFormat")
            .field("stream", &"...")
            .field("new", &self.new)
            .finish()
    }
}

impl Extend<LazyObject> for StreamFormat {
    fn extend<T: IntoIterator<Item = LazyObject>>(&mut self, iter: T) {
        iter.into_iter()
            .for_each(|item| PipeFormat::extend_one(self, item))
    }
}

impl FromIterator<LazyObject> for StreamFormat {
    #[inline]
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = LazyObject>,
    {
        Self {
            stream: None,
            new: iter.into_iter().collect(),
        }
    }
}

impl StreamFormat {
    pub fn from_stream(stream: Pin<Box<dyn Send + Stream<Item = Result<LazyObject>>>>) -> Self {
        Self {
            stream: Some(stream),
            new: Default::default(),
        }
    }

    pub fn from_unit(item: LazyObject) -> Self {
        let mut format = Self::default();
        format.new.push_back(item);
        format
    }

    #[inline]
    pub fn new(
        stream: Pin<Box<dyn Send + Stream<Item = Result<LazyObject>>>>,
        new: &mut VecDeque<LazyObject>,
    ) -> Self {
        Self {
            stream: Some(stream),
            new: {
                let mut buf = VecDeque::default();
                mem::swap(&mut buf, new);
                buf
            },
        }
    }
}

#[async_trait]
impl PipeFormat for StreamFormat {
    #[inline]
    fn extend_one(&mut self, item: LazyObject) {
        self.new.push_back(item)
    }

    async fn batch(&mut self) -> Result<BatchFormat> {
        bail!("streamformat does not support batch mode")
    }

    async fn stream(&mut self) -> Result<Self> {
        let Self { stream, new } = self;
        Ok(Self {
            stream: stream.take(),
            new: {
                let mut buf = VecDeque::default();
                mem::swap(&mut buf, new);
                buf
            },
        })
    }
}

impl Stream for StreamFormat {
    type Item = Result<LazyObject>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let &mut Self {
            ref mut stream,
            ref mut new,
        } = self.get_mut();

        match stream.as_mut().map(|stream| stream.poll_next_unpin(cx)) {
            Some(Poll::Ready(None)) | None => Poll::Ready(new.pop_front().map(Ok)),
            Some(polled) => polled,
        }
    }
}
