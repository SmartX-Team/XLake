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

use crate::{object::LazyObject, PipeEdge, PipeNodeFactory, PipeNodeImpl};

pub type DefaultStreamFactory = MemoryStreamFactory;
pub type DefaultStream = MemoryStream;

pub const NAME: &str = "memory";

#[async_trait]
pub trait PipeStream: Send + fmt::Debug {
    fn extend_one(&mut self, item: LazyObject);

    async fn to_default(&mut self) -> Result<DefaultStream>;
}

#[derive(Copy, Clone, Debug, Default)]
pub struct MemoryStreamFactory;

impl fmt::Display for MemoryStreamFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[async_trait]
impl PipeNodeFactory for MemoryStreamFactory {
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
            model: Some(vec![self.name()]),
            stream: self.name(),
            ..Default::default()
        }
    }

    async fn build(&self, _args: &PlanArguments) -> Result<PipeNodeImpl> {
        Ok(PipeNodeImpl::Stream(Box::new(MemoryStream::default())))
    }
}

#[derive(Default)]
pub struct MemoryStream {
    stream: Option<Pin<Box<dyn Send + Stream<Item = Result<LazyObject>>>>>,
    new: VecDeque<LazyObject>,
}

impl fmt::Debug for MemoryStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamFormat")
            .field("stream", &"...")
            .field("new", &self.new)
            .finish()
    }
}

impl Extend<LazyObject> for MemoryStream {
    fn extend<T: IntoIterator<Item = LazyObject>>(&mut self, iter: T) {
        iter.into_iter()
            .for_each(|item| PipeStream::extend_one(self, item))
    }
}

impl FromIterator<LazyObject> for MemoryStream {
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

impl MemoryStream {
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
}

#[async_trait]
impl PipeStream for MemoryStream {
    #[inline]
    fn extend_one(&mut self, item: LazyObject) {
        self.new.push_back(item)
    }

    async fn to_default(&mut self) -> Result<Self> {
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

impl Stream for MemoryStream {
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
