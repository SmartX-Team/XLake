pub mod batch;
pub mod models;
pub mod object;
pub mod stream;

use std::{
    borrow::Borrow,
    collections::BTreeSet,
    fmt,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Result;
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::Serialize;
use tracing::debug;
use xlake_ast::{Object, PlanArguments, PlanKind, PlanType};

#[async_trait]
pub trait PipeFunc: fmt::Debug {
    async fn call(&self, channel: PipeChannel) -> Result<PipeChannel>;
}

pub trait PipeModelConverter: fmt::Debug {}

#[async_trait]
pub trait PipeModelObject
where
    Self: Sized + PipeModelView,
{
    type View: PipeModelView;
    type ViewRef<'a>: PipeModelView;
    type ViewMut<'a>: PipeModelView;

    fn __model_name() -> String;

    fn __provides() -> BTreeSet<String> {
        let mut set = BTreeSet::default();
        set.insert(<Self as PipeModelObject>::__model_name());
        set
    }
}

pub trait PipeModelOwned<T>
where
    T: Borrow<self::object::LazyObject> + Into<self::object::LazyObject>,
{
    fn __cast(item: T) -> Result<Self, T>
    where
        Self: Sized;

    fn __into_inner(self) -> T
    where
        Self: Sized;
}

pub trait PipeModelOwnedExt<T>
where
    Self: PipeModelOwned<T>,
    T: Borrow<self::object::LazyObject> + Into<self::object::LazyObject>,
{
    #[inline]
    fn into_any(self) -> self::object::LazyObject
    where
        Self: Sized,
    {
        self.__into_inner().into()
    }

    #[inline]
    fn view<V>(self) -> Result<V, Self>
    where
        Self: Sized + Borrow<self::object::LazyObject> + Into<self::object::LazyObject>,
        V: PipeModelOwned<Self>,
    {
        <V as PipeModelOwned<Self>>::__cast(self)
    }
}

impl<O, T> PipeModelOwnedExt<O> for T
where
    Self: PipeModelOwned<O>,
    O: Borrow<self::object::LazyObject> + Into<self::object::LazyObject>,
{
}

pub trait PipeModelView {
    fn __model_name(&self) -> String;

    fn __provides(&self) -> BTreeSet<String> {
        let mut set = BTreeSet::default();
        set.insert(self.__model_name());
        set
    }
}

#[async_trait]
pub trait PipeSink: fmt::Debug {
    async fn call(&self, channel: PipeChannel) -> Result<()>;
}

#[async_trait]
pub trait PipeSrc: fmt::Debug {
    async fn call(&self) -> Result<PipeChannel>;
}

#[async_trait]
pub trait PipeStore: Send + Sync + fmt::Debug {
    async fn contains(&self, hash: &self::models::hash::Hash) -> Result<bool>;

    async fn read_item(&self, hash: &self::models::hash::Hash) -> Result<Object>;

    async fn write_item(&self, hash: &self::models::hash::Hash, object: &Object) -> Result<()>;
}

#[async_trait]
pub trait PipeStoreExt {
    async fn save(&self, channel: PipeChannel) -> Result<PipeChannel>;
}

#[async_trait]
impl<T> PipeStoreExt for Arc<T>
where
    T: 'static + ?Sized + PipeStore,
{
    async fn save(&self, channel: PipeChannel) -> Result<PipeChannel> {
        channel
            .into_stream::<self::object::LazyObject>()
            .await?
            .and_then(|item| async {
                let mut item = match self::models::hash::HashModelView::__cast(item) {
                    Ok(item) => item,
                    Err(item) => return Ok(item),
                };
                let hash = item.hash();
                if self.contains(&hash).await? {
                    // Hit
                    if item.is_ready() {
                        // Drop the future and get it from the store
                        debug!("Hit cache: {hash}");
                        let future = Box::pin({
                            let store = self.clone();
                            async move { store.read_item(&hash).await }
                        });
                        Ok(item.into_any().replace_with(future))
                    } else {
                        Ok(item.into_any())
                    }
                } else {
                    // Miss
                    debug!("Miss cache: {hash}");
                    let content = item.into_any().flatten().await?;
                    self.write_item(&hash, content.as_content_unpolled())
                        .await?;
                    Ok(content)
                }
            })
            .try_collect()
            .await
    }
}

#[derive(Debug)]
pub enum PipeNodeImpl {
    Batch(Box<dyn self::batch::PipeBatch>),
    Func(Box<dyn PipeFunc>),
    Sink(Box<dyn PipeSink>),
    Src(Box<dyn PipeSrc>),
    Store(Arc<dyn PipeStore>),
    Stream(Box<dyn self::stream::PipeStream>),
}

impl PipeNodeImpl {
    pub const fn type_name(&self) -> PlanType {
        match self {
            Self::Batch(_) => PlanType::Batch,
            Self::Func(_) => PlanType::Func,
            Self::Sink(_) => PlanType::Sink,
            Self::Src(_) => PlanType::Src,
            Self::Store(_) => PlanType::Store,
            Self::Stream(_) => PlanType::Stream,
        }
    }
}

pub struct PipeNode {
    pub kind: PlanKind,
    pub args: PlanArguments,
    pub imp: PipeNodeImpl,
}

impl fmt::Debug for PipeNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipeNode")
            .field("kind", &self.kind)
            .field("args", &self.args)
            .finish()
    }
}

impl fmt::Display for PipeNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { kind, args, imp: _ } = self;
        write!(f, "{kind}")?;

        for (index, (key, value)) in args.iter().enumerate() {
            if index > 0 {
                write!(f, ", ")?;
            } else {
                write!(f, " ")?;
            }
            write!(f, "{key}={value:?}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipeEdge {
    pub batch: String,
    pub model: Option<Vec<String>>,
    pub stream: String,
}

impl Default for PipeEdge {
    fn default() -> Self {
        Self {
            batch: self::batch::NAME.into(),
            model: None,
            stream: self::stream::NAME.into(),
        }
    }
}

#[async_trait]
pub trait PipeNodeFactory: fmt::Debug {
    fn kind(&self) -> PlanKind;

    fn name(&self) -> String;

    fn input(&self) -> PipeEdge {
        PipeEdge::default()
    }

    fn output(&self) -> PipeEdge {
        PipeEdge::default()
    }

    async fn build(&self, args: &PlanArguments) -> Result<PipeNodeImpl>;
}

#[derive(Debug)]
pub struct PipeChannel {
    batch: Box<dyn self::batch::PipeBatch>,
    stream: Box<dyn self::stream::PipeStream>,
}

impl Default for PipeChannel {
    fn default() -> Self {
        Self {
            batch: Box::new(self::batch::DefaultBatch::default()),
            stream: Box::new(self::stream::DefaultStream::default()),
        }
    }
}

impl Extend<self::object::LazyObject> for PipeChannel {
    fn extend<T: IntoIterator<Item = self::object::LazyObject>>(&mut self, iter: T) {
        iter.into_iter()
            .for_each(|item| self.stream.extend_one(item))
    }
}

impl FromIterator<self::object::LazyObject> for PipeChannel {
    #[inline]
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = self::object::LazyObject>,
    {
        let stream = self::stream::DefaultStream::from_iter(iter);
        Self::from_stream(stream)
    }
}

impl PipeChannel {
    #[inline]
    pub fn from_batch(batch: impl 'static + self::batch::PipeBatch) -> Self {
        Self {
            batch: Box::new(batch),
            ..Default::default()
        }
    }

    #[inline]
    pub fn from_stream(stream: impl 'static + self::stream::PipeStream) -> Self {
        Self {
            stream: Box::new(stream),
            ..Default::default()
        }
    }

    #[inline]
    pub fn from_unit<O, T>(object: T) -> Self
    where
        O: Borrow<self::object::LazyObject> + Into<self::object::LazyObject>,
        T: Serialize + PipeModelOwned<O>,
    {
        let item = object.into_any();
        let stream = self::stream::DefaultStream::from_unit(item);
        Self::from_stream(stream)
    }

    #[inline]
    pub async fn into_stream<T>(self) -> Result<PipeChannelStream<T>>
    where
        T: Unpin + PipeModelOwned<self::object::LazyObject>,
    {
        let Self { batch, mut stream } = self;

        Ok(PipeChannelStream {
            _view: PhantomData,
            batch,
            stream: stream.to_default().await?,
        })
    }
}

pub struct PipeChannelStream<T>
where
    T: Unpin + PipeModelOwned<self::object::LazyObject>,
{
    _view: PhantomData<T>,
    batch: Box<dyn self::batch::PipeBatch>,
    stream: self::stream::DefaultStream,
}

impl<T> Stream for PipeChannelStream<T>
where
    T: Unpin + PipeModelOwned<self::object::LazyObject>,
{
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().stream.poll_next_unpin(cx).map(|option| {
            option.and_then(|result| result.map(|item| T::__cast(item).ok()).transpose())
        })
    }
}
