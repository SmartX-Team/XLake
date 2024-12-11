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
pub trait PipeFormat: Send + fmt::Debug {
    fn extend_one(&mut self, item: self::object::LazyObject);

    fn stream(&mut self) -> self::stream::StreamFormat;
}

#[async_trait]
pub trait PipeFunc: fmt::Debug {}

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
            .async_iter::<self::object::LazyObject>()
            .then(|item| async {
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
    Format(Box<dyn PipeFormat>),
    Func(Box<dyn PipeFunc>),
    Model(Box<dyn PipeModelConverter>),
    Sink(Box<dyn PipeSink>),
    Src(Box<dyn PipeSrc>),
    Store(Arc<dyn PipeStore>),
}

impl PipeNodeImpl {
    pub const fn type_name(&self) -> PlanType {
        match self {
            Self::Format(_) => PlanType::Format,
            Self::Func(_) => PlanType::Func,
            Self::Model(_) => PlanType::Model,
            Self::Sink(_) => PlanType::Sink,
            Self::Src(_) => PlanType::Src,
            Self::Store(_) => PlanType::Store,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipeEdge {
    pub format: Option<String>,
    pub model: Option<Vec<String>>,
}

#[async_trait]
pub trait PipeNodeBuilder: fmt::Debug {
    fn kind(&self) -> PlanKind;

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
    format: Box<dyn PipeFormat>,
}

impl Default for PipeChannel {
    fn default() -> Self {
        Self {
            format: Box::new(self::stream::StreamFormat::default()),
        }
    }
}

impl Extend<self::object::LazyObject> for PipeChannel {
    fn extend<T: IntoIterator<Item = self::object::LazyObject>>(&mut self, iter: T) {
        iter.into_iter()
            .for_each(|item| self.format.extend_one(item))
    }
}

impl PipeChannel {
    pub fn async_iter<T>(self) -> PipeChannelAsyncIter<T>
    where
        T: Unpin + From<self::object::LazyObject>,
    {
        let Self { mut format } = self;

        PipeChannelAsyncIter {
            _view: PhantomData,
            stream: format.stream(),
        }
    }

    #[inline]
    pub fn stream_unit<O, T>(object: T) -> Result<Self>
    where
        O: Borrow<self::object::LazyObject> + Into<self::object::LazyObject>,
        T: Serialize + PipeModelOwned<O>,
    {
        let item = object.into_any();
        let format = Box::new(self::stream::StreamFormat::from_unit(item));
        Ok(Self { format })
    }
}

pub struct PipeChannelAsyncIter<T>
where
    T: Unpin + TryFrom<self::object::LazyObject>,
{
    _view: PhantomData<T>,
    stream: self::stream::StreamFormat,
}

impl<T> Stream for PipeChannelAsyncIter<T>
where
    T: Unpin + TryFrom<self::object::LazyObject>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .stream
            .poll_next_unpin(cx)
            .map(|maybe_item| maybe_item.and_then(|item| T::try_from(item).ok()))
    }
}
