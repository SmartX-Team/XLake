pub mod models;
pub mod stream;

use std::{
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Result;
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tracing::debug;
use xlake_ast::{Binary, Number, Object, PlanArguments, PlanKind, PlanType, Value};

#[async_trait]
pub trait PipeFormat: Send + fmt::Debug {
    fn extend_one(&mut self, item: LazyObject);

    fn stream(&mut self) -> self::stream::StreamFormat;
}

#[async_trait]
pub trait PipeFunc: fmt::Debug {}

pub trait PipeModel: fmt::Debug {}

pub trait PipeModelEntity {
    type Target: ?Sized;

    fn get<'a>(item: &'a mut LazyObject, key: &str) -> Option<&'a mut Self::Target>;
}

macro_rules! impl_model_entity {
    ( $ty:ty as $variant:ident => $target:ty ) => {
        impl PipeModelEntity for $ty {
            type Target = $target;

            fn get<'a>(item: &'a mut LazyObject, key: &str) -> Option<&'a mut Self::Target> {
                match item.content.get_mut(key)? {
                    Value::$variant(v) => Some(v),
                    _ => None,
                }
            }
        }
    };
}

impl_model_entity!(bool as Bool => bool);
impl_model_entity!(Number as Number => Number);
impl_model_entity!(String as String => String);

impl PipeModelEntity for Binary {
    type Target = Vec<u8>;

    #[inline]
    fn get<'a>(item: &'a mut LazyObject, key: &str) -> Option<&'a mut Self::Target> {
        let value = item.content.get_mut(key)?;
        if let Value::String(v) = value {
            let v = Binary(v.as_bytes().to_vec());
            *value = Value::Binary(v);
        }
        match value {
            Value::Binary(v) => Some(v),
            _ => None,
        }
    }
}

impl PipeModelEntity for Value {
    type Target = Value;

    #[inline]
    fn get<'a>(item: &'a mut LazyObject, key: &str) -> Option<&'a mut Self::Target> {
        item.content.get_mut(key)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct LazyObject {
    #[serde(skip)]
    future: Option<Pin<Box<dyn Send + Future<Output = Result<Object>>>>>,
    content: Object,
}

impl From<Object> for LazyObject {
    #[inline]
    fn from(content: Object) -> Self {
        Self {
            future: None,
            content,
        }
    }
}

impl fmt::Debug for LazyObject {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.content.fmt(f)
    }
}

impl LazyObject {
    #[inline]
    pub fn get<T>(&mut self, key: &str) -> Option<&mut T::Target>
    where
        T: PipeModelEntity,
    {
        <T as PipeModelEntity>::get(self, key)
    }

    #[inline]
    pub fn get_raw(&self, key: &str) -> Option<&Value> {
        self.content.get(key)
    }

    #[inline]
    pub fn get_mut_raw(&mut self, key: &str) -> Option<&mut Value> {
        self.content.get_mut(key)
    }

    #[inline]
    pub fn insert(&mut self, key: String, value: impl Into<Value>) -> Option<Value> {
        self.content.insert(key, value.into())
    }

    pub async fn pull(self) -> Result<Object> {
        let Self {
            mut future,
            mut content,
        } = self;
        if let Some(future) = future.take() {
            // TODO: merge? (idea needed)
            content = future.await?;
        }
        Ok(content)
    }
}

pub trait PipeModelView<T>: Unpin + Into<T> {
    fn cast(item: T) -> Result<Self, T>;
}

impl PipeModelView<Self> for LazyObject {
    #[inline]
    fn cast(item: LazyObject) -> Result<Self, LazyObject> {
        Ok(item)
    }
}

pub trait PipeModelViewExt<V>: PipeModelView<V> {
    #[inline]
    fn view<T>(self) -> Result<T, V>
    where
        T: PipeModelView<V>,
    {
        <T as PipeModelView<V>>::cast(self.into())
    }
}

impl<T, V> PipeModelViewExt<V> for T where T: PipeModelView<V> {}

pub trait PipeModelValue: Serialize {
    type View: PipeModelView<LazyObject>;
    type ViewRef<'a>: PipeModelView<&'a LazyObject>;
    type ViewMut<'a>: PipeModelView<&'a mut LazyObject>;
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
pub trait PipeStore: Sync + fmt::Debug {
    async fn contains(&self, hash: &str) -> Result<bool>;

    async fn read_item(&self, hash: &str) -> Result<Object>;

    async fn write_item(&self, hash: &str, object: &Object) -> Result<()>;
}

#[async_trait]
pub trait PipeStoreExt {
    async fn save(&self, channel: PipeChannel) -> Result<PipeChannel>;
}

#[async_trait]
impl<T> PipeStoreExt for T
where
    T: ?Sized + PipeStore,
{
    async fn save(&self, channel: PipeChannel) -> Result<PipeChannel> {
        channel
            .async_iter::<LazyObject>()
            .then(|item| async {
                let mut item = match self::models::hash::HashModelView::cast(item) {
                    Ok(item) => item,
                    Err(item) => return Ok(item),
                };
                let hash = item.hash().to_string();
                if self.contains(&hash).await? {
                    // Hit
                    if item.item.future.is_some() {
                        // Drop the future and get it from the store
                        debug!("Hit cache: {hash}");
                        // TODO: to be implemented (merge?)
                        self.read_item(&hash).await.map(Into::into)
                    } else {
                        Ok(item.into())
                    }
                } else {
                    // Miss
                    debug!("Miss cache: {hash}");
                    let item: LazyObject = item.into();
                    let content = LazyObject::pull(item).await?;
                    self.write_item(&hash, &content).await?;
                    Ok(content.into())
                }
            })
            .try_collect()
            .await
    }
}

#[async_trait]
impl PipeStoreExt for Box<dyn PipeStore> {
    async fn save(&self, channel: PipeChannel) -> Result<PipeChannel> {
        (&**self).save(channel).await
    }
}

#[derive(Debug)]
pub enum PipeNodeImpl {
    Format(Box<dyn PipeFormat>),
    Func(Box<dyn PipeFunc>),
    Sink(Box<dyn PipeSink>),
    Src(Box<dyn PipeSrc>),
    Store(Box<dyn PipeStore>),
}

impl PipeNodeImpl {
    pub const fn type_name(&self) -> PlanType {
        match self {
            Self::Format(_) => PlanType::Format,
            Self::Func(_) => PlanType::Func,
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

impl Extend<LazyObject> for PipeChannel {
    fn extend<T: IntoIterator<Item = LazyObject>>(&mut self, iter: T) {
        iter.into_iter()
            .for_each(|item| self.format.extend_one(item))
    }
}

impl PipeChannel {
    pub fn async_iter<T>(self) -> PipeChannelAsyncIter<T>
    where
        T: PipeModelView<LazyObject>,
    {
        let Self { mut format } = self;

        PipeChannelAsyncIter {
            _view: PhantomData,
            stream: format.stream(),
        }
    }

    pub fn stream_unit(item: impl Serialize) -> Result<Self> {
        let item: LazyObject = Object::from_value(item)?.into();
        let format = Box::new(self::stream::StreamFormat::from_unit(item));
        Ok(Self { format })
    }
}

pub struct PipeChannelAsyncIter<T>
where
    T: PipeModelView<LazyObject>,
{
    _view: PhantomData<T>,
    stream: self::stream::StreamFormat,
}

impl<T> Stream for PipeChannelAsyncIter<T>
where
    T: PipeModelView<LazyObject>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .stream
            .poll_next_unpin(cx)
            .map(|maybe_item| maybe_item.and_then(|item| T::cast(item).ok()))
    }
}
