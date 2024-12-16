use std::{collections::BTreeSet, fmt, future::Future, ops, pin::Pin};

use anyhow::Result;
use futures::{stream::FuturesOrdered, FutureExt, TryFutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use xlake_ast::{Binary, Number, Object, Value};

#[derive(Debug, Serialize, Deserialize)]
pub struct LazyObject {
    #[serde(skip)]
    layers: Vec<ObjectLayer>,
}

impl From<ObjectLayer> for LazyObject {
    #[inline]
    fn from(layer: ObjectLayer) -> Self {
        Self {
            layers: vec![layer],
        }
    }
}

impl ops::Deref for LazyObject {
    type Target = ObjectLayer;

    fn deref(&self) -> &Self::Target {
        self.layers.last().unwrap()
    }
}

impl ops::DerefMut for LazyObject {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.layers.last_mut().unwrap()
    }
}

impl crate::PipeModelOwned<Self> for LazyObject {
    #[inline]
    fn __cast(item: Self) -> Result<Self, Self> {
        Ok(item)
    }

    #[inline]
    fn __into_inner(self) -> Self {
        self
    }
}

impl LazyObject {
    #[inline]
    pub fn append_future<T>(&mut self, future: MaybeObject<T>)
    where
        T: 'static + Into<Object> + crate::PipeModelObject,
    {
        let future = future.map_ok(|value| value.into()).boxed();
        match self.future.as_ref() {
            Some(_) => {
                let layer = ObjectLayer {
                    content: Default::default(),
                    future: Some(future),
                    models: <T as crate::PipeModelObject>::__provides(),
                };
                self.layers.push(layer)
            }
            None => {
                self.future.replace(future);
            }
        }
    }

    #[inline]
    pub fn append_layer(&mut self, layer: ObjectLayer) {
        self.layers.push(layer)
    }

    pub async fn flatten(mut self) -> Result<Self> {
        let () = self
            .layers
            .iter_mut()
            .map(|layer| layer.take_future())
            .collect::<FuturesOrdered<_>>()
            .try_collect()
            .await?;

        let layer = self.flatten_without_futures();
        Ok(layer.into())
    }

    fn flatten_without_futures(self) -> ObjectLayer {
        let Self { layers } = self;
        let mut object = ObjectLayer {
            content: Default::default(),
            future: None,
            models: Default::default(),
        };
        for mut layer in layers {
            object.merge_without_future(&mut layer)
        }
        object
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.layers.iter().all(|layer| layer.is_ready())
    }

    pub(crate) fn replace_with(self, future: MaybeObject) -> Self {
        let mut layer = self.flatten_without_futures();
        layer.future.replace(future);
        layer.into()
    }
}

type MaybeObject<T = Object> = Pin<Box<dyn Send + Future<Output = Result<T>>>>;

#[derive(Serialize, Deserialize)]
pub struct ObjectLayer {
    #[serde(flatten)]
    content: Object,
    #[serde(skip)]
    future: Option<MaybeObject>,
    #[serde(rename = "__models")]
    models: BTreeSet<String>,
}

impl fmt::Debug for ObjectLayer {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.content.fmt(f)
    }
}

impl ObjectLayer {
    #[inline]
    pub fn empty(models: BTreeSet<String>) -> Self {
        let content = Default::default();
        Self::from_object(content, models)
    }

    #[inline]
    pub fn from_object(content: Object, models: BTreeSet<String>) -> Self {
        Self {
            content,
            future: None,
            models,
        }
    }

    #[inline]
    pub fn from_object_dyn(content: Object) -> Self {
        Self {
            content,
            future: None,
            models: Default::default(),
        }
    }

    pub fn from_owned<T>(object: &T) -> Result<Self>
    where
        T: Serialize + crate::PipeModelView,
    {
        let models = object.__provides();
        Ok(Self {
            content: Object::from_value(object)?,
            future: None,
            models,
        })
    }

    #[inline]
    pub(crate) const fn as_content_unpolled(&self) -> &Object {
        &self.content
    }

    #[inline]
    pub fn get<T>(&mut self, key: &str) -> Option<&mut T::Target>
    where
        T: ValueExt,
    {
        <T as ValueExt>::get(self, key)
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
    pub fn insert(&mut self, key: String, value: Value) -> Option<Value> {
        self.content.insert(key, value)
    }

    #[inline]
    pub(crate) const fn is_ready(&self) -> bool {
        self.future.is_none()
    }

    fn merge_without_future(&mut self, other: &mut Self) {
        self.content.append(&mut other.content);
        self.models.append(&mut other.models);
    }

    async fn take_future(&mut self) -> Result<()> {
        if let Some(future) = self.future.take() {
            self.content.append(&mut *future.await?)
        }
        Ok(())
    }

    pub fn to_string_pretty(&self) -> Result<String> {
        self.content.to_string_pretty()
    }
}

pub trait ValueExt {
    type Target: ?Sized;

    fn get<'a>(layer: &'a mut ObjectLayer, key: &str) -> Option<&'a mut Self::Target>;
}

macro_rules! impl_model_entity {
    ( $ty:ty as $variant:ident => $target:ty ) => {
        impl ValueExt for $ty {
            type Target = $target;

            fn get<'a>(layer: &'a mut ObjectLayer, key: &str) -> Option<&'a mut Self::Target> {
                match layer.content.get_mut(key)? {
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

impl ValueExt for Binary {
    type Target = Vec<u8>;

    #[inline]
    fn get<'a>(layer: &'a mut ObjectLayer, key: &str) -> Option<&'a mut Self::Target> {
        let value = layer.content.get_mut(key)?;
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

impl ValueExt for Value {
    type Target = Value;

    #[inline]
    fn get<'a>(layer: &'a mut ObjectLayer, key: &str) -> Option<&'a mut Self::Target> {
        layer.content.get_mut(key)
    }
}
