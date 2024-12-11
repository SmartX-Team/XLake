use core::fmt;
use std::{
    borrow::{Borrow, BorrowMut},
    collections::BTreeSet,
    ops,
    path::{Path, PathBuf},
};

use anyhow::Result;
use digest::Digest;
use serde::{Deserialize, Serialize};
use xlake_ast::{Binary, Object, Value};

use crate::{
    object::{LazyObject, ObjectLayer},
    PipeModelObject, PipeModelOwned, PipeModelView,
};

pub trait Hashable {
    fn as_bytes(&self) -> &[u8];
}

impl<T> Hashable for &T
where
    T: ?Sized + Hashable,
{
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        (*self).as_bytes()
    }
}

impl Hashable for [u8] {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl Hashable for Vec<u8> {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        <[u8] as Hashable>::as_bytes(self)
    }
}

impl Hashable for Binary {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        <[u8] as Hashable>::as_bytes(self)
    }
}

impl Hashable for str {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Hashable for String {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        <str as Hashable>::as_bytes(self)
    }
}

impl Hashable for Path {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.to_str().unwrap_or("").as_bytes()
    }
}

impl Hashable for PathBuf {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        <Path as Hashable>::as_bytes(self)
    }
}

trait HashableExt: Hashable {
    fn digest_string(&self) -> String {
        let input = ::blake2::Blake2s256::digest(self.as_bytes());
        ::bs58::encode(input)
            .with_alphabet(::bs58::Alphabet::BITCOIN)
            .into_string()
    }
}

impl<T> HashableExt for T where T: Hashable {}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash<T = String>(pub(crate) T);

impl<T> ops::Deref for Hash<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> fmt::Debug for Hash<T>
where
    T: fmt::Debug,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> fmt::Display for Hash<T>
where
    T: fmt::Display,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HashModelObject {
    pub hash: String,
}

impl From<HashModelObject> for Object {
    fn from(object: HashModelObject) -> Self {
        let mut item = Object::default();
        item.insert(self::__keys::hash.into(), object.hash.into());
        item
    }
}

impl From<HashModelObject> for ObjectLayer {
    #[inline]
    fn from(object: HashModelObject) -> Self {
        let models = self::__keys::__provides();
        ObjectLayer::from_object(object.into(), models)
    }
}

impl From<HashModelObject> for LazyObject {
    #[inline]
    fn from(object: HashModelObject) -> Self {
        Self::from(ObjectLayer::from(object))
    }
}

impl PipeModelObject for HashModelObject {
    type View = HashModelView;
    type ViewRef<'a> = HashModelView<&'a LazyObject>;
    type ViewMut<'a> = HashModelView<&'a mut LazyObject>;

    #[inline]
    fn __model_name() -> String {
        self::__keys::__model_name.into()
    }

    #[inline]
    fn __provides() -> BTreeSet<String> {
        self::__keys::__provides()
    }
}

impl PipeModelView for HashModelObject {
    #[inline]
    fn __model_name(&self) -> String {
        self::__keys::__model_name.into()
    }

    #[inline]
    fn __provides(&self) -> BTreeSet<String> {
        self::__keys::__provides()
    }
}

#[derive(Copy, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HashModelView<T = LazyObject> {
    item: T,
}

impl HashModelView {
    fn from_owned(mut layer: ObjectLayer, hash: String) -> Self {
        layer.insert(self::__keys::hash.into(), Value::String(hash));
        Self { item: layer.into() }
    }

    #[inline]
    pub fn new(hashable: impl Hashable) -> Self {
        let layer = ObjectLayer::empty(self::__keys::__provides());
        Self::from_owned(layer, hashable.digest_string())
    }

    #[inline]
    pub fn try_new(
        object: &(impl Serialize + PipeModelView),
        hashable: impl Hashable,
    ) -> Result<Self> {
        let layer = ObjectLayer::from_owned(object)?;
        Ok(Self::from_owned(layer, hashable.digest_string()))
    }
}

impl<T> Borrow<LazyObject> for HashModelView<T>
where
    T: Borrow<LazyObject>,
{
    #[inline]
    fn borrow(&self) -> &LazyObject {
        self.item.borrow()
    }
}

impl<T> BorrowMut<LazyObject> for HashModelView<T>
where
    T: BorrowMut<LazyObject>,
{
    #[inline]
    fn borrow_mut(&mut self) -> &mut LazyObject {
        self.item.borrow_mut()
    }
}

impl From<HashModelView<LazyObject>> for LazyObject {
    #[inline]
    fn from(value: HashModelView<LazyObject>) -> Self {
        value.item
    }
}

impl<'a> From<HashModelView<&'a LazyObject>> for &'a LazyObject {
    #[inline]
    fn from(value: HashModelView<&'a LazyObject>) -> Self {
        value.item
    }
}

impl<'a> From<HashModelView<&'a mut LazyObject>> for &'a mut LazyObject {
    #[inline]
    fn from(value: HashModelView<&'a mut LazyObject>) -> Self {
        value.item
    }
}

impl<T> HashModelView<T>
where
    T: Borrow<LazyObject>,
{
    pub fn hash_raw(&self) -> Hash<&Value> {
        Hash(self.item.borrow().get_raw(self::__keys::hash).unwrap())
    }

    #[inline]
    pub(crate) fn is_ready(&self) -> bool {
        self.item.borrow().is_ready()
    }
}

impl<T> HashModelView<T>
where
    T: BorrowMut<LazyObject>,
{
    pub fn hash(&mut self) -> Hash {
        Hash(
            self.item
                .borrow_mut()
                .get::<String>(self::__keys::hash)
                .unwrap()
                .clone(),
        )
    }

    pub fn hash_mut_raw(&mut self) -> Hash<&mut Value> {
        Hash(
            self.item
                .borrow_mut()
                .get_mut_raw(self::__keys::hash)
                .unwrap(),
        )
    }
}

impl<T> PipeModelOwned<T> for HashModelView<T>
where
    T: Borrow<crate::object::LazyObject> + Into<crate::object::LazyObject>,
{
    #[inline]
    fn __cast(item: T) -> Result<Self, T> {
        if self::__keys::__validate(item.borrow()) {
            Ok(Self { item })
        } else {
            Err(item)
        }
    }

    #[inline]
    fn __into_inner(self) -> T {
        self.item
    }
}

impl<T> PipeModelView for HashModelView<T> {
    #[inline]
    fn __model_name(&self) -> String {
        self::__keys::__model_name.into()
    }

    #[inline]
    fn __provides(&self) -> BTreeSet<String> {
        self::__keys::__provides()
    }
}

impl<T> fmt::Debug for HashModelView<T>
where
    T: Borrow<LazyObject>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fmt = f.debug_struct(stringify!(HashModelView));
        let fmt = fmt.field(self::__keys::hash, self.hash_raw().0);
        fmt.finish()
    }
}

impl<T> fmt::Display for HashModelView<T>
where
    T: Borrow<LazyObject>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.hash_raw().fmt(f)
    }
}

#[allow(non_upper_case_globals)]
mod __keys {
    pub(super) const hash: &str = stringify!(hash);

    pub(super) const __model_name: &str = stringify!(hash);

    pub(super) fn __provides() -> ::std::collections::BTreeSet<String> {
        let mut set = ::std::collections::BTreeSet::default();
        set.insert(self::__model_name.into());
        set
    }

    pub(super) fn __validate(item: &crate::object::LazyObject) -> bool {
        if item.get_raw(self::hash).is_none() {
            return false;
        }
        true
    }
}
