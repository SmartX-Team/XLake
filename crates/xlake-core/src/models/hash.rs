use core::fmt;
use std::path::{Path, PathBuf};

use anyhow::Result;
use digest::Digest;
use serde::{Deserialize, Serialize};
use xlake_ast::{Binary, Object, Value};

use crate::{LazyObject, PipeModelValue, PipeModelView};

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HashModelObject {
    pub hash: String,
}

impl From<HashModelObject> for LazyObject {
    fn from(object: HashModelObject) -> Self {
        #[allow(unused_mut)]
        let mut map = Object::default();
        map.insert(self::__keys::hash.into(), object.hash.into());
        map.into()
    }
}

impl PipeModelValue for HashModelObject {
    type View = HashModelView;
    type ViewRef<'a> = HashModelView<&'a LazyObject>;
    type ViewMut<'a> = HashModelView<&'a mut LazyObject>;
}

#[derive(Copy, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HashModelView<T = LazyObject> {
    pub(crate) item: T,
}

impl HashModelView {
    pub fn new(mut item: LazyObject, hashable: impl Hashable) -> Self {
        item.content.insert(
            self::__keys::hash.into(),
            Value::String(hashable.digest_string()),
        );
        Self { item }
    }

    pub fn try_new(item: impl Serialize, hashable: impl Hashable) -> Result<Self> {
        let item = Object::from_value(item)?.into();
        Ok(Self::new(item, hashable))
    }
}

impl<T> ::core::borrow::Borrow<LazyObject> for HashModelView<T>
where
    T: ::core::borrow::Borrow<LazyObject>,
{
    #[inline]
    fn borrow(&self) -> &LazyObject {
        self.item.borrow()
    }
}

impl<T> ::core::borrow::BorrowMut<LazyObject> for HashModelView<T>
where
    T: ::core::borrow::BorrowMut<LazyObject>,
{
    #[inline]
    fn borrow_mut(&mut self) -> &mut LazyObject {
        self.item.borrow_mut()
    }
}

impl<T> HashModelView<T>
where
    T: ::core::borrow::Borrow<LazyObject>,
{
    pub fn hash_raw(&self) -> &Value {
        self.item.borrow().get_raw(self::__keys::hash).unwrap()
    }
}

impl<T> HashModelView<T>
where
    T: ::core::borrow::BorrowMut<LazyObject>,
{
    pub fn hash(&mut self) -> &mut <String as crate::PipeModelEntity>::Target {
        self.item
            .borrow_mut()
            .get::<String>(self::__keys::hash)
            .unwrap()
    }

    pub fn hash_mut_raw(&mut self) -> &mut Value {
        self.item
            .borrow_mut()
            .get_mut_raw(self::__keys::hash)
            .unwrap()
    }
}

impl<T> fmt::Debug for HashModelView<T>
where
    T: ::core::borrow::Borrow<LazyObject>,
{
    #[allow(unused_mut)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fmt = f.debug_struct(stringify!(HashModelView));
        let mut fmt = fmt.field(self::__keys::hash, self.hash_raw());
        fmt.finish()
    }
}

impl<T> fmt::Display for HashModelView<T>
where
    T: ::core::borrow::Borrow<LazyObject>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.hash_raw().fmt(f)
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

impl<T> PipeModelView<T> for HashModelView<T>
where
    T: Unpin + ::core::borrow::Borrow<LazyObject> + From<HashModelView<T>>,
{
    fn cast(item: T) -> Result<Self, T> {
        if item.borrow().get_raw(self::__keys::hash).is_none() {
            return Err(item);
        }
        Ok(Self { item })
    }
}

#[allow(non_upper_case_globals)]
mod __keys {
    pub(super) const hash: &'static str = stringify!(hash);
}
