use std::{collections::BTreeMap, fmt, ops};

use anyhow::Result;
use num_format::{Locale, ToFormattedString};
use serde::{
    de::{self, DeserializeOwned, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_with::{base64::Base64, serde_as};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[must_use]
pub struct Plan {
    pub kind: PlanKind,
    pub args: PlanArguments,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PlanKind {
    Format { name: String },
    Func { model_name: String, func: String },
    Model { name: String },
    Sink { name: String },
    Src { name: String },
    Store { name: String },
}

impl fmt::Display for PlanKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Func { model_name, func } => write!(f, "{model_name}:{func}"),
            Self::Format { name }
            | Self::Model { name }
            | Self::Sink { name }
            | Self::Src { name }
            | Self::Store { name } => {
                let type_name = self.type_name();
                write!(f, "{name}{type_name}")
            }
        }
    }
}

impl PlanKind {
    pub const fn type_name(&self) -> PlanType {
        match self {
            Self::Format { .. } => PlanType::Format,
            Self::Func { .. } => PlanType::Func,
            Self::Model { .. } => PlanType::Model,
            Self::Sink { .. } => PlanType::Sink,
            Self::Src { .. } => PlanType::Src,
            Self::Store { .. } => PlanType::Store,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PlanType {
    Format,
    Func,
    Model,
    Sink,
    Src,
    Store,
}

impl fmt::Debug for PlanType {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl fmt::Display for PlanType {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl PlanType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Format => "format",
            Self::Func => "function",
            Self::Model => "model",
            Self::Sink => "sink",
            Self::Src => "src",
            Self::Store => "store",
        }
    }
}

pub type PlanArguments = Object;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[must_use]
pub struct PlanArgument {
    pub key: String,
    pub value: Value,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Object(BTreeMap<String, Value>);

impl ops::Deref for Object {
    type Target = BTreeMap<String, Value>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for Object {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl fmt::Debug for Object {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Object {
    pub fn from_json(json: ::serde_json::Value) -> Result<Self> {
        ::serde_json::from_value(json).map_err(Into::into)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        ::serde_json::from_slice(slice).map_err(Into::into)
    }

    pub fn from_value(value: impl Serialize) -> Result<Self> {
        let json = ::serde_json::to_value(value)?;
        Self::from_json(json)
    }

    pub fn to<T>(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let json = self.to_json()?;
        ::serde_json::from_value(json).map_err(Into::into)
    }

    pub fn to_json(&self) -> Result<::serde_json::Value> {
        ::serde_json::to_value(self).map_err(Into::into)
    }

    pub fn to_vec(&self) -> Result<Vec<u8>> {
        ::serde_json::to_vec(self).map_err(Into::into)
    }
}

#[derive(Clone, Serialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Number(Number),
    Binary(Binary),
    String(String),
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => "null".fmt(f),
            Self::Bool(v) => v.fmt(f),
            Self::Number(v) => v.fmt(f),
            Self::Binary(v) => v.fmt(f),
            Self::String(v) => v.fmt(f),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => "null".fmt(f),
            Self::Bool(v) => v.fmt(f),
            Self::Number(v) => v.fmt(f),
            Self::Binary(v) => v.fmt(f),
            Self::String(v) => fmt::Debug::fmt(v, f),
        }
    }
}

macro_rules! impl_atomic_value {
    ( $ty:ty => $variant:ident ) => {
        impl From<$ty> for Value {
            #[inline]
            fn from(value: $ty) -> Self {
                Self::$variant(value)
            }
        }
    };
}

impl_atomic_value!(bool => Bool);
impl_atomic_value!(Number => Number);
impl_atomic_value!(Binary => Binary);
impl_atomic_value!(String => String);

macro_rules! impl_atomic_integer_value {
    ( $( $ty:ty ),* ) => {
        $(
            impl From<$ty> for Value {
                #[inline]
                fn from(value: $ty) -> Self {
                    Self::Number(Number::Fixed(value.into()))
                }
            }
        )*
    };
}

impl_atomic_integer_value!(i8, i16, i32, i64, isize);
impl_atomic_integer_value!(u8, u16, u32, u64, usize);

impl From<&[u8]> for Value {
    #[inline]
    fn from(value: &[u8]) -> Self {
        Self::Binary(Binary(value.into()))
    }
}

impl<const N: usize> From<&[u8; N]> for Value {
    #[inline]
    fn from(value: &[u8; N]) -> Self {
        Self::Binary(Binary(value.into()))
    }
}

impl From<Vec<u8>> for Value {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        Self::Binary(Binary(value))
    }
}

impl From<&str> for Value {
    #[inline]
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

struct ValueVisitor;

macro_rules! impl_atomic_integer_deserialize {
    ( $ty:ty => $method:ident ) => {
        fn $method<E>(self, v: $ty) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Value::Number(Number::Fixed(v.into())))
        }
    };
}

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a map object")
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Null)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Bool(v))
    }

    impl_atomic_integer_deserialize!(i8 => visit_i8);
    impl_atomic_integer_deserialize!(i16 => visit_i16);
    impl_atomic_integer_deserialize!(i32 => visit_i32);
    impl_atomic_integer_deserialize!(i64 => visit_i64);

    impl_atomic_integer_deserialize!(u8 => visit_u8);
    impl_atomic_integer_deserialize!(u16 => visit_u16);
    impl_atomic_integer_deserialize!(u32 => visit_u32);
    impl_atomic_integer_deserialize!(u64 => visit_u64);

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Binary(Binary(v.to_vec())))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Binary(Binary(v)))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(v.into()))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(v))
    }
}

impl<'de> Deserialize<'de> for Value {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueVisitor)
    }
}

#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Binary(#[serde_as(as = "Base64")] pub Vec<u8>);

impl From<Vec<u8>> for Binary {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl ops::Deref for Binary {
    type Target = Vec<u8>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for Binary {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl fmt::Display for Binary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self.0.len().to_formatted_string(&Locale::en);
        write!(f, "Binary({len} bytes)")
    }
}

#[derive(Clone, Serialize)]
#[serde(untagged)]
pub enum Number {
    Fixed(::serde_json::Number),
    Dynamic(String),
}

impl fmt::Debug for Number {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Number {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Number::Fixed(v) => v.fmt(f),
            Number::Dynamic(v) => v.fmt(f),
        }
    }
}

impl<'de> Deserialize<'de> for Number {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ::serde_json::Number::deserialize(deserializer).map(Self::Fixed)
    }
}
