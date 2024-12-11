use core::{borrow, fmt};

use anyhow::Error;
use serde::{Deserialize, Serialize};
use xlake_ast::Binary;
use xlake_core::{models::hash::HashModelView, object::LazyObject};
use xlake_derive::PipeModelObject;

#[derive(Clone, Debug, Serialize, Deserialize, PipeModelObject)]
pub struct BinaryModelObject {
    pub content: Binary,
}

impl TryFrom<BinaryModelObject> for HashModelView {
    type Error = Error;

    #[inline]
    fn try_from(object: BinaryModelObject) -> Result<Self, Self::Error> {
        Self::try_from(&object)
    }
}

impl TryFrom<&BinaryModelObject> for HashModelView {
    type Error = Error;

    #[inline]
    fn try_from(object: &BinaryModelObject) -> Result<Self, Self::Error> {
        HashModelView::try_new(object, &object.content)
    }
}

impl<T> fmt::Display for BinaryModelView<T>
where
    T: borrow::BorrowMut<LazyObject>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.content_raw().fmt(f)
    }
}
