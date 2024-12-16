pub mod split;

use core::{borrow, fmt};

use anyhow::Error;
use serde::{Deserialize, Serialize};
use xlake_core::{models::hash::HashModelView, object::LazyObject};
use xlake_derive::PipeModelObject;

#[derive(Clone, Debug, Serialize, Deserialize, PipeModelObject)]
pub struct DocModelObject {
    pub document: String,
}

impl TryFrom<DocModelObject> for HashModelView {
    type Error = Error;

    #[inline]
    fn try_from(object: DocModelObject) -> Result<Self, Self::Error> {
        Self::try_from(&object)
    }
}

impl TryFrom<&DocModelObject> for HashModelView {
    type Error = Error;

    #[inline]
    fn try_from(object: &DocModelObject) -> Result<Self, Self::Error> {
        HashModelView::try_new(object, &object.document)
    }
}

impl<T> fmt::Display for DocModelView<T>
where
    T: borrow::BorrowMut<LazyObject>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.document_raw().fmt(f)
    }
}

mod consts {
    pub(super) const NAME: &str = "doc";
}
