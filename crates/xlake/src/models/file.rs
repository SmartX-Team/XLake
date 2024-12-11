use core::borrow;

use serde::{Deserialize, Serialize};
use xlake_core::object::LazyObject;
use xlake_derive::PipeModelObject;

#[derive(Clone, Debug, Serialize, Deserialize, PipeModelObject)]
pub struct FileModelObject {
    pub extension: String,
}

impl<T> FileModelView<T>
where
    T: borrow::BorrowMut<LazyObject>,
{
    pub fn new(mut item: T, extension: String) -> Self {
        item.borrow_mut()
            .insert(self::__keys::extension.into(), extension.into());
        Self { item }
    }
}
