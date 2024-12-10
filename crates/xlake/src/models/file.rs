use core::borrow;

use serde::{Deserialize, Serialize};
use xlake_core::LazyObject;
use xlake_derive::PipeModel;

#[derive(Clone, Debug, Serialize, Deserialize, PipeModel)]
pub struct FileModelObject {
    pub extension: String,
}

impl<T> FileModelView<T>
where
    T: borrow::BorrowMut<LazyObject>,
{
    pub fn new(mut item: T, extension: String) -> Self {
        item.borrow_mut()
            .insert(self::__keys::extension.into(), extension);
        Self { item }
    }
}
