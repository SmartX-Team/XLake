use cruet::Inflector;
use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, parse_quote, Data, DataStruct, DeriveInput, Fields, FieldsNamed,
    GenericParam, Generics, Ident, Type, Visibility,
};

#[proc_macro_derive(PipeModelObject)]
pub fn derive_pipe_model(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let DeriveInput {
        attrs: _,
        vis,
        ident,
        generics,
        data,
    } = parse_macro_input!(input as DeriveInput);

    // Used in the quasi-quotation below as `#name`.
    let name_object = ident;
    let name_object_span = name_object.span();
    let name_object_text = match name_object_span.source_text() {
        Some(name) => name,
        None => unreachable!(),
    };
    if !name_object_text.ends_with("Object") {
        panic!("expected struct [iden]Object, found `{name_object_text:?}`")
    }
    let name_object_name = &name_object_text[..name_object_text.len() - 6];

    let name_view_text = format!("{name_object_name}View");
    let name_view = Ident::new(&name_view_text, name_object_span);

    let name_model_text = name_object_name.to_snake_case();
    let name_model = Ident::new(&name_model_text, name_object_span);

    // Add a bound `T: PipeModelObject` to every type parameter T.
    let generics = add_trait_bounds(generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let DataStruct {
        struct_token,
        fields,
        semi_token: _,
    } = match data {
        Data::Struct(data) => data,
        Data::Enum(_) => panic!("Enum types are not supported"),
        Data::Union(_) => panic!("Union types are not supported"),
    };
    let FieldsNamed {
        brace_token: _,
        named: fields,
    } = match fields {
        Fields::Named(fields) => fields,
        Fields::Unnamed(_) => panic!("Unnamed structs are not supported"),
        Fields::Unit => panic!("Unit structs are not supported"),
    };

    struct FieldToken<'a> {
        ident: &'a Ident,
        ident_ref: Ident,
        ident_mut: Ident,
        ty: &'a Type,
        vis: &'a Visibility,
    }

    let fields: Vec<_> = fields
        .iter()
        .map(|field| {
            let ident = field.ident.as_ref().expect("a named struct");
            let ident_span = ident.span();
            let ident_name = ident_span.source_text().unwrap();
            let ident_ref = Ident::new(&format!("{ident_name}_raw"), ident_span);
            let ident_mut = Ident::new(&format!("{ident_name}_mut_raw"), ident_span);

            FieldToken {
                ident,
                ident_ref,
                ident_mut,
                ty: &field.ty,
                vis: &field.vis,
            }
        })
        .collect();

    let fmts = fields.iter().map(
        |FieldToken {
             ident, ident_ref, ..
         }| {
            quote! {
                let mut fmt = fmt.field(self::__keys::#ident, self.#ident_ref());
            }
        },
    );
    let keys = fields.iter().map(|FieldToken { ident, .. }| {
        quote! {
            pub(super) const #ident: &'static str = stringify!(#ident);
        }
    });
    let inserts = fields.iter().map(|FieldToken { ident, .. }| {
        quote! {
            item.insert(self::__keys::#ident.into(), object.#ident.into());
        }
    });
    let methods_ref = fields.iter().map(
        |FieldToken {
             ident,
             ident_ref,
             vis,
             ..
         }| {
            quote! {
                #vis fn #ident_ref(&self) -> &::xlake_ast::Value {
                    self.item
                        .borrow()
                        .get_raw(self::__keys::#ident)
                        .unwrap()
                }
            }
        },
    );
    let methods_mut = fields.iter().map(
        |FieldToken {
             ident,
             ident_mut,
             ty,
             vis,
             ..
         }| {
            quote! {
                #vis fn #ident(&mut self) -> &mut <#ty as ::xlake_core::object::ValueExt>::Target {
                    self.item
                        .borrow_mut()
                        .get::<#ty>(self::__keys::#ident)
                        .unwrap()
                }

                #vis fn #ident_mut(&mut self) -> &mut ::xlake_ast::Value {
                    self.item
                        .borrow_mut()
                        .get_mut_raw(self::__keys::#ident)
                        .unwrap()
                }
            }
        },
    );
    let validates = fields.iter().map(|FieldToken { ident, .. }| {
        quote! {
            if item.get_raw(self::#ident).is_none() {
                return false;
            }
        }
    });

    let expanded = quote! {
        impl #impl_generics From<#name_object #ty_generics> for ::xlake_ast::Object #where_clause {
            fn from(object: #name_object #ty_generics) -> Self {
                #[allow(unused_mut)]
                let mut item = ::xlake_ast::Object::default();
                #(
                    #inserts
                )*
                item
            }
        }

        impl #impl_generics From<#name_object #ty_generics> for ::xlake_core::object::ObjectLayer #where_clause {
            #[inline]
            fn from(object: #name_object #ty_generics) -> Self {
                let models = self::__keys::__provides();
                ::xlake_core::object::ObjectLayer::from_object(object.into(), models)
            }
        }

        impl #impl_generics From<#name_object #ty_generics> for ::xlake_core::object::LazyObject #where_clause {
            #[inline]
            fn from(object: #name_object #ty_generics) -> Self {
                Self::from(::xlake_core::object::ObjectLayer::from(object))
            }
        }

        impl #impl_generics ::xlake_core::PipeModelObject for #name_object #ty_generics #where_clause {
            type View = #name_view;
            type ViewRef<'a> = #name_view<&'a ::xlake_core::object::LazyObject>;
            type ViewMut<'a> = #name_view<&'a mut ::xlake_core::object::LazyObject>;

            #[inline]
            fn __model_name() -> String {
                self::__keys::__model_name.into()
            }

            #[inline]
            fn __provides() -> ::std::collections::BTreeSet<String> {
                self::__keys::__provides()
            }
        }

        impl #impl_generics ::xlake_core::PipeModelView for #name_object #ty_generics #where_clause {
            #[inline]
            fn __model_name(&self) -> String {
                self::__keys::__model_name.into()
            }

            #[inline]
            fn __provides(&self) -> ::std::collections::BTreeSet<String> {
                self::__keys::__provides()
            }
        }

        #[derive(Copy, Clone, Serialize, Deserialize)]
        #[serde(transparent)]
        #vis #struct_token #name_view<T = ::xlake_core::object::LazyObject> {
            item: T,
        }

        impl<T> ::core::borrow::Borrow<::xlake_core::object::LazyObject> for #name_view<T>
            where
                T: ::core::borrow::Borrow<::xlake_core::object::LazyObject>,
            {
                #[inline]
                fn borrow(&self) -> &::xlake_core::object::LazyObject {
                    self.item.borrow()
                }
            }

            impl<T> ::core::borrow::BorrowMut<::xlake_core::object::LazyObject> for #name_view<T>
            where
                T: ::core::borrow::BorrowMut<::xlake_core::object::LazyObject>,
            {
                #[inline]
                fn borrow_mut(&mut self) -> &mut ::xlake_core::object::LazyObject {
                    self.item.borrow_mut()
                }
            }

        impl<T> #name_view<T>
        where
            T: ::core::borrow::Borrow<::xlake_core::object::LazyObject>,
        {
            #(
                #methods_ref
            )*
        }

        impl<T> #name_view<T>
        where
            T: ::core::borrow::BorrowMut<::xlake_core::object::LazyObject>,
        {
            #(
                #methods_mut
            )*
        }

        impl From<#name_view<::xlake_core::object::LazyObject>> for ::xlake_core::object::LazyObject {
            #[inline]
            fn from(value: #name_view<::xlake_core::object::LazyObject>) -> Self {
                value.item
            }
        }

        impl<'a> From<#name_view<&'a ::xlake_core::object::LazyObject>> for &'a ::xlake_core::object::LazyObject {
            #[inline]
            fn from(value: #name_view<&'a ::xlake_core::object::LazyObject>) -> Self {
                value.item
            }
        }

        impl<'a> From<#name_view<&'a mut ::xlake_core::object::LazyObject>> for &'a mut ::xlake_core::object::LazyObject {
            #[inline]
            fn from(value: #name_view<&'a mut ::xlake_core::object::LazyObject>) -> Self {
                value.item
            }
        }

        impl<T> ::core::fmt::Debug for #name_view<T>
        where
            T: ::core::borrow::Borrow<::xlake_core::object::LazyObject>,
        {
            #[allow(unused_mut)]
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                let mut fmt = f.debug_struct(stringify!(#name_view));
                #(
                    #fmts
                )*
                fmt.finish()
            }
        }

        impl<T> ::xlake_core::PipeModelOwned<T> for #name_view<T>
            where
                T: ::core::borrow::Borrow<::xlake_core::object::LazyObject>
                    + Into<::xlake_core::object::LazyObject>,
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

        impl<T> ::xlake_core::PipeModelView for #name_view<T> {
            #[inline]
            fn __model_name(&self) -> String {
                self::__keys::__model_name.into()
            }

            #[inline]
            fn __provides(&self) -> ::std::collections::BTreeSet<String> {
                self::__keys::__provides()
            }
        }

        #[allow(non_upper_case_globals)]
        mod __keys {
            #(
                #keys
            )*

            pub(super) const __model_name: &'static str = stringify!(#name_model);

            pub(super) fn __provides() -> ::std::collections::BTreeSet<String> {
                let mut set = ::std::collections::BTreeSet::default();
                set.insert(self::__model_name.into());
                set
            }

            pub(super) fn __validate(item: &xlake_core::object::LazyObject) -> bool {
                #(
                    #validates
                )*
                true
            }
        }
    };

    // Hand the output tokens back to the compiler.
    TokenStream::from(expanded)
}

// Add a bound `T: PipeModelObject` to every type parameter T.
fn add_trait_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param
                .bounds
                .push(parse_quote!(xlake_core::PipeModelObject));
        }
    }
    generics
}
