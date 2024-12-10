use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, parse_quote, Data, DataStruct, DeriveInput, Fields, FieldsNamed,
    GenericParam, Generics, Ident, Type, Visibility,
};

#[proc_macro_derive(PipeModel)]
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
    let name_view_text = format!("{}View", &name_object_text[..name_object_text.len() - 6]);
    let name_view = Ident::new(&name_view_text, name_object_span);

    // Add a bound `T: PipeModel` to every type parameter T.
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
            let ident_ref = Ident::new(&format!("{ident_name}_raw"), ident_span.clone());
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
            map.insert(self::__keys::#ident.into(), object.#ident.into());
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
                #vis fn #ident(&mut self) -> &mut <#ty as ::xlake_core::PipeModelEntity>::Target {
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
            if item.borrow().get_raw(self::__keys::#ident).is_none() {
                return Err(item);
            }
        }
    });

    let expanded = quote! {
        impl #impl_generics From<#name_object #ty_generics> for ::xlake_core::LazyObject #where_clause {
            fn from(object: #name_object #ty_generics) -> Self {
                #[allow(unused_mut)]
                let mut map = ::xlake_ast::Object::default();
                #(
                    #inserts
                )*
                map.into()
            }
        }

        impl #impl_generics ::xlake_core::PipeModelValue for #name_object #ty_generics #where_clause {
            type View = #name_view;
            type ViewRef<'a> = #name_view<&'a ::xlake_core::LazyObject>;
            type ViewMut<'a> = #name_view<&'a mut ::xlake_core::LazyObject>;
        }

        #[derive(Copy, Clone, Serialize, Deserialize)]
        #[serde(transparent)]
        #vis #struct_token #name_view<T = ::xlake_core::LazyObject> {
            item: T,
        }

        impl<T> ::core::borrow::Borrow<::xlake_core::LazyObject> for #name_view<T>
            where
                T: ::core::borrow::Borrow<::xlake_core::LazyObject>,
            {
                #[inline]
                fn borrow(&self) -> &::xlake_core::LazyObject {
                    self.item.borrow()
                }
            }

            impl<T> ::core::borrow::BorrowMut<::xlake_core::LazyObject> for #name_view<T>
            where
                T: ::core::borrow::BorrowMut<::xlake_core::LazyObject>,
            {
                #[inline]
                fn borrow_mut(&mut self) -> &mut ::xlake_core::LazyObject {
                    self.item.borrow_mut()
                }
            }

        impl<T> #name_view<T>
        where
            T: ::core::borrow::Borrow<::xlake_core::LazyObject>,
        {
            #(
                #methods_ref
            )*
        }

        impl<T> #name_view<T>
        where
            T: ::core::borrow::BorrowMut<::xlake_core::LazyObject>,
        {
            #(
                #methods_mut
            )*
        }

        impl<T> ::core::fmt::Debug for #name_view<T>
        where
            T: ::core::borrow::Borrow<::xlake_core::LazyObject>,
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

        impl From<#name_view<::xlake_core::LazyObject>> for ::xlake_core::LazyObject {
            #[inline]
            fn from(value: #name_view<::xlake_core::LazyObject>) -> Self {
                value.item
            }
        }

        impl<'a> From<#name_view<&'a ::xlake_core::LazyObject>> for &'a ::xlake_core::LazyObject {
            #[inline]
            fn from(value: #name_view<&'a ::xlake_core::LazyObject>) -> Self {
                value.item
            }
        }

        impl<'a> From<#name_view<&'a mut ::xlake_core::LazyObject>>
            for &'a mut ::xlake_core::LazyObject
        {
            #[inline]
            fn from(value: #name_view<&'a mut ::xlake_core::LazyObject>) -> Self {
                value.item
            }
        }

        impl<T> ::xlake_core::PipeModelView<T> for #name_view<T>
        where
            T: Unpin + ::core::borrow::Borrow<::xlake_core::LazyObject> + From<#name_view<T>>,
        {
            fn cast(item: T) -> Result<Self, T> {
                #(
                    #validates
                )*
                Ok(Self { item })
            }
        }

        #[allow(non_upper_case_globals)]
        mod __keys {
            #(
                #keys
            )*
        }
    };

    // Hand the output tokens back to the compiler.
    TokenStream::from(expanded)
}

// Add a bound `T: PipeModel` to every type parameter T.
fn add_trait_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(xlake_core::PipeModel));
        }
    }
    generics
}
