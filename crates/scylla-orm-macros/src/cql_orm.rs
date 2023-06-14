use proc_macro::TokenStream;
use quote::{quote};
use syn::{DeriveInput};

/// #[derive(CqlOrm)] derives FromRow for struct
pub fn cql_orm_derive(tokens_input: TokenStream) -> TokenStream {
    let item = syn::parse::<DeriveInput>(tokens_input).expect("No DeriveInput");
    println!("item: {:#?}", item);

    // let fields_count = struct_fields.named.len();
    let generated = quote! {};

    TokenStream::from(generated)
}
