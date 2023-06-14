use proc_macro::TokenStream;

mod cql_orm;

/// #[derive(CqlOrm)] derives CqlOrm for struct
/// Works only on simple structs without generics etc
#[proc_macro_derive(CqlOrm, attributes(cql))]
pub fn cql_orm_derive(tokens_input: TokenStream) -> TokenStream {
    cql_orm::cql_orm_derive(tokens_input)
}
