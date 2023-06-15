use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, FieldsNamed};

/// #[proc_macro_derive(CqlOrm)]
pub fn cql_orm(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);

    let struct_name = &ast.ident;
    let fields = match ast.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(FieldsNamed { named, .. }) => named,
            _ => panic!("CqlOrm can only be derived for structs with named fields"),
        },
        _ => panic!("CqlOrm can only be derived for structs"),
    };

    let field_names0 = fields.iter().map(|f| &f.ident);
    let field_names1 = field_names0.clone();
    let field_names_string0 = fields.iter().map(|f| f.ident.as_ref().unwrap().to_string());
    let field_names_string1 = field_names_string0.clone();
    let field_names_string2 = field_names_string0.clone();
    let field_names_string3 = field_names_string0.clone();
    let fields_num = field_names0.len();

    let expanded = quote! {
        impl #struct_name {
            pub fn fields() -> Vec<&'static str> {
                vec![
                    #(#field_names_string0),*
                ]
            }

            pub fn fill(&mut self, cols: &scylla_orm::ColumnsMap) {
                #(
                    if cols.has(#field_names_string1) {
                        self.#field_names0 = cols.get_as(#field_names_string2).unwrap_or_default();
                    }
                )*
            }

            pub fn to(&self) -> anyhow::Result<scylla_orm::ColumnsMap> {
                let mut cols = scylla_orm::ColumnsMap::with_capacity(#fields_num);
                #(
                    cols.set_as(#field_names_string3, &self.#field_names1)?;
                )*
                Ok(cols)
            }
        }
    };

    TokenStream::from(expanded)
}
