// concordance-derive/src/lib.rs - Enhanced for Phase 2

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, Attribute, Meta, Expr, Lit};

/// Enhanced derive macro that generates real dispatch handlers
#[proc_macro_derive(Aggregate, attributes(aggregate))]
pub fn derive_aggregate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    
    match expand_aggregate(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_aggregate(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    // Validate this is a struct
    match &input.data {
        Data::Struct(data_struct) => {
            validate_struct_fields(&data_struct.fields)?;
        }
        Data::Enum(_) => {
            return Err(syn::Error::new_spanned(
                input,
                "Aggregate can only be derived for structs, not enums"
            ));
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                input,
                "Aggregate can only be derived for structs, not unions"
            ));
        }
    }

    // Extract aggregate name from attributes
    let aggregate_name = extract_aggregate_name(&input.attrs)?;
    
    // Get the type name for the registration
    let type_name = &input.ident;
    
    // 🎉 PHASE 2: Generate registration with actual dispatch handler!
    let expanded = quote! {
        // Automatically register this aggregate type with REAL dispatch
        ::concordance::concordance_core::inventory::submit! {
            ::concordance::concordance_core::create_aggregate_descriptor::<#type_name>()
        }
    };

    Ok(expanded)
}

fn validate_struct_fields(fields: &Fields) -> syn::Result<()> {
    // Check if struct has a 'key' field of type String
    let has_key_field = match fields {
        Fields::Named(fields_named) => {
            fields_named.named.iter().any(|field| {
                if let Some(ident) = &field.ident {
                    ident == "key"
                } else {
                    false
                }
            })
        }
        Fields::Unnamed(_) => false,
        Fields::Unit => false,
    };

    if !has_key_field {
        return Err(syn::Error::new_spanned(
            fields,
            "Aggregate structs must have a 'key' field of type String"
        ));
    }

    Ok(())
}

fn extract_aggregate_name(attrs: &[Attribute]) -> syn::Result<String> {
    for attr in attrs {
        if attr.path().is_ident("aggregate") {
            return parse_aggregate_attribute(attr);
        }
    }
    
    Err(syn::Error::new_spanned(
        attrs.first(),
        "Missing #[aggregate(name = \"...\")] attribute. Please specify the aggregate name."
    ))
}

fn parse_aggregate_attribute(attr: &Attribute) -> syn::Result<String> {
    match &attr.meta {
        Meta::List(meta_list) => {
            // Parse tokens inside #[aggregate(...)]
            let nested: syn::punctuated::Punctuated<Meta, syn::Token![,]> = 
                meta_list.parse_args_with(syn::punctuated::Punctuated::parse_terminated)?;
            
            for meta in nested {
                if let Meta::NameValue(name_value) = meta {
                    if name_value.path.is_ident("name") {
                        if let Expr::Lit(expr_lit) = &name_value.value {
                            if let Lit::Str(lit_str) = &expr_lit.lit {
                                let name = lit_str.value();
                                if name.is_empty() {
                                    return Err(syn::Error::new_spanned(
                                        lit_str,
                                        "Aggregate name cannot be empty"
                                    ));
                                }
                                if !is_valid_aggregate_name(&name) {
                                    return Err(syn::Error::new_spanned(
                                        lit_str,
                                        "Aggregate name must contain only lowercase letters, numbers, and underscores"
                                    ));
                                }
                                return Ok(name);
                            }
                        }
                        return Err(syn::Error::new_spanned(
                            &name_value.value,
                            "Aggregate name must be a string literal"
                        ));
                    }
                }
            }
            
            Err(syn::Error::new_spanned(
                meta_list,
                "Expected #[aggregate(name = \"aggregate_name\")]"
            ))
        }
        _ => Err(syn::Error::new_spanned(
            attr,
            "Expected #[aggregate(name = \"aggregate_name\")]"
        ))
    }
}

fn is_valid_aggregate_name(name: &str) -> bool {
    !name.is_empty() 
        && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && name.chars().next().unwrap().is_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_aggregate_names() {
        assert!(is_valid_aggregate_name("order"));
        assert!(is_valid_aggregate_name("user_account"));
        assert!(is_valid_aggregate_name("order123"));
        assert!(is_valid_aggregate_name("a"));
    }

    #[test]
    fn test_invalid_aggregate_names() {
        assert!(!is_valid_aggregate_name(""));
        assert!(!is_valid_aggregate_name("Order"));
        assert!(!is_valid_aggregate_name("order-name"));
        assert!(!is_valid_aggregate_name("123order"));
        assert!(!is_valid_aggregate_name("order name"));
        assert!(!is_valid_aggregate_name("_order"));
    }
}