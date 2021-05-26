use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::array::DataType;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub qualifier: Option<String>,
    pub name: String,
    pub data_type: DataType,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            qualifier: None,
            name: name.into(),
            data_type,
        }
    }

    pub fn qualified_name(&self) -> String {
        match &self.qualifier {
            Some(qualified_name) => format!("{}.{}", qualified_name, self.name),
            None => self.name.clone(),
        }
    }
}

pub type SchemaRef = Arc<Schema>;

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    pub fn try_new(fields: Vec<Field>) -> Result<Self> {
        let mut qualified_names = HashSet::new();
        let mut unqualified_names = HashSet::new();

        for field in &fields {
            if let Some(qualifier) = &field.qualifier {
                if !qualified_names.insert((qualifier, &field.name)) {
                    anyhow::bail!(
                        "schema contains duplicate qualified field name: '{}'",
                        field.qualified_name()
                    );
                }
            } else if !unqualified_names.insert(&field.name) {
                anyhow::bail!(
                    "schema contains duplicate unqualified field name: '{}'",
                    field.name
                );
            }
        }

        let mut qualified_names = qualified_names
            .iter()
            .map(|(l, r)| (l.to_owned(), r.to_owned()))
            .collect::<Vec<(&String, &String)>>();
        qualified_names.sort_by(|a, b| {
            let a = format!("{}.{}", a.0, a.1);
            let b = format!("{}.{}", b.0, b.1);
            a.cmp(&b)
        });

        for (qualifier, name) in &qualified_names {
            if unqualified_names.contains(name) {
                anyhow::bail!(
                    "schema contains qualified field name '{}.{}' \
                    and unqualified field name '{}' which would be ambiguous",
                    qualifier,
                    name,
                    name
                );
            }
        }

        Ok(Self { fields })
    }

    pub fn field(&self, qualifier: Option<&str>, name: &str) -> Option<(usize, &Field)> {
        match qualifier {
            Some(qualifier) => self
                .fields
                .iter()
                .enumerate()
                .find(|(_, f)| match &f.qualifier {
                    Some(field_qualifier) => {
                        field_qualifier.eq_ignore_ascii_case(qualifier)
                            && f.name.eq_ignore_ascii_case(name)
                    }
                    None => false,
                }),
            None => self
                .fields
                .iter()
                .enumerate()
                .find(|(_, f)| f.name.eq_ignore_ascii_case(name)),
        }
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }
}
