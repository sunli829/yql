use anyhow::Result;

use crate::array::DataType;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Signature {
    Variadic(&'static [DataType]),
    VariadicEqual,
    Uniform(usize, &'static [DataType]),
    Exact(&'static [DataType]),
    Any(usize),
    OneOf(&'static [Signature]),
}

impl Signature {
    fn get_valid_types(&self, current_types: &[DataType]) -> Result<Vec<Vec<DataType>>> {
        let valid_types: Vec<Vec<DataType>> = match self {
            Signature::Variadic(valid_types) => valid_types
                .iter()
                .copied()
                .map(|valid_type| current_types.iter().map(|_| valid_type).collect())
                .collect(),
            Signature::Uniform(number, valid_types) => valid_types
                .iter()
                .copied()
                .map(|valid_type| (0..*number).map(|_| valid_type).collect())
                .collect(),
            Signature::VariadicEqual => {
                vec![current_types.iter().map(|_| current_types[0]).collect()]
            }
            Signature::Exact(valid_types) => vec![valid_types.to_vec()],
            Signature::Any(number) => {
                anyhow::ensure!(
                    current_types.len() == *number,
                    "invalid arguments number expect: {} actual: {}",
                    *number,
                    current_types.len()
                );
                vec![(0..*number).map(|i| current_types[i]).collect()]
            }
            Signature::OneOf(types) => {
                let mut r = Vec::new();
                for s in *types {
                    r.extend(s.get_valid_types(current_types)?);
                }
                r
            }
        };

        Ok(valid_types)
    }

    fn maybe_data_types(
        valid_types: &[DataType],
        current_types: &[DataType],
    ) -> Option<Vec<DataType>> {
        if valid_types.len() != current_types.len() {
            return None;
        }

        let mut new_type = Vec::with_capacity(valid_types.len());
        for (i, valid_type) in valid_types.iter().enumerate() {
            let current_type = &current_types[i];

            if current_type == valid_type {
                new_type.push(*current_type)
            } else if current_type.can_cast_to(*valid_type) {
                new_type.push(*valid_type)
            } else {
                return None;
            }
        }
        Some(new_type)
    }

    pub fn data_types(&self, current_types: &[DataType]) -> Result<Vec<DataType>> {
        anyhow::ensure!(!current_types.is_empty(), "requires at least one argument.");

        let valid_types = self.get_valid_types(current_types)?;

        if valid_types
            .iter()
            .any(|data_type| data_type == &*current_types)
        {
            return Ok(current_types.to_vec());
        }

        for valid_types in valid_types {
            if let Some(types) = Self::maybe_data_types(&valid_types, &current_types) {
                return Ok(types);
            }
        }

        anyhow::bail!("can't coerce arguments")
    }
}
