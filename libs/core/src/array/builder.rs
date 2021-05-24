use std::any::Any;

/// Trait for dealing with different array builders at runtime.
pub trait ArrayBuilder {
    fn as_any(&self) -> &dyn Any;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;
}
