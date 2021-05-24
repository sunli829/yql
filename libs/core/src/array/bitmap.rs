use bytes::{BufMut, Bytes, BytesMut};

#[derive(Default)]
pub struct BitmapBuilder {
    data: BytesMut,
}

impl BitmapBuilder {
    #[inline]
    pub fn set(&mut self, index: usize, is_valid: bool) {
        if !is_valid {
            let data_len = (index / 8) + 1;
            while self.data.len() < data_len {
                self.data.put_u8(0);
            }
            self.data.as_mut()[index / 8] |= 1 << (index % 8);
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn finish(self) -> Bitmap {
        Bitmap {
            offset: 0,
            data: self.data.freeze(),
        }
    }
}

#[derive(Clone)]
pub struct Bitmap {
    offset: usize,
    data: Bytes,
}

impl Bitmap {
    pub fn offset(&self, offset: usize) -> Bitmap {
        Bitmap {
            offset: self.offset + offset,
            data: self.data.clone(),
        }
    }

    #[inline]
    pub fn is_null(&self, index: usize) -> bool {
        self.data
            .get((index + self.offset) / 8)
            .map(|x| (*x & (0x1 << ((index + self.offset) % 8))) > 0)
            .unwrap_or_default()
    }

    #[inline]
    pub fn is_valid(&self, index: usize) -> bool {
        !self.is_null(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        let mut builder = BitmapBuilder::default();

        builder.set(8, false);
        builder.set(13, false);
        builder.set(5, false);
        builder.set(0, false);

        assert_eq!(builder.data.len(), 2);
        assert_eq!(builder.data, [(1u8 << 5) | 1, 1 | (1u8 << 5)].as_ref());
    }

    #[test]
    fn test_bitmap() {
        let mut builder = BitmapBuilder::default();

        builder.set(8, false);
        builder.set(13, false);
        builder.set(5, false);
        builder.set(0, false);

        let bitmap = builder.finish();

        assert!(bitmap.is_null(0));
        assert!(bitmap.is_null(5));
        assert!(bitmap.is_null(8));
        assert!(bitmap.is_null(13));

        assert!(bitmap.is_valid(1));
        assert!(bitmap.is_valid(3));
        assert!(bitmap.is_valid(9));
        assert!(bitmap.is_valid(14));
        assert!(bitmap.is_valid(100));
    }

    #[test]
    fn test_slice() {
        let mut builder = BitmapBuilder::default();

        builder.set(8, false);
        builder.set(13, false);
        builder.set(5, false);
        builder.set(0, false);

        let bitmap = builder.finish();
        let bitmap2 = bitmap.offset(3);

        assert!(bitmap2.is_null(2));
        assert!(bitmap2.is_null(5));
        assert!(bitmap2.is_null(10));
    }
}
