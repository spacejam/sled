use super::*;

/// InvertibleTransforms are transformations on the raw log message buffers
/// buffers before they are persisted to disk and when they are first read
/// from disk.
///
/// `InvertibleTransform::forward` is called prior to writing to disk
/// `InvertibleTransform::backward` is called after reading from disk
///
/// InvertibleTransforms can be used to implement custum compression,
/// encryption and other goodies on the raw binary messages.
///
/// InvertibleTransformations can be chained together by passing the output
/// of one transform to the next on the forwards pass, and applying the same
/// transforms in the reverse order on the backwards pass.
///
/// **Notes for testing**
/// all transforms should pass the inverse law
///   i.e. `backwards(forwards(data) == data`
///
/// The trait has `InvertibleTransform::test_inverse_law` which can be used in
/// tests to help test this property.
pub trait InvertibleTransform {
    type Error: std::error::Error;

    /// This is where we apply our transformation to raw message data.
    /// e.g. In a compression transform, this is where we compress.
    fn forward(&self, &[u8]) -> Result<Vec<u8>, Self::Error>;

    /// This is where we undo the transformation done in ::forward.
    /// e.g. In a compression transform, this is where we decompress.
    fn backward(&self, &[u8]) -> Result<Vec<u8>, Self::Error>;

    #[cfg(test)]
    fn test_inverse_law(&self, buf: &[u8]) -> Result<(), Self::Error> {
        assert_eq!(buf, &*self.backward(&self.forward(&buf)?)?);
        Ok(())
    }
}
