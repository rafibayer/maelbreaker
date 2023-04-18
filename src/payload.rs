/// Helper macro to derive the necessary traits on an enum to implement Payload.
/// also marks the enum with serde attributes to type-tag and rename as snake_case
#[macro_export]
macro_rules! payload {
    // add option to specifiy aliases if somehow this collides with your naming
    ($de:ident, $se:ident, $i:item) => {
        use serde::{Deserialize as $de, Serialize as $se};

        #[derive(Debug, Clone, PartialEq, Eq, $de, $se)]
        #[serde(tag = "type", rename_all = "snake_case")]
        $i
    };
    ($i:item) => {
        payload!(__DE, __SE, $i);
    };
}
