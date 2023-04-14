#[macro_export]
macro_rules! payload {
    ($i:item) => {
        // aliasing to avoid name collisions
        use serde::{Deserialize as __DE, Serialize as __SE};

        #[derive(Debug, Clone, __DE, __SE)]
        #[serde(tag = "type", rename_all = "snake_case")]
        $i
    };
}
