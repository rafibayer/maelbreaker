//! https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#errors

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    Timeout = 0,
    NodeNotFound = 1,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TxnConflict = 30,
}

impl ErrorCode {
    pub fn code(self) -> u8 {
        self as u8
    }
}

// useless, I just love pattern matching :)
pub fn is_definite(error: ErrorCode) -> bool {
    use ErrorCode::*;
    !matches!(error, Timeout | Crash)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_compare_u8() {
        assert_eq!(0, ErrorCode::Timeout.code())
    }
}
