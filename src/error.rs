use std::fmt::{self, Debug, Display, Formatter};

use crate::instruction::Instruction;

#[derive(Debug, Clone)]
pub enum ParseError {
    InsufficientData,
    InsufficientWaiting(Instruction, usize),
    InvalidInstruction,
    InvalidData,
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::InsufficientData => write!(f, "INSUFFICIENT DATA"),
            ParseError::InsufficientWaiting(_, _) => write!(f, "WAITING FOR DATA"),
            ParseError::InvalidInstruction => write!(f, "INVALID INSTRUCION"),
            ParseError::InvalidData => write!(f, "INVALID DATA"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum NetError {
    ConnClosedByClient,
}

impl Display for NetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            NetError::ConnClosedByClient => write!(f, "CONNECTION CLOSED BY CLIENT"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CleanupError {
    NeedToRepeat,
}

impl Display for CleanupError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CleanupError::NeedToRepeat => write!(f, "NEED TO REPEAT"),
        }
    }
}
