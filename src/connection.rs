use std::io::Cursor;

use anyhow::{anyhow, Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{
    error::{NetError, ParseError},
    instruction::{self, Instruction},
};

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    waiting_instruction: Option<(Instruction, usize)>,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::new(),
            waiting_instruction: None,
        }
    }

    pub async fn read_instruction(&mut self) -> Result<Instruction> {
        loop {
            match self.waiting_instruction.clone() {
                Some((ins, data_size)) => {
                    // Waiting for data following a instruction
                    let n = self.stream.read_buf(&mut self.buffer).await?;
                    if n == 0 {
                        anyhow::bail!(NetError::ConnClosedByClient)
                    }
                    match self.parse_data(data_size).await {
                        Ok(data) => {
                            self.clear_waiting();
                            return Ok(instruction::complete_ins(ins, data));
                        }
                        Err(err) => match err.downcast_ref() {
                            Some(ParseError::InsufficientData) => {
                                // Need more bytes
                                continue;
                            }
                            _ => anyhow::bail!(err),
                        },
                    }
                }
                None => {
                    // Waiting for new instruction
                    let n = self.stream.read_buf(&mut self.buffer).await?;
                    if n == 0 {
                        anyhow::bail!(NetError::ConnClosedByClient)
                    }
                    match self.parse_command().await {
                        Ok(ins) => return Ok(ins.clone()),
                        Err(e) => match e.downcast_ref() {
                            Some(ParseError::InsufficientWaiting(ins, data_size)) => {
                                self.set_waiting(ins.clone(), *data_size);
                                continue;
                            }
                            Some(ParseError::InsufficientData) => {
                                // Need more bytes
                                continue;
                            }
                            _ => return Err(e),
                        },
                    };
                }
            }
        }
    }

    pub fn set_waiting(&mut self, ins: Instruction, data_size: usize) {
        self.waiting_instruction = Some((ins, data_size));
    }

    pub fn clear_waiting(&mut self) {
        self.waiting_instruction = None;
    }

    pub async fn write_line(&mut self, line: String) -> Result<()> {
        self.stream
            .write_all(line.as_bytes())
            .await
            .context("Failed to write")?;
        self.stream
            .write_all(b"\r\n")
            .await
            .context("Failed to write")?;
        self.stream.flush().await.context("Failed to flush")?;
        Ok(())
    }

    async fn parse_data(&mut self, data_size: usize) -> Result<Bytes> {
        let mut buf_cursor = Cursor::new(&self.buffer[..]);
        let line = get_line(&mut buf_cursor)?; // gets a line till the delimiter \r\n
        let line = String::from_utf8(line.to_vec());
        self.buffer.advance(buf_cursor.position() as usize); // advance the buffer to clear current instruciton
        if line.is_err() {
            anyhow::bail!(ParseError::InvalidData)
        }
        let data = Bytes::from(line.unwrap());

        if data.len() != data_size {
            anyhow::bail!(ParseError::InvalidData)
        }

        Ok(data)
    }

    async fn parse_command(&mut self) -> Result<Instruction> {
        let mut buf_cursor = Cursor::new(&self.buffer[..]);
        let line = get_line(&mut buf_cursor)?; // gets a line till the delimiter \r\n
        let line = String::from_utf8(line.to_vec());
        self.buffer.advance(buf_cursor.position() as usize); // advance the buffer to clear current instruciton
        if line.is_err() {
            anyhow::bail!(ParseError::InvalidInstruction)
        }
        let instruction = instruction::parse_string(line.unwrap())?;

        Ok(instruction)
    }
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8]> {
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);

            return Ok(&src.get_ref()[start..i]);
        }
    }
    Err(anyhow!(ParseError::InsufficientData))
}
