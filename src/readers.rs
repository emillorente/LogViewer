use std::fs;
use std::io::{BufRead, BufReader, Error as IoError, Read, Seek, SeekFrom};
use std::path::Path;

pub trait LogReader {
    fn seek(&mut self, pos: u64) -> Result<(), IoError>;
    fn tell(&self) -> u64;
    fn read_record(&mut self) -> Result<Option<String>, IoError>;
}

pub struct LogFile {
    file: BufReader<fs::File>,
    pos: u64,
}

impl LogFile {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<LogFile, IoError> {
        Ok(LogFile {
            file: BufReader::new(fs::File::open(path)?),
            pos: 0,
        })
    }
}

impl LogReader for LogFile {
    fn seek(&mut self, pos: u64) -> Result<(), IoError> {
        self.file.seek(SeekFrom::Start(pos))?;
        self.pos = pos;
        Ok(())
    }

    fn tell(&self) -> u64 {
        self.pos
    }

    fn read_record(&mut self) -> Result<Option<String>, IoError> {
        let mut line = String::new();
        let ret = self.file.read_line(&mut line)?;
        if ret == 0 {
            Ok(None)
        } else {
            self.pos += ret as u64;
            if line.len() >= 2 && line.ends_with("\r\n") {
                line.pop();
                line.pop();
            } else if !line.is_empty() && line.ends_with("\n") {
                line.pop();
            }
            Ok(Some(line))
        }
    }
}

/// Reader for CORE.OUT format: records are delimited by `~@_~` (not by newlines).
/// This handles multi-line XML messages that span many physical lines.
pub struct LogCoreReader {
    file: BufReader<fs::File>,
    pos: u64,
}

impl LogCoreReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<LogCoreReader, IoError> {
        Ok(LogCoreReader {
            file: BufReader::new(fs::File::open(path)?),
            pos: 0,
        })
    }
}

impl LogReader for LogCoreReader {
    fn seek(&mut self, pos: u64) -> Result<(), IoError> {
        self.file.seek(SeekFrom::Start(pos))?;
        self.pos = pos;
        Ok(())
    }

    fn tell(&self) -> u64 {
        self.pos
    }

    fn read_record(&mut self) -> Result<Option<String>, IoError> {
        let mut record = String::new();
        let mut state: u8 = 0;
        let mut started = false;

        loop {
            let mut byte = [0u8; 1];
            match self.file.read(&mut byte) {
                Ok(0) => {
                    if record.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(record));
                }
                Ok(_) => {
                    self.pos += 1;
                    let c = byte[0] as char;
                    // Skip leading whitespace/newlines before first record byte
                    if !started && (c == '\n' || c == '\r' || c == ' ' || c == '\t') {
                        continue;
                    }
                    started = true;
                    record.push(c);

                    match state {
                        0 if c == '~' => state = 1,
                        1 if c == '@' => state = 2,
                        2 if c == '_' => state = 3,
                        3 if c == '~' => return Ok(Some(record)),
                        _ => state = if c == '~' { 1 } else { 0 },
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

pub struct LogQueryReader {
    file: BufReader<fs::File>,
    pos: u64,
}

impl LogQueryReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<LogQueryReader, IoError> {
        Ok(LogQueryReader {
            file: BufReader::new(fs::File::open(path)?),
            pos: 0,
        })
    }

    fn read_line_trim(&mut self) -> Result<Option<String>, IoError> {
        let mut line = String::new();
        let ret = self.file.read_line(&mut line)?;
        if ret == 0 {
            return Ok(None);
        }
        self.pos += ret as u64;
        if line.len() >= 2 && line.ends_with("\r\n") { line.pop(); line.pop(); }
        else if !line.is_empty() && line.ends_with("\n") { line.pop(); }
        Ok(Some(line))
    }
}

impl LogReader for LogQueryReader {
    fn seek(&mut self, pos: u64) -> Result<(), IoError> {
        self.file.seek(SeekFrom::Start(pos))?;
        self.pos = pos;
        Ok(())
    }

    fn tell(&self) -> u64 {
        self.pos
    }

    fn read_record(&mut self) -> Result<Option<String>, IoError> {
        let header = loop {
            match self.read_line_trim()? {
                None => return Ok(None),
                Some(line) if !line.is_empty() => break line,
                _ => continue,
            }
        };

        let sql = loop {
            match self.read_line_trim()? {
                None => break String::new(),
                Some(line) if !line.is_empty() && line != "go" => break line,
                _ => continue,
            }
        };

        // skip "go" line
        let _ = self.read_line_trim()?;
        // skip blank line after go
        let _ = self.read_line_trim()?;

        Ok(Some(header + "~" + &sql))
    }
}

pub fn detect_reader<P: AsRef<Path>>(path: P) -> Result<Box<dyn LogReader>, IoError> {
    let path = path.as_ref();
    let mut file = BufReader::new(fs::File::open(path)?);
    let mut buf = [0u8; 512];
    let n = file.read(&mut buf)?;
    let head = String::from_utf8_lossy(&buf[..n]);

    drop(file);
    if head.trim_start().starts_with("/***") {
        Ok(Box::new(LogQueryReader::open(path)?))
    } else if head.contains("~@_~") {
        Ok(Box::new(LogCoreReader::open(path)?))
    } else {
        Ok(Box::new(LogFile::open(path)?))
    }
}
