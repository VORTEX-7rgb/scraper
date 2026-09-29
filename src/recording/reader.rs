use crate::error::{EngineError, Result};
use crate::recording::types::{CURRENT_SCHEMA_VERSION, ResearchEvent};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

/// Reader for parsing and validating NDJSON canonical research event streams.
pub struct ResearchReader<R: Read> {
    reader: BufReader<R>,
    line_number: usize,
}

impl<R: Read> ResearchReader<R> {
    /// Create a new research stream reader.
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            line_number: 0,
        }
    }

    /// Read the next valid `ResearchEvent` from the stream.
    ///
    /// Returns `Ok(Some(event))` on success, `Ok(None)` on clean EOF,
    /// or `Err(...)` if a malformed JSON line, unsupported schema version,
    /// truncated record, or invariant failure is encountered.
    pub fn next_event(&mut self) -> Result<Option<ResearchEvent>> {
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = self.reader.read_line(&mut line)?;
            if bytes_read == 0 {
                return Ok(None); // EOF
            }

            self.line_number += 1;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue; // Skip blank lines
            }

            // Parse line into ResearchEvent
            let event: ResearchEvent =
                serde_json::from_str(trimmed).map_err(|err| EngineError::CorruptedRecord {
                    line: self.line_number,
                    reason: format!("JSON deserialization error: {err}"),
                })?;

            // Validate schema version
            if event.schema_version != CURRENT_SCHEMA_VERSION {
                return Err(EngineError::UnsupportedSchemaVersion {
                    found: event.schema_version,
                    supported: CURRENT_SCHEMA_VERSION,
                });
            }

            // Validate invariants
            event
                .validate()
                .map_err(|err| EngineError::CorruptedRecord {
                    line: self.line_number,
                    reason: err.to_string(),
                })?;

            return Ok(Some(event));
        }
    }

    /// Read all remaining events in the stream into a vector.
    pub fn read_all(&mut self) -> Result<Vec<ResearchEvent>> {
        let mut events = Vec::new();
        while let Some(event) = self.next_event()? {
            events.push(event);
        }
        Ok(events)
    }

    /// Current line number in the stream.
    pub fn line_number(&self) -> usize {
        self.line_number
    }
}

impl ResearchReader<File> {
    /// Open a file for reading research events.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self::new(file))
    }
}
