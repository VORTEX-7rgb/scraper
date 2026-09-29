use crate::error::{EngineError, Result};
use crate::observatory::types::{
    DislocationObservation, OpportunityKey, OpportunityRecord, PersistenceTransition,
};
use crate::recording::types::ResearchEvent;
use crate::types::MarketEvent;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

/// High-fidelity append-only recorder for research market and dislocation event streams.
pub struct ResearchRecorder<W: Write> {
    writer: BufWriter<W>,
    record_count: u64,
    auto_flush: bool,
}

impl<W: Write> ResearchRecorder<W> {
    /// Create a new recorder with an existing writer.
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::new(writer),
            record_count: 0,
            auto_flush: false,
        }
    }

    /// Create a new recorder specifying auto-flush policy.
    pub fn with_auto_flush(writer: W, auto_flush: bool) -> Self {
        Self {
            writer: BufWriter::new(writer),
            record_count: 0,
            auto_flush,
        }
    }

    /// Number of records successfully written to the stream.
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Append a canonical research event envelope to the stream.
    pub fn append(&mut self, event: &ResearchEvent) -> Result<()> {
        event.validate()?;

        let json = serde_json::to_string(event).map_err(|err| {
            EngineError::Recording(format!(
                "Serialization failure for event {:?}: {err}",
                event.event_type
            ))
        })?;

        self.writer.write_all(json.as_bytes())?;
        self.writer.write_all(b"\n")?;
        self.record_count += 1;

        if self.auto_flush {
            self.writer.flush()?;
        }

        Ok(())
    }

    /// Record a raw market event.
    pub fn record_market_event(&mut self, timestamp_ns: i64, event: MarketEvent) -> Result<()> {
        let envelope = ResearchEvent::new_market_event(timestamp_ns, event);
        self.append(&envelope)
    }

    /// Record a cross-book dislocation observation.
    pub fn record_dislocation(&mut self, observation: DislocationObservation) -> Result<()> {
        let envelope = ResearchEvent::new_dislocation(observation);
        self.append(&envelope)
    }

    /// Record an opportunity persistence transition.
    pub fn record_transition(
        &mut self,
        key: OpportunityKey,
        timestamp_ns: i64,
        transition: PersistenceTransition,
    ) -> Result<()> {
        let envelope = ResearchEvent::new_transition(key, timestamp_ns, transition);
        self.append(&envelope)
    }

    /// Record a finalized persistent opportunity record.
    pub fn record_opportunity(&mut self, record: OpportunityRecord) -> Result<()> {
        let envelope = ResearchEvent::new_opportunity(record);
        self.append(&envelope)
    }

    /// Explicitly flush internal write buffers.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Flush buffers and return the underlying writer.
    pub fn into_inner(mut self) -> Result<W> {
        self.writer.flush()?;
        self.writer
            .into_inner()
            .map_err(|err| EngineError::Io(err.into_error()))
    }
}

impl ResearchRecorder<File> {
    /// Create or append to a file at the specified path.
    pub fn open_file(path: impl AsRef<Path>, auto_flush: bool) -> Result<Self> {
        let path_ref = path.as_ref();
        if let Some(parent) = path_ref.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path_ref)?;

        Ok(Self::with_auto_flush(file, auto_flush))
    }

    /// Create a new file (truncating existing) at the specified path.
    pub fn create_file(path: impl AsRef<Path>, auto_flush: bool) -> Result<Self> {
        let path_ref = path.as_ref();
        if let Some(parent) = path_ref.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path_ref)?;

        Ok(Self::with_auto_flush(file, auto_flush))
    }
}
