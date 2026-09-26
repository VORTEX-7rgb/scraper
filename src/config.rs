use crate::error::{EngineError, Result};
use crate::types::{MarketType, VenueId};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppConfig {
    pub app: AppSettings,
    pub storage: StorageSettings,
    pub engine: EngineSettings,
    pub venues: Vec<VenueSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppSettings {
    pub name: String,
    pub environment: String,
    pub log_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageSettings {
    pub data_dir: PathBuf,
    pub sqlite_path: PathBuf,
    pub zstd_raw_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EngineSettings {
    pub min_spread_bps: f64,
    pub test_notional_tiers: Vec<f64>,
    pub max_book_age_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VenueSettings {
    pub id: VenueId,
    pub enabled: bool,
    pub symbols: Vec<String>,
    pub market_types: Vec<MarketType>,
}

impl AppConfig {
    /// Load and validate configuration from a TOML file.
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())?;
        let config: Self = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Perform structural invariant validations on configuration values.
    pub fn validate(&self) -> Result<()> {
        if self.app.name.trim().is_empty() {
            return Err(EngineError::Config("app.name cannot be empty".into()));
        }

        if self.engine.min_spread_bps <= 0.0 {
            return Err(EngineError::Config(
                "engine.min_spread_bps must be greater than zero".into(),
            ));
        }

        if self.engine.test_notional_tiers.is_empty() {
            return Err(EngineError::Config(
                "engine.test_notional_tiers cannot be empty".into(),
            ));
        }

        for &tier in &self.engine.test_notional_tiers {
            if tier <= 0.0 {
                return Err(EngineError::Config(format!(
                    "Invalid notional tier {tier}: must be greater than zero"
                )));
            }
        }

        if self.venues.is_empty() {
            return Err(EngineError::Config(
                "At least one venue must be configured".into(),
            ));
        }

        for venue in &self.venues {
            if venue.enabled && venue.symbols.is_empty() {
                return Err(EngineError::Config(format!(
                    "Enabled venue '{}' must configure at least one symbol",
                    venue.id
                )));
            }
        }

        Ok(())
    }
}
