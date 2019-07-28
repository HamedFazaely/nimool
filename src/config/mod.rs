use log::{Log, Record, Level, Metadata, SetLoggerError, LevelFilter};
use std::time::Duration;


pub struct SimpleLogger;


impl Log for SimpleLogger {
    /// Determines if a log message with the specified metadata would be
    /// logged.
    ///
    /// This is used by the `log_enabled!` macro to allow callers to avoid
    /// expensive computation of log message arguments if the message would be
    /// discarded anyway.
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Trace
    }

    /// Logs the `Record`.
    ///
    /// Note that `enabled` is *not* necessarily called before this method.
    /// Implementations of `log` should perform all necessary filtering
    /// internally.
    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }

    /// Flushes any buffered records.
    fn flush(&self) {}
}

pub fn init_logger(level: LevelFilter, logger: &'static Log) -> Result<(), SetLoggerError> {
    log::set_max_level(level);
    let res = log::set_logger(logger);
    info!("logger initialized");
    res
}


pub struct AppConf {
    pub index_path: &'static str,
    pub writer_buff_size: usize,
    pub listen_address: &'static str,
    pub listen_port: u16,
    pub auto_commit_interval: Duration,
}

