use chrono::{DateTime, Utc};
use num_bigint::BigUint;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::{debug, error};

const DEFAULT_BITS_MACHINE: u8 = 3; // up to 8 machines
const DEFAULT_BITS_GENERATOR: u8 = 10; // 0-1023
const DEFAULT_BITS_SEQUENCE: u8 = 8; // 0-255

struct State {
    sequence: u64,
    time_prev: u64,
}

/// The Maylily struct represents an ID generator based on configurable parameters like machine ID,
/// generator ID, and time base. It follows the builder pattern to allow for customization.
///
/// # Examples
///
/// Basic usage:
/// ```
/// use maylily::Maylily;
///
/// #[tokio::main]
/// async fn main() {
///     let maylily = Maylily::new();
///     let id = maylily.generate().await.unwrap();
///     println!("Generated ID: {}", id);
/// }
/// ```
///
/// Customizing settings:
/// ```
/// use maylily::Maylily;
///
/// #[tokio::main]
/// async fn main() {
///     let maylily = Maylily::new()
///         .machine_id(1)
///         .radix(16); // Set radix to hexadecimal
///     let id = maylily.generate().await.unwrap();
///     println!("Generated ID: {}", id);
/// }
/// ```
#[derive(Clone)]
pub struct Maylily {
    radix: u32,
    time_base: DateTime<Utc>,
    machine_id: u64,
    machine_bits: u8,
    generator_id: u64,
    generator_bits: u8,
    sequence_bits: u8,
    state: Arc<Mutex<State>>,
}

impl Maylily {
    /// Creates a new instance of the Maylily ID generator with default settings.
    /// The default configuration uses a machine ID of `0`, a radix of `10` (decimal), and the
    /// generator ID is derived from the process ID.
    ///
    /// # Examples
    ///
    /// ```
    /// let maylily = Maylily::new();
    /// ```
    pub fn new() -> Self {
        let generator_id = std::process::id() as u64 % (1 << DEFAULT_BITS_GENERATOR);

        Self {
            radix: 10,
            time_base: DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
                .expect("Failed to parse time base")
                .with_timezone(&Utc),
            machine_id: 0,
            machine_bits: DEFAULT_BITS_MACHINE,
            generator_id,
            generator_bits: DEFAULT_BITS_GENERATOR,
            sequence_bits: DEFAULT_BITS_SEQUENCE,
            state: Arc::new(Mutex::new(State {
                sequence: 0,
                time_prev: 0,
            })),
        }
    }

    /// Sets the radix (base) for the generated ID. The radix determines the numeric base (2-36).
    /// For example, radix `16` generates IDs in hexadecimal.
    ///
    /// # Examples
    ///
    /// ```
    /// let maylily = Maylily::new().radix(16); // Set radix to hexadecimal
    /// ```
    pub fn radix(mut self, radix: u32) -> Self {
        self.radix = radix;
        self
    }

    /// Sets the base time for ID generation using `DateTime<Utc>`.
    /// By default, the time base is set to `2000-01-01T00:00:00Z`.
    /// This time serves as the reference point for the ID generation timestamps.
    ///
    /// # Examples
    ///
    /// ```
    /// use chrono::{Utc, TimeZone};
    /// let maylily = Maylily::new()
    ///     .time_base(Utc.ymd(2020, 1, 1).and_hms(0, 0, 0)); // Set time base to the year 2020
    /// ```
    pub fn time_base(mut self, time_base: DateTime<Utc>) -> Self {
        self.time_base = time_base;
        self
    }

    /// Sets the machine ID, which must be unique across different instances of the generator.
    /// This ensures that multiple instances running on different machines do not generate the same ID.
    ///
    /// # Examples
    ///
    /// ```
    /// let maylily = Maylily::new().machine_id(1); // Set machine ID to 1
    /// ```
    pub fn machine_id(mut self, machine_id: u64) -> Self {
        self.machine_id = machine_id;
        self
    }

    /// Sets the number of bits used to represent the machine ID.
    /// This defines the maximum number of machines the system can support.
    ///
    /// # Examples
    ///
    /// ```
    /// let maylily = Maylily::new().machine_bits(4); // Set 4 bits for machine ID
    /// ```
    pub fn machine_bits(mut self, machine_bits: u8) -> Self {
        self.machine_bits = machine_bits;
        self
    }

    /// Sets the generator ID, which must be unique on the same machine.
    /// This allows multiple generators on the same machine to create distinct IDs.
    ///
    /// # Examples
    ///
    /// ```
    /// let maylily = Maylily::new().generator_id(5); // Set generator ID to 5
    /// ```
    pub fn generator_id(mut self, generator_id: u64) -> Self {
        self.generator_id = generator_id;
        self
    }

    /// Sets the number of bits used to represent the generator ID.
    /// This controls how many different generator instances can be supported per machine.
    ///
    /// # Examples
    ///
    /// ```
    /// let maylily = Maylily::new().generator_bits(8); // Set 8 bits for generator ID
    /// ```
    pub fn generator_bits(mut self, generator_bits: u8) -> Self {
        self.generator_bits = generator_bits;
        self
    }

    /// Sets the number of bits used to represent the sequence number.
    /// This limits how many IDs can be generated per millisecond.
    ///
    /// # Examples
    ///
    /// ```
    /// let maylily = Maylily::new().sequence_bits(10); // Allow up to 1024 IDs per millisecond
    /// ```
    pub fn sequence_bits(mut self, sequence_bits: u8) -> Self {
        self.sequence_bits = sequence_bits;
        self
    }

    /// Generates a unique ID asynchronously. The ID generation ensures that no two IDs are
    /// created with the same timestamp, machine ID, generator ID, and sequence number.
    ///
    /// # Examples
    ///
    /// ```
    /// #[tokio::main]
    /// async fn main() {
    ///     let maylily = Maylily::new();
    ///     let id = maylily.generate().await.unwrap();
    ///     println!("Generated ID: {}", id);
    /// }
    /// ```
    pub async fn generate(&self) -> Result<String, String> {
        let mut state = self.state.lock().await;
        loop {
            let current_time = Utc::now().timestamp_millis() as u64;

            debug!("Current time: {}", current_time);
            debug!("Previous time: {}", state.time_prev);

            if current_time < state.time_prev {
                error!(
                    "Clock moved backwards. Refusing to generate id for {} milliseconds",
                    state.time_prev - current_time
                );
                return Err(format!(
                    "Clock moved backwards. Refusing to generate id for {} milliseconds",
                    state.time_prev - current_time
                ));
            }

            if current_time > state.time_prev {
                state.sequence = 0;
            }

            if state.sequence < (1 << self.sequence_bits) {
                let id = self.build_id(current_time, state.sequence);
                state.sequence += 1;
                state.time_prev = current_time;
                debug!("Generated ID: {}", id);
                return Ok(id);
            }

            sleep(Duration::from_millis(1)).await;
        }
    }

    /// Builds the unique ID using the current time, machine ID, generator ID, and sequence number.
    fn build_id(&self, time: u64, sequence: u64) -> String {
        let time_diff = time - self.time_base.timestamp_millis() as u64;

        let id = ((((((time_diff) << self.machine_bits) + self.machine_id)
            << self.generator_bits)
            + self.generator_id)
            << self.sequence_bits)
            + sequence;

        let id_biguint = BigUint::from(id);

        id_biguint.to_str_radix(self.radix as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_generate_default() {
        let runtime = Runtime::new().unwrap();
        let maylily = Maylily::new();
        let id = runtime.block_on(maylily.generate()).unwrap();
        assert!(id.len() > 0);
    }

    #[test]
    fn test_custom_machine_id() {
        let runtime = Runtime::new().unwrap();
        let maylily = Maylily::new().machine_id(5);
        let id = runtime.block_on(maylily.generate()).unwrap();
        assert!(id.len() > 0);
    }

    #[test]
    fn test_radix_base_16() {
        let runtime = Runtime::new().unwrap();
        let maylily = Maylily::new().radix(16);
        let id = runtime.block_on(maylily.generate()).unwrap();
        assert!(id.len() > 0);
    }
}
