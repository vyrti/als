use als_compression::{AlsCompressor, AlsError, AlsParser, CompressorConfig};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, error, info, warn};
use std::fs;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::time::Instant;

/// ALS (Adaptive Logic Stream) compression tool for structured data
#[derive(Parser)]
#[command(name = "als")]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Enable verbose output
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Suppress all non-error output
    #[arg(short, long, global = true, conflicts_with = "verbose")]
    quiet: bool,

    /// Configuration file path (TOML or JSON)
    #[arg(short, long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

/// Supported input/output formats
#[derive(Debug, Clone, Copy, ValueEnum)]
enum Format {
    /// CSV (Comma-Separated Values)
    Csv,
    /// JSON (JavaScript Object Notation)
    Json,
    /// ALS (Adaptive Logic Stream)
    Als,
    /// Auto-detect format from file extension or content
    Auto,
}

impl Format {
    fn as_str(&self) -> &'static str {
        match self {
            Format::Csv => "csv",
            Format::Json => "json",
            Format::Als => "als",
            Format::Auto => "auto",
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Compress CSV or JSON data to ALS format
    Compress {
        /// Input file (use '-' for stdin)
        #[arg(short, long, value_name = "FILE", default_value = "-")]
        input: String,

        /// Output file (use '-' for stdout)
        #[arg(short, long, value_name = "FILE", default_value = "-")]
        output: String,

        /// Input format: csv, json, or auto-detect
        #[arg(short, long, value_enum, default_value = "auto")]
        format: Format,
    },

    /// Decompress ALS data to CSV or JSON format
    Decompress {
        /// Input file (use '-' for stdin)
        #[arg(short, long, value_name = "FILE", default_value = "-")]
        input: String,

        /// Output file (use '-' for stdout)
        #[arg(short, long, value_name = "FILE", default_value = "-")]
        output: String,

        /// Output format: csv or json
        #[arg(short, long, value_enum, default_value = "csv")]
        format: Format,
    },

    /// Display information about ALS compressed data
    Info {
        /// Input file (use '-' for stdin)
        #[arg(short, long, value_name = "FILE", default_value = "-")]
        input: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Set up logging based on verbosity flags
    setup_logging(cli.verbose, cli.quiet);

    // Load configuration if specified
    let config = if let Some(config_path) = &cli.config {
        load_config(config_path)?
    } else {
        CompressorConfig::default()
    };

    // Execute the appropriate command
    match cli.command {
        Commands::Compress {
            input,
            output,
            format,
        } => {
            compress_command(&input, &output, format, config, cli.verbose, cli.quiet)?;
        }
        Commands::Decompress {
            input,
            output,
            format,
        } => {
            decompress_command(&input, &output, format, cli.verbose, cli.quiet)?;
        }
        Commands::Info { input } => {
            info_command(&input, cli.verbose, cli.quiet)?;
        }
    }

    Ok(())
}

/// Set up logging based on verbosity flags
fn setup_logging(verbose: bool, quiet: bool) {
    let log_level = if quiet {
        "error"
    } else if verbose {
        "debug"
    } else {
        "info"
    };

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level))
        .format_timestamp(None)
        .format_module_path(false)
        .format_target(false)
        .init();

    debug!("Logging initialized at {} level", log_level);
}

/// Load configuration from a file
fn load_config(_path: &PathBuf) -> Result<CompressorConfig> {
    // For now, return default config
    // TODO: Implement actual config file loading in task 35.6
    Ok(CompressorConfig::default())
}

/// Read input from file or stdin
fn read_input(input: &str) -> Result<String> {
    if input == "-" {
        // Read from stdin
        let mut buffer = String::new();
        io::stdin()
            .read_to_string(&mut buffer)
            .context("Failed to read from stdin")?;
        Ok(buffer)
    } else {
        // Read from file
        fs::read_to_string(input).with_context(|| format!("Failed to read input file: {}", input))
    }
}

/// Write output to file or stdout
fn write_output(output: &str, content: &str) -> Result<()> {
    if output == "-" {
        // Write to stdout
        io::stdout()
            .write_all(content.as_bytes())
            .context("Failed to write to stdout")?;
        io::stdout().flush().context("Failed to flush stdout")?;
    } else {
        // Write to file
        fs::write(output, content)
            .with_context(|| format!("Failed to write output file: {}", output))?;
    }
    Ok(())
}

/// Detect input format from content or file extension
fn detect_format(input: &str, content: &str) -> Format {
    // First try to detect from file extension
    if input != "-" {
        if input.ends_with(".csv") {
            return Format::Csv;
        } else if input.ends_with(".json") {
            return Format::Json;
        } else if input.ends_with(".als") {
            return Format::Als;
        }
    }

    // Try to detect from content
    let trimmed = content.trim_start();

    // JSON typically starts with [ or {
    if trimmed.starts_with('[') || trimmed.starts_with('{') {
        return Format::Json;
    }

    // ALS format starts with version (!v) or schema (#)
    if trimmed.starts_with("!v") || trimmed.starts_with('#') || trimmed.starts_with('$') {
        return Format::Als;
    }

    // Default to CSV
    Format::Csv
}

/// Execute the compress command
fn compress_command(
    input: &str,
    output: &str,
    format: Format,
    config: CompressorConfig,
    _verbose: bool,
    quiet: bool,
) -> Result<()> {
    let start_time = Instant::now();

    info!("Starting compression: {} -> {}", input, output);

    // Read input with progress bar for large files
    let progress = create_progress_bar(quiet, "Reading input");
    let input_data = read_input(input)?;
    progress.finish_and_clear();

    if input_data.is_empty() {
        warn!("Input is empty");
        write_output(output, "")?;
        return Ok(());
    }

    let input_size = input_data.len();
    debug!("Read {} bytes from input", input_size);

    // Detect format if auto
    let detected_format = match format {
        Format::Auto => {
            let detected = detect_format(input, &input_data);
            info!("Auto-detected format: {}", detected.as_str());
            detected
        }
        _ => format,
    };

    debug!("Input format: {}", detected_format.as_str());

    // Create compressor
    let compressor = AlsCompressor::with_config(config);

    // Compress based on format with progress indication
    let progress = create_progress_bar(quiet, "Compressing");
    let compress_start = Instant::now();

    let compressed = match detected_format {
        Format::Csv => {
            debug!("Compressing CSV data");
            compressor
                .compress_csv(&input_data)
                .map_err(|e| map_als_error(e, "CSV compression"))?
        }
        Format::Json => {
            debug!("Compressing JSON data");
            compressor
                .compress_json(&input_data)
                .map_err(|e| map_als_error(e, "JSON compression"))?
        }
        Format::Als => {
            error!("Input is already in ALS format");
            anyhow::bail!("Input is already in ALS format. Use 'decompress' command instead.");
        }
        Format::Auto => {
            error!("Failed to detect input format");
            anyhow::bail!("Failed to detect input format");
        }
    };

    let compress_duration = compress_start.elapsed();
    progress.finish_and_clear();

    let output_size = compressed.len();
    let ratio = input_size as f64 / output_size as f64;
    let throughput = (input_size as f64 / 1_048_576.0) / compress_duration.as_secs_f64();

    debug!("Compressed {} bytes to {} bytes", input_size, output_size);
    debug!("Compression ratio: {:.2}x", ratio);
    debug!("Compression time: {:.3}s", compress_duration.as_secs_f64());
    debug!("Throughput: {:.2} MB/s", throughput);

    // Write output
    let progress = create_progress_bar(quiet, "Writing output");
    write_output(output, &compressed)?;
    progress.finish_and_clear();

    let total_duration = start_time.elapsed();

    // Display summary
    if !quiet {
        let savings = ((1.0 - (output_size as f64 / input_size as f64)) * 100.0).max(0.0);
        eprintln!("✓ Compression complete");
        eprintln!("  Input:       {}", format_bytes(input_size));
        eprintln!("  Output:      {}", format_bytes(output_size));
        eprintln!("  Ratio:       {:.2}x", ratio);
        eprintln!("  Savings:     {:.1}%", savings);
        eprintln!("  Time:        {:.3}s", total_duration.as_secs_f64());
        eprintln!("  Throughput:  {:.2} MB/s", throughput);
    }

    info!(
        "Compression completed in {:.3}s",
        total_duration.as_secs_f64()
    );

    Ok(())
}

/// Execute the decompress command
fn decompress_command(
    input: &str,
    output: &str,
    format: Format,
    _verbose: bool,
    quiet: bool,
) -> Result<()> {
    let start_time = Instant::now();

    info!("Starting decompression: {} -> {}", input, output);
    debug!("Output format: {}", format.as_str());

    // Read ALS input with progress bar
    let progress = create_progress_bar(quiet, "Reading input");
    let als_data = read_input(input)?;
    progress.finish_and_clear();

    if als_data.is_empty() {
        warn!("Input is empty");
        write_output(output, "")?;
        return Ok(());
    }

    let input_size = als_data.len();
    debug!("Read {} bytes from input", input_size);

    // Validate that format is CSV or JSON (not ALS or Auto)
    let output_format = match format {
        Format::Csv => Format::Csv,
        Format::Json => Format::Json,
        Format::Als => {
            error!("Cannot decompress to ALS format");
            anyhow::bail!("Cannot decompress to ALS format. Use 'csv' or 'json' as output format.");
        }
        Format::Auto => {
            // Default to CSV for auto-detection
            info!("Auto-detecting output format: defaulting to CSV");
            Format::Csv
        }
    };

    // Create parser
    let parser = AlsParser::new();

    // Decompress based on output format with progress indication
    let progress = create_progress_bar(quiet, "Decompressing");
    let decompress_start = Instant::now();

    let decompressed = match output_format {
        Format::Csv => {
            debug!("Decompressing to CSV");
            parser
                .to_csv(&als_data)
                .map_err(|e| map_als_error(e, "ALS decompression to CSV"))?
        }
        Format::Json => {
            debug!("Decompressing to JSON");
            parser
                .to_json(&als_data)
                .map_err(|e| map_als_error(e, "ALS decompression to JSON"))?
        }
        _ => unreachable!("Output format should be CSV or JSON at this point"),
    };

    let decompress_duration = decompress_start.elapsed();
    progress.finish_and_clear();

    let output_size = decompressed.len();
    let expansion_ratio = output_size as f64 / input_size as f64;
    let throughput = (output_size as f64 / 1_048_576.0) / decompress_duration.as_secs_f64();

    debug!("Decompressed {} bytes to {} bytes", input_size, output_size);
    debug!("Expansion ratio: {:.2}x", expansion_ratio);
    debug!(
        "Decompression time: {:.3}s",
        decompress_duration.as_secs_f64()
    );
    debug!("Throughput: {:.2} MB/s", throughput);

    // Write output
    let progress = create_progress_bar(quiet, "Writing output");
    write_output(output, &decompressed)?;
    progress.finish_and_clear();

    let total_duration = start_time.elapsed();

    // Display summary
    if !quiet {
        eprintln!("✓ Decompression complete");
        eprintln!("  Input:       {}", format_bytes(input_size));
        eprintln!("  Output:      {}", format_bytes(output_size));
        eprintln!("  Expansion:   {:.2}x", expansion_ratio);
        eprintln!("  Time:        {:.3}s", total_duration.as_secs_f64());
        eprintln!("  Throughput:  {:.2} MB/s", throughput);
    }

    info!(
        "Decompression completed in {:.3}s",
        total_duration.as_secs_f64()
    );

    Ok(())
}

/// Execute the info command
fn info_command(input: &str, verbose: bool, quiet: bool) -> Result<()> {
    let start_time = Instant::now();

    info!("Reading ALS document info from {}", input);

    // Read ALS input with progress bar
    let progress = create_progress_bar(quiet, "Reading input");
    let als_data = read_input(input)?;
    progress.finish_and_clear();

    if als_data.is_empty() {
        warn!("Input is empty");
        return Ok(());
    }

    debug!("Read {} bytes from input", als_data.len());

    // Parse the ALS document
    let progress = create_progress_bar(quiet, "Parsing ALS");
    let parser = AlsParser::new();
    let parse_start = Instant::now();

    let doc = parser
        .parse(&als_data)
        .map_err(|e| map_als_error(e, "ALS parsing"))?;

    let parse_duration = parse_start.elapsed();
    progress.finish_and_clear();

    debug!(
        "Parsed ALS document in {:.3}s",
        parse_duration.as_secs_f64()
    );

    // Display document information
    if !quiet {
        display_document_info(&doc, &als_data, verbose);
    }

    let total_duration = start_time.elapsed();
    debug!(
        "Info command completed in {:.3}s",
        total_duration.as_secs_f64()
    );

    Ok(())
}

/// Display information about an ALS document
fn display_document_info(doc: &als_compression::AlsDocument, als_data: &str, verbose: bool) {
    use als_compression::FormatIndicator;

    println!("=== ALS Document Information ===\n");

    // Document metadata
    println!(
        "Format: {}",
        match doc.format_indicator {
            FormatIndicator::Als => "ALS (Adaptive Logic Stream)",
            FormatIndicator::Ctx => "CTX (Columnar Text - Fallback)",
        }
    );
    println!("Version: {}", doc.version);
    println!("Columns: {}", doc.column_count());
    println!("Rows: {}", doc.row_count());
    println!("Compressed size: {} bytes", als_data.len());

    // Calculate estimated uncompressed size
    let estimated_uncompressed = estimate_uncompressed_size(doc);
    if estimated_uncompressed > 0 {
        let ratio = estimated_uncompressed as f64 / als_data.len() as f64;
        println!(
            "Estimated uncompressed size: {} bytes",
            estimated_uncompressed
        );
        println!("Compression ratio: {:.2}x", ratio);
        let savings =
            ((1.0 - (als_data.len() as f64 / estimated_uncompressed as f64)) * 100.0).max(0.0);
        println!("Space savings: {:.1}%", savings);
    }

    // Schema information
    if !doc.schema.is_empty() {
        println!("\n--- Schema ---");
        for (i, col_name) in doc.schema.iter().enumerate() {
            println!("  {}: {}", i + 1, col_name);
        }
    }

    // Dictionary information
    if !doc.dictionaries.is_empty() {
        println!("\n--- Dictionaries ---");
        for (dict_name, entries) in &doc.dictionaries {
            println!("  {}: {} entries", dict_name, entries.len());
            if verbose {
                for (i, entry) in entries.iter().enumerate() {
                    let display_entry = if entry.len() > 50 {
                        format!("{}...", &entry[..47])
                    } else {
                        entry.clone()
                    };
                    println!("    [{}]: {}", i, display_entry);
                }
            }
        }
    }

    // Pattern statistics
    println!("\n--- Compression Patterns ---");
    let pattern_stats = analyze_patterns(doc);

    if pattern_stats.ranges > 0 {
        println!(
            "  Ranges: {} (sequential/arithmetic sequences)",
            pattern_stats.ranges
        );
    }
    if pattern_stats.multipliers > 0 {
        println!(
            "  Multipliers: {} (repeated values)",
            pattern_stats.multipliers
        );
    }
    if pattern_stats.toggles > 0 {
        println!(
            "  Toggles: {} (alternating patterns)",
            pattern_stats.toggles
        );
    }
    if pattern_stats.dict_refs > 0 {
        println!("  Dictionary references: {}", pattern_stats.dict_refs);
    }
    if pattern_stats.raw_values > 0 {
        println!(
            "  Raw values: {} (no compression)",
            pattern_stats.raw_values
        );
    }

    let total_operators = pattern_stats.ranges
        + pattern_stats.multipliers
        + pattern_stats.toggles
        + pattern_stats.dict_refs
        + pattern_stats.raw_values;
    if total_operators > 0 {
        let compressed_ops = pattern_stats.ranges
            + pattern_stats.multipliers
            + pattern_stats.toggles
            + pattern_stats.dict_refs;
        let compression_effectiveness = (compressed_ops as f64 / total_operators as f64) * 100.0;
        println!(
            "  Compression effectiveness: {:.1}% of operators use compression",
            compression_effectiveness
        );
    }

    // Per-column information (verbose mode)
    if verbose && !doc.streams.is_empty() {
        println!("\n--- Per-Column Details ---");
        for (i, (col_name, stream)) in doc.schema.iter().zip(doc.streams.iter()).enumerate() {
            let col_stats = analyze_column_stream(stream);
            println!("  Column {}: {}", i + 1, col_name);
            println!("    Operators: {}", stream.operator_count());
            println!("    Expanded values: {}", stream.expanded_count());
            if col_stats.ranges > 0 {
                println!("    - Ranges: {}", col_stats.ranges);
            }
            if col_stats.multipliers > 0 {
                println!("    - Multipliers: {}", col_stats.multipliers);
            }
            if col_stats.toggles > 0 {
                println!("    - Toggles: {}", col_stats.toggles);
            }
            if col_stats.dict_refs > 0 {
                println!("    - Dictionary refs: {}", col_stats.dict_refs);
            }
            if col_stats.raw_values > 0 {
                println!("    - Raw values: {}", col_stats.raw_values);
            }
        }
    }

    println!();
}

/// Pattern statistics for a document or column
#[derive(Debug, Default)]
struct PatternStats {
    ranges: usize,
    multipliers: usize,
    toggles: usize,
    dict_refs: usize,
    raw_values: usize,
}

/// Analyze patterns used in the entire document
fn analyze_patterns(doc: &als_compression::AlsDocument) -> PatternStats {
    let mut stats = PatternStats::default();

    for stream in &doc.streams {
        for op in &stream.operators {
            count_operator_patterns(op, &mut stats);
        }
    }

    stats
}

/// Analyze patterns used in a single column stream
fn analyze_column_stream(stream: &als_compression::ColumnStream) -> PatternStats {
    let mut stats = PatternStats::default();

    for op in &stream.operators {
        count_operator_patterns(op, &mut stats);
    }

    stats
}

/// Count patterns in an operator (recursively for nested operators)
fn count_operator_patterns(op: &als_compression::AlsOperator, stats: &mut PatternStats) {
    use als_compression::AlsOperator;

    match op {
        AlsOperator::Range { .. } => stats.ranges += 1,
        AlsOperator::Multiply { value, .. } => {
            stats.multipliers += 1;
            // Count nested operator
            count_operator_patterns(value, stats);
        }
        AlsOperator::Toggle { .. } => stats.toggles += 1,
        AlsOperator::DictRef(_) => stats.dict_refs += 1,
        AlsOperator::Raw(_) => stats.raw_values += 1,
    }
}

/// Estimate the uncompressed size of the document
fn estimate_uncompressed_size(doc: &als_compression::AlsDocument) -> usize {
    let row_count = doc.row_count();
    if row_count == 0 {
        return 0;
    }

    // Estimate based on expanded values
    // Assume average value length of 10 characters + 1 for delimiter
    let estimated_value_size = 11;
    let total_values = row_count * doc.column_count();

    // Add schema overhead (column names + delimiters)
    let schema_size: usize = doc.schema.iter().map(|s| s.len() + 1).sum();

    schema_size + (total_values * estimated_value_size)
}

/// Create a progress bar (spinner) for operations
fn create_progress_bar(quiet: bool, message: &str) -> ProgressBar {
    if quiet {
        // Return a hidden progress bar in quiet mode
        ProgressBar::hidden()
    } else {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        pb.set_message(message.to_string());
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        pb
    }
}

/// Format bytes in human-readable format
fn format_bytes(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];

    if bytes == 0 {
        return "0 B".to_string();
    }

    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[0])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}

/// Map AlsError to anyhow::Error with context
fn map_als_error(error: AlsError, context: &str) -> anyhow::Error {
    match error {
        AlsError::CsvParseError {
            line,
            column,
            message,
        } => {
            anyhow::anyhow!(
                "{}: CSV parse error at line {}, column {}: {}",
                context,
                line,
                column,
                message
            )
        }
        AlsError::LogParseError { line, message } => {
            anyhow::anyhow!("{}: Log parse error at line {}: {}", context, line, message)
        }
        AlsError::JsonParseError(e) => {
            anyhow::anyhow!("{}: JSON parse error: {}", context, e)
        }
        AlsError::AlsSyntaxError { position, message } => {
            anyhow::anyhow!(
                "{}: ALS syntax error at position {}: {}",
                context,
                position,
                message
            )
        }
        AlsError::InvalidDictRef { index, size } => {
            anyhow::anyhow!(
                "{}: Invalid dictionary reference _{} (dictionary has {} entries)",
                context,
                index,
                size
            )
        }
        AlsError::RangeOverflow { start, end, step } => {
            anyhow::anyhow!(
                "{}: Range overflow: {} to {} with step {} would produce too many values",
                context,
                start,
                end,
                step
            )
        }
        AlsError::VersionMismatch { expected, found } => {
            anyhow::anyhow!(
                "{}: Version mismatch: expected <= {}, found {}",
                context,
                expected,
                found
            )
        }
        AlsError::ColumnMismatch { schema, data } => {
            anyhow::anyhow!(
                "{}: Column count mismatch: schema has {} columns, data has {} columns",
                context,
                schema,
                data
            )
        }
        AlsError::IoError(e) => {
            anyhow::anyhow!("{}: IO error: {}", context, e)
        }
    }
}
