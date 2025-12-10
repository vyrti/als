//! Syslog/Linux log parsing and conversion.
//!
//! This module provides functions for parsing syslog-format log files
//! (like linux.log) into `TabularData` structures optimized for compression.
//!
//! ## Log Format
//!
//! Standard syslog format: `<Month> <Day> <Time> <Hostname> <Service>[<PID>]: <Message>`
//!
//! ## Compression Optimization
//!
//! The parser extracts structured fields to maximize compression:
//! - Timestamps are split into components for pattern detection
//! - Services and hostnames are extracted for dictionary encoding
//! - PIDs are extracted as integers for range compression
//! - Message templates are separated from variable parameters

use crate::convert::{Column, TabularData, Value};
use crate::error::{AlsError, Result};
use std::borrow::Cow;

/// Parsed syslog entry with all extracted fields.
#[derive(Debug, Clone)]
pub struct SyslogEntry<'a> {
    /// Month (e.g., "Jun")
    pub month: &'a str,
    /// Day of month (1-31)
    pub day: u8,
    /// Time string (HH:MM:SS)
    pub time: &'a str,
    /// Hostname
    pub hostname: &'a str,
    /// Service/program name
    pub service: &'a str,
    /// Process ID (if present)
    pub pid: Option<u32>,
    /// Log message
    pub message: &'a str,
    /// Message type (extracted template)
    pub message_type: MessageType,
    /// Extracted parameters from message
    pub params: MessageParams<'a>,
}

/// Categorized message types for better compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageType {
    /// SSH authentication failure
    AuthFailure,
    /// SSH check pass (user unknown)
    CheckPass,
    /// Session opened
    SessionOpened,
    /// Session closed
    SessionClosed,
    /// FTP connection
    FtpConnection,
    /// FTP timeout
    FtpTimeout,
    /// System service status (startup/shutdown)
    ServiceStatus,
    /// Log rotation
    LogRotate,
    /// Syslog restart
    SyslogRestart,
    /// SNMP packet received
    SnmpPacket,
    /// Kerberos authentication
    KerberosAuth,
    /// Unknown/other message type
    Other,
}

impl MessageType {
    /// Get a short string representation for compression.
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageType::AuthFailure => "auth_fail",
            MessageType::CheckPass => "check_pass",
            MessageType::SessionOpened => "sess_open",
            MessageType::SessionClosed => "sess_close",
            MessageType::FtpConnection => "ftp_conn",
            MessageType::FtpTimeout => "ftp_timeout",
            MessageType::ServiceStatus => "svc_status",
            MessageType::LogRotate => "logrotate",
            MessageType::SyslogRestart => "syslog_restart",
            MessageType::SnmpPacket => "snmp",
            MessageType::KerberosAuth => "kerberos",
            MessageType::Other => "other",
        }
    }
}

/// Extracted parameters from log messages.
#[derive(Debug, Clone, Default)]
pub struct MessageParams<'a> {
    /// Remote host (IP or hostname)
    pub rhost: Option<&'a str>,
    /// Username
    pub user: Option<&'a str>,
    /// UID
    pub uid: Option<u32>,
    /// Effective UID
    pub euid: Option<u32>,
    /// TTY
    pub tty: Option<&'a str>,
    /// Remote user
    pub ruser: Option<&'a str>,
    /// IP address
    pub ip: Option<&'a str>,
    /// Resolved hostname
    pub resolved_host: Option<&'a str>,
    /// Exit code
    pub exit_code: Option<i32>,
    /// Service name (for status messages)
    pub service_name: Option<&'a str>,
    /// Status (succeeded/failed)
    pub status: Option<&'a str>,
    /// Raw message (for Other type)
    pub raw: Option<&'a str>,
}

/// Parse a syslog-format log file into TabularData.
///
/// This function parses each line and extracts structured fields
/// optimized for ALS compression.
///
/// # Arguments
///
/// * `input` - The log file content
///
/// # Returns
///
/// A `TabularData` structure with columns optimized for compression.
///
/// # Example
///
/// ```ignore
/// use als_compression::convert::syslog::parse_syslog;
///
/// let log = "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=218.188.2.4";
/// let data = parse_syslog(log).unwrap();
/// assert!(data.column_count() > 0);
/// ```
pub fn parse_syslog(input: &str) -> Result<TabularData<'static>> {
    if input.trim().is_empty() {
        return Ok(TabularData::new());
    }

    let lines: Vec<&str> = input.lines().collect();
    let line_count = lines.len();

    // Pre-allocate vectors for each column
    let mut months: Vec<Value<'static>> = Vec::with_capacity(line_count);
    let mut days: Vec<Value<'static>> = Vec::with_capacity(line_count);
    let mut times: Vec<Value<'static>> = Vec::with_capacity(line_count);
    let mut hostnames: Vec<Value<'static>> = Vec::with_capacity(line_count);
    let mut services: Vec<Value<'static>> = Vec::with_capacity(line_count);
    let mut pids: Vec<Value<'static>> = Vec::with_capacity(line_count);
    let mut msg_types: Vec<Value<'static>> = Vec::with_capacity(line_count);
    let mut rhosts: Vec<Value<'static>> = Vec::with_capacity(line_count);
    let mut users: Vec<Value<'static>> = Vec::with_capacity(line_count);
    let mut raw_msgs: Vec<Value<'static>> = Vec::with_capacity(line_count);

    for (_line_num, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        match parse_syslog_line(trimmed) {
            Ok(entry) => {
                months.push(Value::String(Cow::Owned(entry.month.to_string())));
                days.push(Value::Integer(entry.day as i64));
                times.push(Value::String(Cow::Owned(entry.time.to_string())));
                hostnames.push(Value::String(Cow::Owned(entry.hostname.to_string())));
                services.push(Value::String(Cow::Owned(entry.service.to_string())));
                pids.push(entry.pid.map(|p| Value::Integer(p as i64)).unwrap_or(Value::Null));
                msg_types.push(Value::String(Cow::Owned(entry.message_type.as_str().to_string())));
                rhosts.push(entry.params.rhost
                    .map(|h| Value::String(Cow::Owned(h.to_string())))
                    .unwrap_or(Value::Null));
                users.push(entry.params.user
                    .map(|u| Value::String(Cow::Owned(u.to_string())))
                    .unwrap_or(Value::Null));
                raw_msgs.push(Value::String(Cow::Owned(entry.message.to_string())));
            }
            Err(_) => {
                // For unparseable lines, store as raw with nulls for structured fields
                months.push(Value::Null);
                days.push(Value::Null);
                times.push(Value::Null);
                hostnames.push(Value::Null);
                services.push(Value::Null);
                pids.push(Value::Null);
                msg_types.push(Value::String(Cow::Owned("parse_error".to_string())));
                rhosts.push(Value::Null);
                users.push(Value::Null);
                raw_msgs.push(Value::String(Cow::Owned(trimmed.to_string())));
            }
        }
    }

    // Skip empty results
    if months.is_empty() {
        return Ok(TabularData::new());
    }

    let mut data = TabularData::with_capacity(10);
    data.add_column(Column::new(Cow::Borrowed("month"), months));
    data.add_column(Column::new(Cow::Borrowed("day"), days));
    data.add_column(Column::new(Cow::Borrowed("time"), times));
    data.add_column(Column::new(Cow::Borrowed("hostname"), hostnames));
    data.add_column(Column::new(Cow::Borrowed("service"), services));
    data.add_column(Column::new(Cow::Borrowed("pid"), pids));
    data.add_column(Column::new(Cow::Borrowed("msg_type"), msg_types));
    data.add_column(Column::new(Cow::Borrowed("rhost"), rhosts));
    data.add_column(Column::new(Cow::Borrowed("user"), users));
    data.add_column(Column::new(Cow::Borrowed("message"), raw_msgs));

    Ok(data)
}

/// Parse a single syslog line.
fn parse_syslog_line(line: &str) -> Result<SyslogEntry<'_>> {
    // Format: "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: message"
    // Note: Single-digit days may have extra space: "Jul  1 ..."
    
    // Split by whitespace to handle variable spacing
    let parts: Vec<&str> = line.split_whitespace().collect();
    
    if parts.len() < 5 {
        return Err(AlsError::CsvParseError {
            line: 0,
            column: 0,
            message: format!("Invalid syslog format: not enough fields"),
        });
    }

    let month = parts[0];
    let day: u8 = parts[1].parse().map_err(|_| AlsError::CsvParseError {
        line: 0,
        column: 0,
        message: format!("Invalid day: {}", parts[1]),
    })?;
    let time = parts[2];
    let hostname = parts[3];
    
    // Reconstruct the rest of the line (service[pid]: message)
    // Find where the hostname ends in the original line
    let hostname_end = find_nth_field_end(line, 4);
    let rest = line[hostname_end..].trim_start();
    
    let (service, pid, message) = parse_service_and_message(rest)?;
    
    // Classify message and extract parameters
    let (message_type, params) = classify_message(message);

    Ok(SyslogEntry {
        month,
        day,
        time,
        hostname,
        service,
        pid,
        message,
        message_type,
        params,
    })
}

/// Find the end position of the nth whitespace-separated field.
fn find_nth_field_end(s: &str, n: usize) -> usize {
    let mut field_count = 0;
    let mut in_field = false;
    
    for (i, c) in s.char_indices() {
        if c.is_whitespace() {
            if in_field {
                field_count += 1;
                if field_count == n {
                    return i;
                }
                in_field = false;
            }
        } else {
            in_field = true;
        }
    }
    
    s.len()
}

/// Parse the service[pid]: message portion.
fn parse_service_and_message(input: &str) -> Result<(&str, Option<u32>, &str)> {
    // Find the colon that separates service from message
    let colon_pos = input.find(':').ok_or_else(|| AlsError::CsvParseError {
        line: 0,
        column: 0,
        message: "No colon found in service/message".to_string(),
    })?;

    let service_part = &input[..colon_pos];
    let message = input[colon_pos + 1..].trim_start();

    // Parse service[pid] or just service
    if let Some(bracket_start) = service_part.find('[') {
        if let Some(bracket_end) = service_part.find(']') {
            let service = &service_part[..bracket_start];
            let pid_str = &service_part[bracket_start + 1..bracket_end];
            let pid = pid_str.parse().ok();
            return Ok((service, pid, message));
        }
    }

    Ok((service_part, None, message))
}

/// Classify message type and extract parameters.
fn classify_message(message: &str) -> (MessageType, MessageParams<'_>) {
    let mut params = MessageParams::default();

    // Authentication failure
    if message.starts_with("authentication failure") {
        params.rhost = extract_param(message, "rhost=");
        params.user = extract_param(message, "user=");
        params.uid = extract_param(message, "uid=").and_then(|s| s.parse().ok());
        params.euid = extract_param(message, "euid=").and_then(|s| s.parse().ok());
        params.tty = extract_param(message, "tty=");
        params.ruser = extract_param(message, "ruser=");
        return (MessageType::AuthFailure, params);
    }

    // Check pass
    if message.starts_with("check pass") {
        return (MessageType::CheckPass, params);
    }

    // Session opened
    if message.starts_with("session opened") {
        params.user = extract_session_user(message);
        return (MessageType::SessionOpened, params);
    }

    // Session closed
    if message.starts_with("session closed") {
        params.user = extract_session_user(message);
        return (MessageType::SessionClosed, params);
    }

    // FTP connection
    if message.starts_with("connection from") {
        let (ip, host) = extract_ftp_connection(message);
        params.ip = ip;
        params.resolved_host = host;
        return (MessageType::FtpConnection, params);
    }

    // FTP timeout
    if message.contains("timed out") {
        return (MessageType::FtpTimeout, params);
    }

    // Service status (startup/shutdown)
    if message.contains("startup succeeded") || message.contains("shutdown succeeded") {
        params.status = if message.contains("startup") { Some("startup") } else { Some("shutdown") };
        return (MessageType::ServiceStatus, params);
    }

    // Log rotation
    if message.starts_with("ALERT exited abnormally") {
        params.exit_code = extract_exit_code(message);
        return (MessageType::LogRotate, params);
    }

    // Syslog restart
    if message.contains("restart") {
        return (MessageType::SyslogRestart, params);
    }

    // SNMP
    if message.starts_with("Received SNMP") {
        params.ip = extract_snmp_source(message);
        return (MessageType::SnmpPacket, params);
    }

    // Kerberos
    if message.contains("Kerberos") || message.contains("Authentication failed from") {
        params.ip = extract_kerberos_source(message);
        return (MessageType::KerberosAuth, params);
    }

    // Unknown
    params.raw = Some(message);
    (MessageType::Other, params)
}

/// Extract a parameter value from a message.
/// Handles key=value pairs separated by spaces.
fn extract_param<'a>(message: &'a str, param: &str) -> Option<&'a str> {
    // Split by whitespace and find the param
    for part in message.split_whitespace() {
        if part.starts_with(param) {
            let value = &part[param.len()..];
            if value.is_empty() {
                return None;
            }
            return Some(value);
        }
    }
    
    // Fallback: try direct find for params that might have spaces before them
    if let Some(start) = message.find(param) {
        let value_start = start + param.len();
        let rest = &message[value_start..];
        let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
        let value = &rest[..end];
        if !value.is_empty() {
            return Some(value);
        }
    }
    
    None
}

/// Extract user from session messages.
fn extract_session_user(message: &str) -> Option<&str> {
    // "session opened for user cyrus by (uid=0)"
    let marker = "for user ";
    let start = message.find(marker)?;
    let value_start = start + marker.len();
    let rest = &message[value_start..];
    let end = rest.find(' ').unwrap_or(rest.len());
    Some(&rest[..end])
}

/// Extract IP and hostname from FTP connection message.
fn extract_ftp_connection(message: &str) -> (Option<&str>, Option<&str>) {
    // "connection from 24.54.76.216 (24-54-76-216.bflony.adelphia.net) at ..."
    let marker = "connection from ";
    if let Some(start) = message.find(marker) {
        let rest = &message[start + marker.len()..];
        
        // Find IP (ends at space or parenthesis)
        let ip_end = rest.find(|c| c == ' ' || c == '(').unwrap_or(rest.len());
        let ip = &rest[..ip_end];
        
        // Find hostname in parentheses
        if let Some(paren_start) = rest.find('(') {
            if let Some(paren_end) = rest.find(')') {
                let host = &rest[paren_start + 1..paren_end];
                return (Some(ip), if host.is_empty() { None } else { Some(host) });
            }
        }
        
        return (Some(ip), None);
    }
    (None, None)
}

/// Extract exit code from logrotate message.
fn extract_exit_code(message: &str) -> Option<i32> {
    // "ALERT exited abnormally with [1]"
    let start = message.find('[')?;
    let end = message.find(']')?;
    message[start + 1..end].parse().ok()
}

/// Extract source IP from SNMP message.
fn extract_snmp_source(message: &str) -> Option<&str> {
    // "Received SNMP packet(s) from 67.170.148.126"
    let marker = "from ";
    let start = message.find(marker)?;
    let rest = &message[start + marker.len()..];
    let end = rest.find(' ').unwrap_or(rest.len());
    Some(&rest[..end])
}

/// Extract source from Kerberos message.
fn extract_kerberos_source(message: &str) -> Option<&str> {
    // "Authentication failed from 163.27.187.39 (163.27.187.39): ..."
    let marker = "from ";
    let start = message.find(marker)?;
    let rest = &message[start + marker.len()..];
    let end = rest.find(|c| c == ' ' || c == '(').unwrap_or(rest.len());
    Some(&rest[..end])
}

/// Convert TabularData back to syslog format.
///
/// This function reconstructs log lines from the structured data.
pub fn to_syslog(data: &TabularData) -> Result<String> {
    if data.is_empty() || data.column_count() == 0 {
        return Ok(String::new());
    }

    let mut output = String::new();
    
    // Get column indices
    let month_idx = data.columns.iter().position(|c| c.name == "month");
    let day_idx = data.columns.iter().position(|c| c.name == "day");
    let time_idx = data.columns.iter().position(|c| c.name == "time");
    let hostname_idx = data.columns.iter().position(|c| c.name == "hostname");
    let service_idx = data.columns.iter().position(|c| c.name == "service");
    let pid_idx = data.columns.iter().position(|c| c.name == "pid");
    let message_idx = data.columns.iter().position(|c| c.name == "message");

    for row_idx in 0..data.row_count {
        let month = month_idx.and_then(|i| data.columns[i].values[row_idx].as_str()).unwrap_or("");
        let day = day_idx.and_then(|i| data.columns[i].values[row_idx].as_integer()).unwrap_or(0);
        let time = time_idx.and_then(|i| data.columns[i].values[row_idx].as_str()).unwrap_or("");
        let hostname = hostname_idx.and_then(|i| data.columns[i].values[row_idx].as_str()).unwrap_or("");
        let service = service_idx.and_then(|i| data.columns[i].values[row_idx].as_str()).unwrap_or("");
        let pid = pid_idx.and_then(|i| data.columns[i].values[row_idx].as_integer());
        let message = message_idx.and_then(|i| data.columns[i].values[row_idx].as_str()).unwrap_or("");

        // Reconstruct line
        if let Some(p) = pid {
            output.push_str(&format!("{} {:2} {} {} {}[{}]: {}\n", 
                month, day, time, hostname, service, p, message));
        } else {
            output.push_str(&format!("{} {:2} {} {} {}: {}\n", 
                month, day, time, hostname, service, message));
        }
    }

    Ok(output)
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_syslog_empty() {
        let data = parse_syslog("").unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn test_parse_syslog_auth_failure() {
        let log = "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=218.188.2.4";
        let data = parse_syslog(log).unwrap();
        
        assert_eq!(data.row_count, 1);
        assert_eq!(data.column_count(), 10);
        
        // Check parsed values
        assert_eq!(data.columns[0].values[0].as_str(), Some("Jun"));
        assert_eq!(data.columns[1].values[0].as_integer(), Some(14));
        assert_eq!(data.columns[2].values[0].as_str(), Some("15:16:01"));
        assert_eq!(data.columns[3].values[0].as_str(), Some("combo"));
        assert_eq!(data.columns[4].values[0].as_str(), Some("sshd(pam_unix)"));
        assert_eq!(data.columns[5].values[0].as_integer(), Some(19939));
        assert_eq!(data.columns[6].values[0].as_str(), Some("auth_fail"));
        assert_eq!(data.columns[7].values[0].as_str(), Some("218.188.2.4"));
    }

    #[test]
    fn test_parse_syslog_session() {
        let log = "Jun 15 04:06:18 combo su(pam_unix)[21416]: session opened for user cyrus by (uid=0)";
        let data = parse_syslog(log).unwrap();
        
        assert_eq!(data.row_count, 1);
        assert_eq!(data.columns[6].values[0].as_str(), Some("sess_open"));
        assert_eq!(data.columns[8].values[0].as_str(), Some("cyrus"));
    }

    #[test]
    fn test_parse_syslog_ftp_connection() {
        let log = "Jun 17 07:07:00 combo ftpd[29504]: connection from 24.54.76.216 (24-54-76-216.bflony.adelphia.net) at Fri Jun 17 07:07:00 2005";
        let data = parse_syslog(log).unwrap();
        
        assert_eq!(data.row_count, 1);
        assert_eq!(data.columns[4].values[0].as_str(), Some("ftpd"));
        assert_eq!(data.columns[5].values[0].as_integer(), Some(29504));
        assert_eq!(data.columns[6].values[0].as_str(), Some("ftp_conn"));
    }

    #[test]
    fn test_parse_syslog_multiple_lines() {
        let log = r#"Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=218.188.2.4
Jun 14 15:16:02 combo sshd(pam_unix)[19937]: check pass; user unknown
Jun 15 04:06:18 combo su(pam_unix)[21416]: session opened for user cyrus by (uid=0)"#;
        
        let data = parse_syslog(log).unwrap();
        
        assert_eq!(data.row_count, 3);
        assert_eq!(data.columns[6].values[0].as_str(), Some("auth_fail"));
        assert_eq!(data.columns[6].values[1].as_str(), Some("check_pass"));
        assert_eq!(data.columns[6].values[2].as_str(), Some("sess_open"));
    }

    #[test]
    fn test_parse_syslog_logrotate() {
        let log = "Jun 15 04:06:20 combo logrotate: ALERT exited abnormally with [1]";
        let data = parse_syslog(log).unwrap();
        
        assert_eq!(data.row_count, 1);
        assert_eq!(data.columns[6].values[0].as_str(), Some("logrotate"));
    }

    #[test]
    fn test_parse_syslog_service_status() {
        let log = "Jun 19 04:08:57 combo cups: cupsd shutdown succeeded";
        let data = parse_syslog(log).unwrap();
        
        assert_eq!(data.row_count, 1);
        assert_eq!(data.columns[6].values[0].as_str(), Some("svc_status"));
    }

    #[test]
    fn test_parse_syslog_auth_with_user() {
        let log = "Jun 15 02:04:59 combo sshd(pam_unix)[20882]: authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=220-135-151-1.hinet-ip.hinet.net  user=root";
        let data = parse_syslog(log).unwrap();
        
        assert_eq!(data.row_count, 1);
        assert_eq!(data.columns[7].values[0].as_str(), Some("220-135-151-1.hinet-ip.hinet.net"));
        assert_eq!(data.columns[8].values[0].as_str(), Some("root"));
    }

    #[test]
    fn test_to_syslog_roundtrip() {
        let original = "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=218.188.2.4";
        let data = parse_syslog(original).unwrap();
        let output = to_syslog(&data).unwrap();
        
        // The output should contain the key parts
        assert!(output.contains("Jun"));
        assert!(output.contains("14"));
        assert!(output.contains("15:16:01"));
        assert!(output.contains("combo"));
        assert!(output.contains("sshd(pam_unix)"));
        assert!(output.contains("19939"));
    }

    #[test]
    fn test_message_type_as_str() {
        assert_eq!(MessageType::AuthFailure.as_str(), "auth_fail");
        assert_eq!(MessageType::SessionOpened.as_str(), "sess_open");
        assert_eq!(MessageType::FtpConnection.as_str(), "ftp_conn");
    }

    #[test]
    fn test_extract_param() {
        let msg = "authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=218.188.2.4";
        assert_eq!(extract_param(msg, "uid="), Some("0"));
        assert_eq!(extract_param(msg, "rhost="), Some("218.188.2.4"));
        assert_eq!(extract_param(msg, "nonexistent="), None);
    }

    #[test]
    fn test_extract_ftp_connection() {
        let msg = "connection from 24.54.76.216 (24-54-76-216.bflony.adelphia.net) at Fri Jun 17 07:07:00 2005";
        let (ip, host) = extract_ftp_connection(msg);
        assert_eq!(ip, Some("24.54.76.216"));
        assert_eq!(host, Some("24-54-76-216.bflony.adelphia.net"));
    }

    #[test]
    fn test_extract_ftp_connection_empty_host() {
        let msg = "connection from 222.33.90.199 () at Mon Jun 20 03:40:59 2005";
        let (ip, host) = extract_ftp_connection(msg);
        assert_eq!(ip, Some("222.33.90.199"));
        assert_eq!(host, None);
    }

    #[test]
    fn test_types_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SyslogEntry<'static>>();
        assert_send_sync::<MessageType>();
        assert_send_sync::<MessageParams<'static>>();
    }

    #[test]
    fn test_parse_syslog_single_digit_day() {
        // Single digit days have extra space: "Jul  1" instead of "Jul 14"
        let log = "Jul  1 00:21:28 combo sshd(pam_unix)[19630]: authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=60.30.224.116  user=root";
        let data = parse_syslog(log).unwrap();
        
        assert_eq!(data.row_count, 1);
        assert_eq!(data.columns[0].values[0].as_str(), Some("Jul"));
        assert_eq!(data.columns[1].values[0].as_integer(), Some(1));
        assert_eq!(data.columns[8].values[0].as_str(), Some("root"));
    }
}