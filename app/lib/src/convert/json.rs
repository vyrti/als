//! JSON parsing and writing.
//!
//! This module provides functions for converting between JSON format and
//! `TabularData` structures. It handles JSON arrays of objects, nested
//! object flattening with dot-notation, and null value preservation.

use crate::convert::{Column, TabularData, Value};
use crate::error::{AlsError, Result};
use serde_json;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io;

/// Parse JSON array of objects into `TabularData`.
///
/// This function parses a JSON array where each element is an object with
/// consistent keys. Nested objects are flattened using dot-notation
/// (e.g., `{"user": {"name": "Alice"}}` becomes column `user.name`).
///
/// # Arguments
///
/// * `input` - JSON text to parse (must be an array of objects)
///
/// # Returns
///
/// A `TabularData` structure containing the parsed data.
///
/// # Examples
///
/// ```
/// use als_compression::convert::json::parse_json;
///
/// let json = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
/// let data = parse_json(json).unwrap();
/// assert_eq!(data.column_count(), 2);
/// assert_eq!(data.row_count, 2);
/// ```
pub fn parse_json(input: &str) -> Result<TabularData<'static>> {
    // Handle empty input
    if input.trim().is_empty() {
        return Ok(TabularData::new());
    }

    // Parse JSON
    let json_value: serde_json::Value = serde_json::from_str(input)?;

    // Ensure it's an array
    let array = match json_value {
        serde_json::Value::Array(arr) => arr,
        _ => {
            return Err(AlsError::JsonParseError(serde_json::Error::io(
                io::Error::new(io::ErrorKind::InvalidData, "Expected JSON array of objects"),
            )))
        }
    };

    // Handle empty array
    if array.is_empty() {
        return Ok(TabularData::new());
    }

    // Flatten all objects and collect all column names
    let mut flattened_rows: Vec<HashMap<String, serde_json::Value>> = Vec::new();
    let mut all_columns: std::collections::HashSet<String> = std::collections::HashSet::new();

    for item in array {
        match item {
            serde_json::Value::Object(obj) => {
                let flattened = flatten_object(&obj, "");
                for key in flattened.keys() {
                    all_columns.insert(key.clone());
                }
                flattened_rows.push(flattened);
            }
            _ => {
                return Err(AlsError::JsonParseError(serde_json::Error::io(
                    io::Error::new(io::ErrorKind::InvalidData, "Array must contain only objects"),
                )))
            }
        }
    }

    // Sort column names for consistent ordering
    let mut column_names: Vec<String> = all_columns.into_iter().collect();
    column_names.sort();

    // Build columns
    let mut columns_data: HashMap<String, Vec<Value<'static>>> = HashMap::new();
    for col_name in &column_names {
        columns_data.insert(col_name.clone(), Vec::new());
    }

    // Populate columns from flattened rows
    for row in &flattened_rows {
        for col_name in &column_names {
            let value = row
                .get(col_name)
                .map(|v| json_value_to_value(v))
                .unwrap_or(Value::Null);
            columns_data.get_mut(col_name).unwrap().push(value);
        }
    }

    // Create TabularData
    let mut data = TabularData::with_capacity(column_names.len());
    for col_name in column_names {
        let values = columns_data.remove(&col_name).unwrap();
        data.add_column(Column::new(Cow::Owned(col_name), values));
    }

    Ok(data)
}

/// Flatten a JSON object using dot-notation for nested keys.
///
/// For example: `{"user": {"name": "Alice", "age": 30}}` becomes:
/// - `user.name` -> "Alice"
/// - `user.age` -> 30
fn flatten_object(
    obj: &serde_json::Map<String, serde_json::Value>,
    prefix: &str,
) -> HashMap<String, serde_json::Value> {
    let mut result = HashMap::new();

    for (key, value) in obj {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", prefix, key)
        };

        match value {
            serde_json::Value::Object(nested_obj) => {
                // Recursively flatten nested objects
                let nested = flatten_object(nested_obj, &full_key);
                result.extend(nested);
            }
            _ => {
                // Non-object values are added directly
                result.insert(full_key, value.clone());
            }
        }
    }

    result
}

/// Convert a `serde_json::Value` to our `Value` type.
fn json_value_to_value(json_val: &serde_json::Value) -> Value<'static> {
    match json_val {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                // Fallback for numbers that don't fit i64 or f64
                Value::String(Cow::Owned(n.to_string()))
            }
        }
        serde_json::Value::String(s) => Value::String(Cow::Owned(s.clone())),
        serde_json::Value::Array(_) => {
            // Arrays are serialized as JSON strings
            Value::String(Cow::Owned(json_val.to_string()))
        }
        serde_json::Value::Object(_) => {
            // This shouldn't happen after flattening, but handle it
            Value::String(Cow::Owned(json_val.to_string()))
        }
    }
}

/// Convert `TabularData` to JSON array format.
///
/// This function serializes tabular data to a JSON array of objects.
/// Dot-notation column names are reconstructed into nested objects.
///
/// # Arguments
///
/// * `data` - The tabular data to convert
///
/// # Returns
///
/// A JSON string representation of the data.
///
/// # Examples
///
/// ```
/// use als_compression::convert::{TabularData, Column, Value};
/// use als_compression::convert::json::to_json;
/// use std::borrow::Cow;
///
/// let mut data = TabularData::new();
/// data.add_column(Column::new(
///     Cow::Borrowed("id"),
///     vec![Value::Integer(1), Value::Integer(2)],
/// ));
/// data.add_column(Column::new(
///     Cow::Borrowed("name"),
///     vec![Value::string("Alice"), Value::string("Bob")],
/// ));
///
/// let json = to_json(&data).unwrap();
/// assert!(json.contains("\"id\""));
/// assert!(json.contains("\"name\""));
/// ```
pub fn to_json(data: &TabularData) -> Result<String> {
    // Handle empty data
    if data.is_empty() || data.column_count() == 0 {
        return Ok("[]".to_string());
    }

    let mut array = Vec::new();

    // Build each row as a JSON object
    for row_idx in 0..data.row_count {
        let mut row_obj = serde_json::Map::new();

        for col in &data.columns {
            let value = &col.values[row_idx];
            let json_value = value_to_json_value(value);

            // Handle dot-notation to reconstruct nested objects
            insert_nested(&mut row_obj, col.name.as_ref(), json_value);
        }

        array.push(serde_json::Value::Object(row_obj));
    }

    // Serialize to JSON string
    serde_json::to_string(&array).map_err(|e| e.into())
}

/// Insert a value into a JSON object, creating nested structure for dot-notation keys.
///
/// For example, inserting key "user.name" with value "Alice" creates:
/// `{"user": {"name": "Alice"}}`
fn insert_nested(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: serde_json::Value,
) {
    let parts: Vec<&str> = key.split('.').collect();

    if parts.len() == 1 {
        // Simple key, insert directly
        obj.insert(key.to_string(), value);
    } else {
        // Nested key, create intermediate objects
        let mut current = obj;

        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // Last part, insert the value
                current.insert(part.to_string(), value);
                break;
            } else {
                // Intermediate part, ensure object exists
                current = current
                    .entry(part.to_string())
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()))
                    .as_object_mut()
                    .expect("Expected object for nested key");
            }
        }
    }
}

/// Convert our `Value` type to `serde_json::Value`.
fn value_to_json_value(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => {
            serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::Boolean(b) => serde_json::Value::Bool(*b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::ColumnType;

    #[test]
    fn test_parse_json_basic() {
        let json = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
        let data = parse_json(json).unwrap();

        assert_eq!(data.column_count(), 2);
        assert_eq!(data.row_count, 2);

        // Columns should be sorted alphabetically
        let col_names = data.column_names();
        assert_eq!(col_names, vec!["id", "name"]);

        // Check first row
        let row0 = data.get_row(0).unwrap();
        assert_eq!(row0[0].as_integer(), Some(1));
        assert_eq!(row0[1].as_str(), Some("Alice"));

        // Check second row
        let row1 = data.get_row(1).unwrap();
        assert_eq!(row1[0].as_integer(), Some(2));
        assert_eq!(row1[1].as_str(), Some("Bob"));
    }

    #[test]
    fn test_parse_json_empty() {
        let json = "";
        let data = parse_json(json).unwrap();

        assert_eq!(data.column_count(), 0);
        assert_eq!(data.row_count, 0);
        assert!(data.is_empty());
    }

    #[test]
    fn test_parse_json_empty_array() {
        let json = "[]";
        let data = parse_json(json).unwrap();

        assert_eq!(data.column_count(), 0);
        assert_eq!(data.row_count, 0);
        assert!(data.is_empty());
    }

    #[test]
    fn test_parse_json_single_object() {
        let json = r#"[{"id": 42, "name": "Alice"}]"#;
        let data = parse_json(json).unwrap();

        assert_eq!(data.column_count(), 2);
        assert_eq!(data.row_count, 1);
    }

    #[test]
    fn test_parse_json_nested_objects() {
        let json = r#"[
            {"id": 1, "user": {"name": "Alice", "age": 30}},
            {"id": 2, "user": {"name": "Bob", "age": 25}}
        ]"#;
        let data = parse_json(json).unwrap();

        assert_eq!(data.column_count(), 3);
        assert_eq!(data.row_count, 2);

        // Check flattened column names
        let col_names = data.column_names();
        assert!(col_names.contains(&"id"));
        assert!(col_names.contains(&"user.name"));
        assert!(col_names.contains(&"user.age"));

        // Check values
        let id_col = data.get_column_by_name("id").unwrap();
        assert_eq!(id_col.values[0].as_integer(), Some(1));

        let name_col = data.get_column_by_name("user.name").unwrap();
        assert_eq!(name_col.values[0].as_str(), Some("Alice"));

        let age_col = data.get_column_by_name("user.age").unwrap();
        assert_eq!(age_col.values[0].as_integer(), Some(30));
    }

    #[test]
    fn test_parse_json_null_values() {
        let json = r#"[
            {"id": 1, "name": "Alice", "email": null},
            {"id": 2, "name": null, "email": "bob@example.com"}
        ]"#;
        let data = parse_json(json).unwrap();

        assert_eq!(data.row_count, 2);

        let email_col = data.get_column_by_name("email").unwrap();
        assert!(email_col.values[0].is_null());
        assert_eq!(email_col.values[1].as_str(), Some("bob@example.com"));

        let name_col = data.get_column_by_name("name").unwrap();
        assert_eq!(name_col.values[0].as_str(), Some("Alice"));
        assert!(name_col.values[1].is_null());
    }

    #[test]
    fn test_parse_json_missing_keys() {
        // Objects with inconsistent keys should fill missing values with null
        let json = r#"[
            {"id": 1, "name": "Alice"},
            {"id": 2, "email": "bob@example.com"}
        ]"#;
        let data = parse_json(json).unwrap();

        assert_eq!(data.column_count(), 3);
        assert_eq!(data.row_count, 2);

        // First row has name but no email
        let name_col = data.get_column_by_name("name").unwrap();
        assert_eq!(name_col.values[0].as_str(), Some("Alice"));
        assert!(name_col.values[1].is_null());

        let email_col = data.get_column_by_name("email").unwrap();
        assert!(email_col.values[0].is_null());
        assert_eq!(email_col.values[1].as_str(), Some("bob@example.com"));
    }

    #[test]
    fn test_parse_json_type_inference() {
        let json = r#"[
            {"int": 42, "float": 3.14, "bool": true, "str": "hello"}
        ]"#;
        let data = parse_json(json).unwrap();

        let int_col = data.get_column_by_name("int").unwrap();
        assert_eq!(int_col.inferred_type, ColumnType::Integer);
        assert_eq!(int_col.values[0].as_integer(), Some(42));

        let float_col = data.get_column_by_name("float").unwrap();
        assert_eq!(float_col.inferred_type, ColumnType::Float);
        assert_eq!(float_col.values[0].as_float(), Some(3.14));

        let bool_col = data.get_column_by_name("bool").unwrap();
        assert_eq!(bool_col.inferred_type, ColumnType::Boolean);
        assert_eq!(bool_col.values[0].as_boolean(), Some(true));

        let str_col = data.get_column_by_name("str").unwrap();
        assert_eq!(str_col.inferred_type, ColumnType::String);
        assert_eq!(str_col.values[0].as_str(), Some("hello"));
    }

    #[test]
    fn test_parse_json_error_not_array() {
        let json = r#"{"id": 1, "name": "Alice"}"#;
        let result = parse_json(json);

        assert!(result.is_err());
        assert!(matches!(result, Err(AlsError::JsonParseError(_))));
    }

    #[test]
    fn test_parse_json_error_array_not_objects() {
        let json = r#"[1, 2, 3]"#;
        let result = parse_json(json);

        assert!(result.is_err());
        assert!(matches!(result, Err(AlsError::JsonParseError(_))));
    }

    #[test]
    fn test_parse_json_error_invalid_json() {
        let json = r#"[{"id": 1, "name": "Alice"#; // Missing closing brackets
        let result = parse_json(json);

        assert!(result.is_err());
        assert!(matches!(result, Err(AlsError::JsonParseError(_))));
    }

    #[test]
    fn test_to_json_basic() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Borrowed("id"),
            vec![Value::Integer(1), Value::Integer(2)],
        ));
        data.add_column(Column::new(
            Cow::Borrowed("name"),
            vec![Value::string("Alice"), Value::string("Bob")],
        ));

        let json = to_json(&data).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(parsed.is_array());
        let array = parsed.as_array().unwrap();
        assert_eq!(array.len(), 2);

        assert_eq!(array[0]["id"], 1);
        assert_eq!(array[0]["name"], "Alice");
        assert_eq!(array[1]["id"], 2);
        assert_eq!(array[1]["name"], "Bob");
    }

    #[test]
    fn test_to_json_empty() {
        let data = TabularData::new();
        let json = to_json(&data).unwrap();

        assert_eq!(json, "[]");
    }

    #[test]
    fn test_to_json_single_row() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Borrowed("id"),
            vec![Value::Integer(42)],
        ));

        let json = to_json(&data).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(parsed.is_array());
        let array = parsed.as_array().unwrap();
        assert_eq!(array.len(), 1);
        assert_eq!(array[0]["id"], 42);
    }

    #[test]
    fn test_to_json_null_values() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Borrowed("col"),
            vec![Value::Null, Value::Integer(1), Value::Null],
        ));

        let json = to_json(&data).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        let array = parsed.as_array().unwrap();
        assert_eq!(array.len(), 3);
        assert!(array[0]["col"].is_null());
        assert_eq!(array[1]["col"], 1);
        assert!(array[2]["col"].is_null());
    }

    #[test]
    fn test_to_json_nested_reconstruction() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Borrowed("id"),
            vec![Value::Integer(1)],
        ));
        data.add_column(Column::new(
            Cow::Borrowed("user.name"),
            vec![Value::string("Alice")],
        ));
        data.add_column(Column::new(
            Cow::Borrowed("user.age"),
            vec![Value::Integer(30)],
        ));

        let json = to_json(&data).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        let array = parsed.as_array().unwrap();
        assert_eq!(array.len(), 1);

        let obj = &array[0];
        assert_eq!(obj["id"], 1);
        assert_eq!(obj["user"]["name"], "Alice");
        assert_eq!(obj["user"]["age"], 30);
    }

    #[test]
    fn test_to_json_all_types() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Borrowed("int"),
            vec![Value::Integer(42)],
        ));
        data.add_column(Column::new(
            Cow::Borrowed("float"),
            vec![Value::Float(3.14)],
        ));
        data.add_column(Column::new(
            Cow::Borrowed("bool"),
            vec![Value::Boolean(true)],
        ));
        data.add_column(Column::new(
            Cow::Borrowed("str"),
            vec![Value::string("hello")],
        ));
        data.add_column(Column::new(
            Cow::Borrowed("null"),
            vec![Value::Null],
        ));

        let json = to_json(&data).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        let array = parsed.as_array().unwrap();
        let obj = &array[0];

        assert_eq!(obj["int"], 42);
        assert_eq!(obj["float"], 3.14);
        assert_eq!(obj["bool"], true);
        assert_eq!(obj["str"], "hello");
        assert!(obj["null"].is_null());
    }

    #[test]
    fn test_json_round_trip() {
        let original_json = r#"[
            {"id": 1, "name": "Alice", "active": true},
            {"id": 2, "name": "Bob", "active": false}
        ]"#;

        let data = parse_json(original_json).unwrap();
        let output_json = to_json(&data).unwrap();

        // Parse the output again to verify
        let data2 = parse_json(&output_json).unwrap();

        assert_eq!(data.column_count(), data2.column_count());
        assert_eq!(data.row_count, data2.row_count);
        assert_eq!(data.column_names(), data2.column_names());
    }

    #[test]
    fn test_json_round_trip_nested() {
        let original_json = r#"[
            {"id": 1, "user": {"name": "Alice", "age": 30}},
            {"id": 2, "user": {"name": "Bob", "age": 25}}
        ]"#;

        let data = parse_json(original_json).unwrap();
        let output_json = to_json(&data).unwrap();

        // Parse the output again to verify structure
        let parsed: serde_json::Value = serde_json::from_str(&output_json).unwrap();
        let array = parsed.as_array().unwrap();

        assert_eq!(array[0]["id"], 1);
        assert_eq!(array[0]["user"]["name"], "Alice");
        assert_eq!(array[0]["user"]["age"], 30);
    }

    #[test]
    fn test_flatten_object() {
        let mut obj = serde_json::Map::new();
        obj.insert("id".to_string(), serde_json::json!(1));

        let mut user = serde_json::Map::new();
        user.insert("name".to_string(), serde_json::json!("Alice"));
        user.insert("age".to_string(), serde_json::json!(30));
        obj.insert("user".to_string(), serde_json::Value::Object(user));

        let flattened = flatten_object(&obj, "");

        assert_eq!(flattened.len(), 3);
        assert_eq!(flattened.get("id").unwrap(), &serde_json::json!(1));
        assert_eq!(flattened.get("user.name").unwrap(), &serde_json::json!("Alice"));
        assert_eq!(flattened.get("user.age").unwrap(), &serde_json::json!(30));
    }

    #[test]
    fn test_insert_nested() {
        let mut obj = serde_json::Map::new();

        insert_nested(&mut obj, "id", serde_json::json!(1));
        insert_nested(&mut obj, "user.name", serde_json::json!("Alice"));
        insert_nested(&mut obj, "user.age", serde_json::json!(30));

        assert_eq!(obj.get("id").unwrap(), &serde_json::json!(1));
        assert_eq!(
            obj.get("user").unwrap().get("name").unwrap(),
            &serde_json::json!("Alice")
        );
        assert_eq!(
            obj.get("user").unwrap().get("age").unwrap(),
            &serde_json::json!(30)
        );
    }

    #[test]
    fn test_json_value_to_value() {
        assert!(json_value_to_value(&serde_json::Value::Null).is_null());
        assert_eq!(
            json_value_to_value(&serde_json::json!(42)).as_integer(),
            Some(42)
        );
        assert_eq!(
            json_value_to_value(&serde_json::json!(3.14)).as_float(),
            Some(3.14)
        );
        assert_eq!(
            json_value_to_value(&serde_json::json!("hello")).as_str(),
            Some("hello")
        );
        assert_eq!(
            json_value_to_value(&serde_json::json!(true)).as_boolean(),
            Some(true)
        );
    }

    #[test]
    fn test_value_to_json_value() {
        assert!(value_to_json_value(&Value::Null).is_null());
        assert_eq!(value_to_json_value(&Value::Integer(42)), serde_json::json!(42));
        assert_eq!(value_to_json_value(&Value::Float(3.14)), serde_json::json!(3.14));
        assert_eq!(
            value_to_json_value(&Value::string("hello")),
            serde_json::json!("hello")
        );
        assert_eq!(
            value_to_json_value(&Value::Boolean(true)),
            serde_json::json!(true)
        );
    }
}
