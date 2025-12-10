//! Tabular data model with zero-copy support.
//!
//! This module defines the `TabularData` struct and related types for
//! representing structured data in a format-agnostic way.

use std::borrow::Cow;

/// Zero-copy tabular data representation.
///
/// `TabularData` represents structured data as a collection of columns,
/// where each column contains values of a consistent type. The lifetime
/// parameter `'a` allows for zero-copy references to the original data
/// when possible.
///
/// # Examples
///
/// ```
/// use als_compression::convert::{TabularData, Column, Value, ColumnType};
/// use std::borrow::Cow;
///
/// let mut data = TabularData::new();
/// data.add_column(Column::new("id", vec![
///     Value::Integer(1),
///     Value::Integer(2),
///     Value::Integer(3),
/// ]));
/// data.add_column(Column::new("name", vec![
///     Value::String(Cow::Borrowed("Alice")),
///     Value::String(Cow::Borrowed("Bob")),
///     Value::String(Cow::Borrowed("Charlie")),
/// ]));
///
/// assert_eq!(data.row_count, 3);
/// assert_eq!(data.column_count(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct TabularData<'a> {
    /// Columns of data.
    pub columns: Vec<Column<'a>>,
    /// Number of rows in the data.
    pub row_count: usize,
}

impl<'a> TabularData<'a> {
    /// Create a new empty tabular data structure.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            row_count: 0,
        }
    }

    /// Create tabular data with pre-allocated column capacity.
    pub fn with_capacity(column_count: usize) -> Self {
        Self {
            columns: Vec::with_capacity(column_count),
            row_count: 0,
        }
    }

    /// Add a column to the data.
    ///
    /// # Panics
    ///
    /// Panics if the column has a different number of values than existing columns.
    pub fn add_column(&mut self, column: Column<'a>) {
        let col_len = column.len();
        
        if self.columns.is_empty() {
            self.row_count = col_len;
        } else {
            assert_eq!(
                col_len, self.row_count,
                "Column '{}' has {} values, expected {}",
                column.name, col_len, self.row_count
            );
        }
        
        self.columns.push(column);
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get a column by index.
    pub fn get_column(&self, index: usize) -> Option<&Column<'a>> {
        self.columns.get(index)
    }

    /// Get a column by name.
    pub fn get_column_by_name(&self, name: &str) -> Option<&Column<'a>> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get the column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_ref()).collect()
    }

    /// Get a row as a vector of values.
    ///
    /// Returns `None` if the row index is out of bounds.
    pub fn get_row(&self, index: usize) -> Option<Vec<&Value<'a>>> {
        if index >= self.row_count {
            return None;
        }
        
        Some(
            self.columns
                .iter()
                .map(|col| &col.values[index])
                .collect()
        )
    }

    /// Check if the data is empty (no rows).
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Iterate over rows.
    pub fn rows(&self) -> impl Iterator<Item = Vec<&Value<'a>>> {
        (0..self.row_count).map(move |i| {
            self.columns
                .iter()
                .map(|col| &col.values[i])
                .collect()
        })
    }

    /// Convert to owned data (removes lifetime dependency).
    pub fn into_owned(self) -> TabularData<'static> {
        TabularData {
            columns: self.columns.into_iter().map(|c| c.into_owned()).collect(),
            row_count: self.row_count,
        }
    }
}

impl Default for TabularData<'_> {
    fn default() -> Self {
        Self::new()
    }
}

/// A single column of data.
///
/// Contains the column name, values, and inferred type.
#[derive(Debug, Clone)]
pub struct Column<'a> {
    /// Column name.
    pub name: Cow<'a, str>,
    /// Column values.
    pub values: Vec<Value<'a>>,
    /// Inferred column type based on values.
    pub inferred_type: ColumnType,
}

impl<'a> Column<'a> {
    /// Create a new column with the given name and values.
    ///
    /// The column type is automatically inferred from the values.
    pub fn new<S: Into<Cow<'a, str>>>(name: S, values: Vec<Value<'a>>) -> Self {
        let inferred_type = Self::infer_type(&values);
        Self {
            name: name.into(),
            values,
            inferred_type,
        }
    }

    /// Create a new column with an explicit type.
    pub fn with_type<S: Into<Cow<'a, str>>>(
        name: S,
        values: Vec<Value<'a>>,
        column_type: ColumnType,
    ) -> Self {
        Self {
            name: name.into(),
            values,
            inferred_type: column_type,
        }
    }

    /// Get the number of values in the column.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the column is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a value by index.
    pub fn get(&self, index: usize) -> Option<&Value<'a>> {
        self.values.get(index)
    }

    /// Infer the column type from values.
    fn infer_type(values: &[Value<'a>]) -> ColumnType {
        if values.is_empty() {
            return ColumnType::String; // Default for empty columns
        }

        let mut has_integer = false;
        let mut has_float = false;
        let mut has_string = false;
        let mut has_boolean = false;

        for value in values {
            match value {
                Value::Null => {} // Null is compatible with any type
                Value::Integer(_) => has_integer = true,
                Value::Float(_) => has_float = true,
                Value::String(_) => has_string = true,
                Value::Boolean(_) => has_boolean = true,
            }
        }

        // Determine the most specific type
        let type_count = [has_integer, has_float, has_string, has_boolean]
            .iter()
            .filter(|&&b| b)
            .count();

        if type_count == 0 {
            // All nulls
            ColumnType::String
        } else if type_count > 1 {
            // Mixed types
            if has_string {
                ColumnType::String
            } else if has_float && has_integer {
                ColumnType::Float // Integers can be represented as floats
            } else {
                ColumnType::Mixed
            }
        } else if has_integer {
            ColumnType::Integer
        } else if has_float {
            ColumnType::Float
        } else if has_boolean {
            ColumnType::Boolean
        } else {
            ColumnType::String
        }
    }

    /// Convert to owned column (removes lifetime dependency).
    pub fn into_owned(self) -> Column<'static> {
        Column {
            name: Cow::Owned(self.name.into_owned()),
            values: self.values.into_iter().map(|v| v.into_owned()).collect(),
            inferred_type: self.inferred_type,
        }
    }
}

/// A single value in the tabular data.
///
/// Values can be null, integers, floats, strings, or booleans.
/// String values use `Cow` for zero-copy support.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Value<'a> {
    /// Null/missing value.
    #[default]
    Null,
    /// Integer value (i64).
    Integer(i64),
    /// Floating point value (f64).
    Float(f64),
    /// String value with zero-copy support.
    String(Cow<'a, str>),
    /// Boolean value.
    Boolean(bool),
}

impl<'a> Value<'a> {
    /// Create a string value from a borrowed string.
    pub fn string(s: &'a str) -> Self {
        Value::String(Cow::Borrowed(s))
    }

    /// Create a string value from an owned string.
    pub fn string_owned(s: String) -> Self {
        Value::String(Cow::Owned(s))
    }

    /// Check if the value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if the value is an integer.
    pub fn is_integer(&self) -> bool {
        matches!(self, Value::Integer(_))
    }

    /// Check if the value is a float.
    pub fn is_float(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    /// Check if the value is a string.
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    /// Check if the value is a boolean.
    pub fn is_boolean(&self) -> bool {
        matches!(self, Value::Boolean(_))
    }

    /// Get the value as an integer, if it is one.
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Get the value as a float, if it is one.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Get the value as a string reference, if it is one.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s.as_ref()),
            _ => None,
        }
    }

    /// Get the value as a boolean, if it is one.
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Convert the value to a string representation.
    ///
    /// For ALS format, null values are represented as `NULL_TOKEN` and
    /// empty strings as `EMPTY_TOKEN`.
    pub fn to_string_repr(&self) -> Cow<'_, str> {
        match self {
            Value::Null => Cow::Borrowed(crate::als::NULL_TOKEN),
            Value::Integer(i) => Cow::Owned(i.to_string()),
            Value::Float(f) => Cow::Owned(f.to_string()),
            Value::String(s) => {
                if s.is_empty() {
                    Cow::Borrowed(crate::als::EMPTY_TOKEN)
                } else {
                    Cow::Borrowed(s.as_ref())
                }
            }
            Value::Boolean(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
        }
    }

    /// Convert to owned value (removes lifetime dependency).
    pub fn into_owned(self) -> Value<'static> {
        match self {
            Value::Null => Value::Null,
            Value::Integer(i) => Value::Integer(i),
            Value::Float(f) => Value::Float(f),
            Value::String(s) => Value::String(Cow::Owned(s.into_owned())),
            Value::Boolean(b) => Value::Boolean(b),
        }
    }
}



impl From<i64> for Value<'_> {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<f64> for Value<'_> {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<bool> for Value<'_> {
    fn from(b: bool) -> Self {
        Value::Boolean(b)
    }
}

impl From<String> for Value<'_> {
    fn from(s: String) -> Self {
        Value::String(Cow::Owned(s))
    }
}

impl<'a> From<&'a str> for Value<'a> {
    fn from(s: &'a str) -> Self {
        Value::String(Cow::Borrowed(s))
    }
}

/// Column type enumeration.
///
/// Represents the inferred or declared type of a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ColumnType {
    /// Integer values (i64).
    Integer,
    /// Floating point values (f64).
    Float,
    /// String values.
    #[default]
    String,
    /// Boolean values.
    Boolean,
    /// Mixed types (column contains multiple incompatible types).
    Mixed,
}

impl ColumnType {
    /// Check if this type can represent the given value.
    pub fn can_represent(&self, value: &Value) -> bool {
        match (self, value) {
            (_, Value::Null) => true, // Null is compatible with any type
            (ColumnType::Integer, Value::Integer(_)) => true,
            (ColumnType::Float, Value::Float(_)) => true,
            (ColumnType::Float, Value::Integer(_)) => true, // Integers can be floats
            (ColumnType::String, Value::String(_)) => true,
            (ColumnType::Boolean, Value::Boolean(_)) => true,
            (ColumnType::Mixed, _) => true, // Mixed accepts anything
            _ => false,
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tabular_data_new() {
        let data = TabularData::new();
        assert!(data.is_empty());
        assert_eq!(data.column_count(), 0);
        assert_eq!(data.row_count, 0);
    }

    #[test]
    fn test_tabular_data_add_column() {
        let mut data = TabularData::new();
        data.add_column(Column::new("id", vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]));
        
        assert_eq!(data.column_count(), 1);
        assert_eq!(data.row_count, 3);
    }

    #[test]
    #[should_panic(expected = "has 2 values, expected 3")]
    fn test_tabular_data_add_column_mismatch() {
        let mut data = TabularData::new();
        data.add_column(Column::new("col1", vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]));
        data.add_column(Column::new("col2", vec![
            Value::Integer(1),
            Value::Integer(2),
        ]));
    }

    #[test]
    fn test_tabular_data_get_column() {
        let mut data = TabularData::new();
        data.add_column(Column::new("id", vec![Value::Integer(1)]));
        data.add_column(Column::new("name", vec![Value::string("Alice")]));
        
        assert!(data.get_column(0).is_some());
        assert!(data.get_column(2).is_none());
        
        assert!(data.get_column_by_name("id").is_some());
        assert!(data.get_column_by_name("unknown").is_none());
    }

    #[test]
    fn test_tabular_data_get_row() {
        let mut data = TabularData::new();
        data.add_column(Column::new("id", vec![
            Value::Integer(1),
            Value::Integer(2),
        ]));
        data.add_column(Column::new("name", vec![
            Value::string("Alice"),
            Value::string("Bob"),
        ]));
        
        let row = data.get_row(0).unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row[0].as_integer(), Some(1));
        assert_eq!(row[1].as_str(), Some("Alice"));
        
        assert!(data.get_row(5).is_none());
    }

    #[test]
    fn test_tabular_data_rows_iterator() {
        let mut data = TabularData::new();
        data.add_column(Column::new("id", vec![
            Value::Integer(1),
            Value::Integer(2),
        ]));
        
        let rows: Vec<_> = data.rows().collect();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_tabular_data_column_names() {
        let mut data = TabularData::new();
        data.add_column(Column::new("id", vec![Value::Integer(1)]));
        data.add_column(Column::new("name", vec![Value::string("Alice")]));
        
        assert_eq!(data.column_names(), vec!["id", "name"]);
    }

    #[test]
    fn test_column_new() {
        let col = Column::new("test", vec![
            Value::Integer(1),
            Value::Integer(2),
        ]);
        
        assert_eq!(col.name, "test");
        assert_eq!(col.len(), 2);
        assert_eq!(col.inferred_type, ColumnType::Integer);
    }

    #[test]
    fn test_column_type_inference() {
        // All integers
        let col = Column::new("int", vec![Value::Integer(1), Value::Integer(2)]);
        assert_eq!(col.inferred_type, ColumnType::Integer);
        
        // All floats
        let col = Column::new("float", vec![Value::Float(1.0), Value::Float(2.0)]);
        assert_eq!(col.inferred_type, ColumnType::Float);
        
        // All strings
        let col = Column::new("str", vec![Value::string("a"), Value::string("b")]);
        assert_eq!(col.inferred_type, ColumnType::String);
        
        // All booleans
        let col = Column::new("bool", vec![Value::Boolean(true), Value::Boolean(false)]);
        assert_eq!(col.inferred_type, ColumnType::Boolean);
        
        // Mixed int and float -> Float
        let col = Column::new("mixed", vec![Value::Integer(1), Value::Float(2.0)]);
        assert_eq!(col.inferred_type, ColumnType::Float);
        
        // Mixed with string -> String
        let col = Column::new("mixed", vec![Value::Integer(1), Value::string("a")]);
        assert_eq!(col.inferred_type, ColumnType::String);
        
        // All nulls -> String (default)
        let col = Column::new("null", vec![Value::Null, Value::Null]);
        assert_eq!(col.inferred_type, ColumnType::String);
        
        // Empty -> String (default)
        let col: Column = Column::new("empty", vec![]);
        assert_eq!(col.inferred_type, ColumnType::String);
    }

    #[test]
    fn test_value_constructors() {
        assert!(Value::Null.is_null());
        assert!(Value::Integer(42).is_integer());
        assert!(Value::Float(3.14).is_float());
        assert!(Value::string("hello").is_string());
        assert!(Value::string_owned("world".to_string()).is_string());
        assert!(Value::Boolean(true).is_boolean());
    }

    #[test]
    fn test_value_accessors() {
        assert_eq!(Value::Integer(42).as_integer(), Some(42));
        assert_eq!(Value::Integer(42).as_float(), Some(42.0));
        assert_eq!(Value::Float(3.14).as_float(), Some(3.14));
        assert_eq!(Value::string("hello").as_str(), Some("hello"));
        assert_eq!(Value::Boolean(true).as_boolean(), Some(true));
        
        assert_eq!(Value::Null.as_integer(), None);
        assert_eq!(Value::string("hello").as_integer(), None);
    }

    #[test]
    fn test_value_to_string_repr() {
        assert_eq!(Value::Null.to_string_repr(), crate::als::NULL_TOKEN);
        assert_eq!(Value::Integer(42).to_string_repr(), "42");
        assert_eq!(Value::Float(3.14).to_string_repr(), "3.14");
        assert_eq!(Value::string("hello").to_string_repr(), "hello");
        assert_eq!(Value::string("").to_string_repr(), crate::als::EMPTY_TOKEN);
        assert_eq!(Value::Boolean(true).to_string_repr(), "true");
        assert_eq!(Value::Boolean(false).to_string_repr(), "false");
    }

    #[test]
    fn test_value_from_impls() {
        let v: Value = 42i64.into();
        assert_eq!(v, Value::Integer(42));
        
        let v: Value = 3.14f64.into();
        assert_eq!(v, Value::Float(3.14));
        
        let v: Value = true.into();
        assert_eq!(v, Value::Boolean(true));
        
        let v: Value = "hello".into();
        assert!(matches!(v, Value::String(Cow::Borrowed("hello"))));
        
        let v: Value = String::from("world").into();
        assert!(matches!(v, Value::String(Cow::Owned(_))));
    }

    #[test]
    fn test_column_type_can_represent() {
        assert!(ColumnType::Integer.can_represent(&Value::Integer(1)));
        assert!(ColumnType::Integer.can_represent(&Value::Null));
        assert!(!ColumnType::Integer.can_represent(&Value::string("a")));
        
        assert!(ColumnType::Float.can_represent(&Value::Float(1.0)));
        assert!(ColumnType::Float.can_represent(&Value::Integer(1)));
        
        assert!(ColumnType::Mixed.can_represent(&Value::Integer(1)));
        assert!(ColumnType::Mixed.can_represent(&Value::string("a")));
    }

    #[test]
    fn test_into_owned() {
        let data = {
            let s = String::from("test");
            let mut data = TabularData::new();
            data.add_column(Column::new(s.as_str(), vec![Value::string(s.as_str())]));
            data.into_owned()
        };
        
        // Data should still be valid after original strings are dropped
        assert_eq!(data.column_count(), 1);
        assert_eq!(data.columns[0].name, "test");
    }

    #[test]
    fn test_value_default() {
        assert_eq!(Value::default(), Value::Null);
    }

    #[test]
    fn test_column_type_default() {
        assert_eq!(ColumnType::default(), ColumnType::String);
    }

    #[test]
    fn test_types_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TabularData<'static>>();
        assert_send_sync::<Column<'static>>();
        assert_send_sync::<Value<'static>>();
        assert_send_sync::<ColumnType>();
    }
}
