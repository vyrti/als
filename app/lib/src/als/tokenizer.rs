//! ALS format tokenizer.
//!
//! This module provides a lexer for tokenizing ALS format input into
//! discrete tokens that can be consumed by the parser.
//!
//! # Token Types
//!
//! - Version prefix: `!v1` or `!ctx`
//! - Dictionary header: `$name:val1|val2`
//! - Schema prefix: `#column_name`
//! - Operators: `>`, `*`, `~`
//! - Column separator: `|`
//! - Dictionary reference: `_0`, `_1`, etc.
//! - Numbers and raw values

use crate::error::{AlsError, Result};

/// Token types produced by the ALS tokenizer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// Version indicator: `!v1` (ALS) or `!ctx` (CTX fallback)
    Version(VersionType),
    /// Dictionary header: `$name:val1|val2|val3`
    DictionaryHeader {
        /// Dictionary name
        name: String,
        /// Dictionary values
        values: Vec<String>,
    },
    /// Schema column: `#column_name`
    SchemaColumn(String),
    /// Integer literal
    Integer(i64),
    /// Float literal
    Float(f64),
    /// Raw string value (possibly escaped)
    RawValue(String),
    /// Range operator: `>`
    RangeOp,
    /// Multiplier operator: `*`
    MultiplyOp,
    /// Toggle operator: `~`
    ToggleOp,
    /// Column separator: `|`
    ColumnSeparator,
    /// Dictionary reference: `_0`, `_1`, etc.
    DictRef(usize),
    /// Step separator in ranges: `:`
    StepSeparator,
    /// Open parenthesis for grouping: `(`
    OpenParen,
    /// Close parenthesis for grouping: `)`
    CloseParen,
    /// Newline (significant in some contexts)
    Newline,
    /// End of input
    Eof,
}

/// Version type indicator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionType {
    /// ALS format with version number
    Als(u8),
    /// CTX fallback format
    Ctx,
}

/// ALS tokenizer that produces tokens from input text.
pub struct Tokenizer<'a> {
    input: &'a str,
    chars: std::iter::Peekable<std::str::CharIndices<'a>>,
    position: usize,
    /// Whether we're in the header section (before streams)
    in_header: bool,
}

impl<'a> Tokenizer<'a> {
    /// Create a new tokenizer for the given input.
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            chars: input.char_indices().peekable(),
            position: 0,
            in_header: true,
        }
    }

    /// Get the current position in the input.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Peek at the next character without consuming it.
    fn peek_char(&mut self) -> Option<char> {
        self.chars.peek().map(|(_, c)| *c)
    }

    /// Consume and return the next character.
    fn next_char(&mut self) -> Option<char> {
        self.chars.next().map(|(pos, c)| {
            self.position = pos + c.len_utf8();
            c
        })
    }

    /// Skip whitespace characters (except newlines in certain contexts).
    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c == ' ' || c == '\t' || c == '\r' {
                self.next_char();
            } else {
                break;
            }
        }
    }

    /// Read an escaped string value until a delimiter is encountered.
    fn read_escaped_value(&mut self, delimiters: &[char]) -> Result<String> {
        let mut result = String::new();
        let start_pos = self.position;

        while let Some(c) = self.peek_char() {
            if delimiters.contains(&c) {
                break;
            }

            self.next_char();

            if c == '\\' {
                // Handle escape sequence
                match self.next_char() {
                    Some('>') => result.push('>'),
                    Some('*') => result.push('*'),
                    Some('~') => result.push('~'),
                    Some('|') => result.push('|'),
                    Some('_') => result.push('_'),
                    Some('#') => result.push('#'),
                    Some('$') => result.push('$'),
                    Some(':') => result.push(':'),
                    Some('\\') => result.push('\\'),
                    Some('n') => result.push('\n'),
                    Some('t') => result.push('\t'),
                    Some('r') => result.push('\r'),
                    Some(' ') => result.push(' '),
                    Some('0') => {
                        // Null token - return special marker
                        return Ok("\0".to_string());
                    }
                    Some('e') => {
                        // Empty token - return empty string marker
                        return Ok(String::new());
                    }
                    Some(other) => {
                        return Err(AlsError::AlsSyntaxError {
                            position: self.position,
                            message: format!("Unknown escape sequence: \\{}", other),
                        });
                    }
                    None => {
                        return Err(AlsError::AlsSyntaxError {
                            position: start_pos,
                            message: "Incomplete escape sequence at end of input".to_string(),
                        });
                    }
                }
            } else {
                result.push(c);
            }
        }

        Ok(result)
    }

    /// Read an identifier (alphanumeric + underscore).
    fn read_identifier(&mut self) -> String {
        let mut result = String::new();

        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '_' || c == '.' {
                result.push(c);
                self.next_char();
            } else {
                break;
            }
        }

        result
    }

    /// Read a number (integer or float).
    fn read_number(&mut self, first_char: char) -> Result<Token> {
        let start_pos = self.position - first_char.len_utf8();
        let mut num_str = String::new();
        num_str.push(first_char);

        let mut has_dot = false;
        let mut has_exp = false;

        while let Some(c) = self.peek_char() {
            match c {
                '0'..='9' => {
                    num_str.push(c);
                    self.next_char();
                }
                '.' if !has_dot && !has_exp => {
                    has_dot = true;
                    num_str.push(c);
                    self.next_char();
                }
                'e' | 'E' if !has_exp => {
                    // Only treat as exponent if followed by digit or sign+digit
                    // We need to look ahead without consuming
                    let chars_vec: Vec<char> = self.input[self.position..].chars().take(3).collect();
                    if chars_vec.len() >= 2 {
                        let next_is_sign = chars_vec[1] == '+' || chars_vec[1] == '-';
                        let has_digit_after = if next_is_sign && chars_vec.len() >= 3 {
                            chars_vec[2].is_ascii_digit()
                        } else {
                            chars_vec[1].is_ascii_digit()
                        };
                        
                        if has_digit_after {
                            has_exp = true;
                            num_str.push(c);
                            self.next_char();
                            // Handle optional sign after exponent
                            if let Some(sign) = self.peek_char() {
                                if sign == '+' || sign == '-' {
                                    num_str.push(sign);
                                    self.next_char();
                                }
                            }
                        } else {
                            // Not a valid exponent, stop here
                            break;
                        }
                    } else {
                        // Not enough characters for valid exponent
                        break;
                    }
                }
                _ => break,
            }
        }

        if has_dot || has_exp {
            num_str
                .parse::<f64>()
                .map(Token::Float)
                .map_err(|_| AlsError::AlsSyntaxError {
                    position: start_pos,
                    message: format!("Invalid float: {}", num_str),
                })
        } else {
            num_str
                .parse::<i64>()
                .map(Token::Integer)
                .map_err(|_| AlsError::AlsSyntaxError {
                    position: start_pos,
                    message: format!("Invalid integer: {}", num_str),
                })
        }
    }

    /// Parse a version prefix (!v1 or !ctx).
    fn parse_version(&mut self) -> Result<Token> {
        let start_pos = self.position;
        
        // Read the rest of the version string
        let mut version_str = String::new();
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() {
                version_str.push(c);
                self.next_char();
            } else {
                break;
            }
        }

        if version_str == "ctx" {
            Ok(Token::Version(VersionType::Ctx))
        } else if version_str.starts_with('v') {
            let version_num = version_str[1..]
                .parse::<u8>()
                .map_err(|_| AlsError::AlsSyntaxError {
                    position: start_pos,
                    message: format!("Invalid version number: {}", version_str),
                })?;
            Ok(Token::Version(VersionType::Als(version_num)))
        } else {
            Err(AlsError::AlsSyntaxError {
                position: start_pos,
                message: format!("Invalid version prefix: !{}", version_str),
            })
        }
    }

    /// Parse a dictionary header ($name:val1|val2).
    fn parse_dictionary_header(&mut self) -> Result<Token> {
        let name = self.read_identifier();
        
        // Expect colon
        if self.peek_char() != Some(':') {
            return Err(AlsError::AlsSyntaxError {
                position: self.position,
                message: "Expected ':' after dictionary name".to_string(),
            });
        }
        self.next_char(); // consume ':'

        // Read values separated by |
        let mut values = Vec::new();
        loop {
            let value = self.read_escaped_value(&['|', '\n', '\r'])?;
            values.push(value);

            if self.peek_char() == Some('|') {
                self.next_char(); // consume '|'
            } else {
                break;
            }
        }

        Ok(Token::DictionaryHeader { name, values })
    }

    /// Parse a schema column (#column_name).
    fn parse_schema_column(&mut self) -> Result<Token> {
        let name = self.read_identifier();
        if name.is_empty() {
            // Read as escaped value if not a simple identifier
            let value = self.read_escaped_value(&[' ', '\t', '\n', '\r', '|'])?;
            Ok(Token::SchemaColumn(value))
        } else {
            Ok(Token::SchemaColumn(name))
        }
    }

    /// Parse a dictionary reference (_0, _1, etc.).
    fn parse_dict_ref(&mut self) -> Result<Token> {
        let start_pos = self.position;
        let mut num_str = String::new();

        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() {
                num_str.push(c);
                self.next_char();
            } else {
                break;
            }
        }

        if num_str.is_empty() {
            // Not a dict ref, treat underscore as part of a raw value
            return Ok(Token::RawValue("_".to_string()));
        }

        num_str
            .parse::<usize>()
            .map(Token::DictRef)
            .map_err(|_| AlsError::AlsSyntaxError {
                position: start_pos,
                message: format!("Invalid dictionary reference index: {}", num_str),
            })
    }

    /// Get the next token from the input.
    pub fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace();

        let c = match self.peek_char() {
            Some(c) => c,
            None => return Ok(Token::Eof),
        };

        match c {
            '!' => {
                self.next_char();
                self.parse_version()
            }
            '$' => {
                self.next_char();
                self.parse_dictionary_header()
            }
            '#' => {
                self.next_char();
                self.parse_schema_column()
            }
            '_' => {
                self.next_char();
                self.parse_dict_ref()
            }
            '>' => {
                self.next_char();
                Ok(Token::RangeOp)
            }
            '*' => {
                self.next_char();
                Ok(Token::MultiplyOp)
            }
            '~' => {
                self.next_char();
                Ok(Token::ToggleOp)
            }
            '|' => {
                self.next_char();
                self.in_header = false; // After first |, we're in streams
                Ok(Token::ColumnSeparator)
            }
            ':' => {
                self.next_char();
                Ok(Token::StepSeparator)
            }
            '(' => {
                self.next_char();
                Ok(Token::OpenParen)
            }
            ')' => {
                self.next_char();
                Ok(Token::CloseParen)
            }
            '\n' => {
                self.next_char();
                Ok(Token::Newline)
            }
            '-' | '0'..='9' => {
                self.next_char();
                self.read_number(c)
            }
            _ => {
                // Read as raw value
                let value = self.read_escaped_value(&[' ', '\t', '\n', '\r', '|', '>', '*', '~', ':', '(', ')'])?;
                if value.is_empty() {
                    // Skip and try again
                    self.next_char();
                    self.next_token()
                } else {
                    Ok(Token::RawValue(value))
                }
            }
        }
    }

    /// Peek at the next token without consuming it.
    pub fn peek_token(&mut self) -> Result<Token> {
        let saved_position = self.position;
        
        let token = self.next_token()?;
        
        // Restore state
        self.position = saved_position;
        self.chars = self.input.char_indices().peekable();
        // Advance to saved position
        while let Some((pos, _)) = self.chars.peek() {
            if *pos >= saved_position {
                break;
            }
            self.chars.next();
        }
        
        Ok(token)
    }

    /// Tokenize the entire input and return all tokens.
    pub fn tokenize_all(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token()?;
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }
        Ok(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_version_als() {
        let mut tokenizer = Tokenizer::new("!v1");
        assert_eq!(tokenizer.next_token().unwrap(), Token::Version(VersionType::Als(1)));
    }

    #[test]
    fn test_tokenize_version_ctx() {
        let mut tokenizer = Tokenizer::new("!ctx");
        assert_eq!(tokenizer.next_token().unwrap(), Token::Version(VersionType::Ctx));
    }

    #[test]
    fn test_tokenize_dictionary_header() {
        let mut tokenizer = Tokenizer::new("$colors:red|green|blue");
        let token = tokenizer.next_token().unwrap();
        assert_eq!(
            token,
            Token::DictionaryHeader {
                name: "colors".to_string(),
                values: vec!["red".to_string(), "green".to_string(), "blue".to_string()],
            }
        );
    }

    #[test]
    fn test_tokenize_schema_column() {
        let mut tokenizer = Tokenizer::new("#name #age #city");
        assert_eq!(tokenizer.next_token().unwrap(), Token::SchemaColumn("name".to_string()));
        assert_eq!(tokenizer.next_token().unwrap(), Token::SchemaColumn("age".to_string()));
        assert_eq!(tokenizer.next_token().unwrap(), Token::SchemaColumn("city".to_string()));
    }

    #[test]
    fn test_tokenize_dict_ref() {
        let mut tokenizer = Tokenizer::new("_0 _1 _42");
        assert_eq!(tokenizer.next_token().unwrap(), Token::DictRef(0));
        assert_eq!(tokenizer.next_token().unwrap(), Token::DictRef(1));
        assert_eq!(tokenizer.next_token().unwrap(), Token::DictRef(42));
    }

    #[test]
    fn test_tokenize_operators() {
        let mut tokenizer = Tokenizer::new("> * ~ | : ( )");
        assert_eq!(tokenizer.next_token().unwrap(), Token::RangeOp);
        assert_eq!(tokenizer.next_token().unwrap(), Token::MultiplyOp);
        assert_eq!(tokenizer.next_token().unwrap(), Token::ToggleOp);
        assert_eq!(tokenizer.next_token().unwrap(), Token::ColumnSeparator);
        assert_eq!(tokenizer.next_token().unwrap(), Token::StepSeparator);
        assert_eq!(tokenizer.next_token().unwrap(), Token::OpenParen);
        assert_eq!(tokenizer.next_token().unwrap(), Token::CloseParen);
    }

    #[test]
    fn test_tokenize_integers() {
        let mut tokenizer = Tokenizer::new("42 -17 0 999");
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(42));
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(-17));
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(0));
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(999));
    }

    #[test]
    fn test_tokenize_floats() {
        let mut tokenizer = Tokenizer::new("3.14 -2.5 1e10 2.5e-3");
        assert_eq!(tokenizer.next_token().unwrap(), Token::Float(3.14));
        assert_eq!(tokenizer.next_token().unwrap(), Token::Float(-2.5));
        assert_eq!(tokenizer.next_token().unwrap(), Token::Float(1e10));
        assert_eq!(tokenizer.next_token().unwrap(), Token::Float(2.5e-3));
    }

    #[test]
    fn test_tokenize_raw_values() {
        let mut tokenizer = Tokenizer::new("hello world");
        assert_eq!(tokenizer.next_token().unwrap(), Token::RawValue("hello".to_string()));
        assert_eq!(tokenizer.next_token().unwrap(), Token::RawValue("world".to_string()));
    }

    #[test]
    fn test_tokenize_escaped_values() {
        let mut tokenizer = Tokenizer::new("hello\\>world a\\*b");
        assert_eq!(tokenizer.next_token().unwrap(), Token::RawValue("hello>world".to_string()));
        assert_eq!(tokenizer.next_token().unwrap(), Token::RawValue("a*b".to_string()));
    }

    #[test]
    fn test_tokenize_range_expression() {
        let mut tokenizer = Tokenizer::new("1>5");
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(1));
        assert_eq!(tokenizer.next_token().unwrap(), Token::RangeOp);
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(5));
    }

    #[test]
    fn test_tokenize_range_with_step() {
        let mut tokenizer = Tokenizer::new("10>50:10");
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(10));
        assert_eq!(tokenizer.next_token().unwrap(), Token::RangeOp);
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(50));
        assert_eq!(tokenizer.next_token().unwrap(), Token::StepSeparator);
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(10));
    }

    #[test]
    fn test_tokenize_multiply_expression() {
        let mut tokenizer = Tokenizer::new("hello*3");
        assert_eq!(tokenizer.next_token().unwrap(), Token::RawValue("hello".to_string()));
        assert_eq!(tokenizer.next_token().unwrap(), Token::MultiplyOp);
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(3));
    }

    #[test]
    fn test_tokenize_toggle_expression() {
        let mut tokenizer = Tokenizer::new("T~F*4");
        assert_eq!(tokenizer.next_token().unwrap(), Token::RawValue("T".to_string()));
        assert_eq!(tokenizer.next_token().unwrap(), Token::ToggleOp);
        assert_eq!(tokenizer.next_token().unwrap(), Token::RawValue("F".to_string()));
        assert_eq!(tokenizer.next_token().unwrap(), Token::MultiplyOp);
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(4));
    }

    #[test]
    fn test_tokenize_complete_document() {
        let input = "!v1\n$d:a|b\n#col1 #col2\n1>3|_0 _1";
        let mut tokenizer = Tokenizer::new(input);
        
        assert_eq!(tokenizer.next_token().unwrap(), Token::Version(VersionType::Als(1)));
        assert_eq!(tokenizer.next_token().unwrap(), Token::Newline);
        assert!(matches!(tokenizer.next_token().unwrap(), Token::DictionaryHeader { .. }));
        assert_eq!(tokenizer.next_token().unwrap(), Token::Newline);
        assert_eq!(tokenizer.next_token().unwrap(), Token::SchemaColumn("col1".to_string()));
        assert_eq!(tokenizer.next_token().unwrap(), Token::SchemaColumn("col2".to_string()));
        assert_eq!(tokenizer.next_token().unwrap(), Token::Newline);
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(1));
        assert_eq!(tokenizer.next_token().unwrap(), Token::RangeOp);
        assert_eq!(tokenizer.next_token().unwrap(), Token::Integer(3));
        assert_eq!(tokenizer.next_token().unwrap(), Token::ColumnSeparator);
        assert_eq!(tokenizer.next_token().unwrap(), Token::DictRef(0));
        assert_eq!(tokenizer.next_token().unwrap(), Token::DictRef(1));
        assert_eq!(tokenizer.next_token().unwrap(), Token::Eof);
    }

    #[test]
    fn test_tokenize_eof() {
        let mut tokenizer = Tokenizer::new("");
        assert_eq!(tokenizer.next_token().unwrap(), Token::Eof);
    }

    #[test]
    fn test_invalid_escape_sequence() {
        let mut tokenizer = Tokenizer::new("hello\\x");
        let result = tokenizer.next_token();
        assert!(result.is_err());
    }

    #[test]
    fn test_incomplete_escape_sequence() {
        let mut tokenizer = Tokenizer::new("hello\\");
        let result = tokenizer.next_token();
        assert!(result.is_err());
    }
}
