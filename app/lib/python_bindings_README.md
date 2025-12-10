# Python Bindings for ALS Compression

This document describes how to build and use the Python bindings for the ALS compression library.

## Building the Python Module

The Python bindings are built using [maturin](https://github.com/PyO3/maturin), which handles the compilation and packaging of Rust code as a Python extension module.

### Prerequisites

1. Install Rust (if not already installed):
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. Install maturin:
   ```bash
   pip install maturin
   ```

### Building for Development

To build and install the module in development mode:

```bash
cd app/lib
maturin develop --features python
```

This will compile the Rust code and install the `als_compression` module in your current Python environment.

### Building a Wheel

To build a distributable wheel:

```bash
cd app/lib
maturin build --release --features python
```

The wheel will be created in `target/wheels/`.

## Usage Examples

### Basic Compression and Decompression

```python
from als_compression import AlsCompressor, AlsParser

# Create compressor and parser
compressor = AlsCompressor()
parser = AlsParser()

# Compress CSV data
csv_data = """id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35"""

als = compressor.compress_csv(csv_data)
print(f"Compressed: {als}")

# Decompress back to CSV
csv_result = parser.to_csv(als)
print(f"Decompressed: {csv_result}")

# Compress JSON data
json_data = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'
als_json = compressor.compress_json(json_data)
print(f"Compressed JSON: {als_json}")

# Decompress to JSON
json_result = parser.to_json(als_json)
print(f"Decompressed JSON: {json_result}")
```

### Working with pandas DataFrames

```python
import pandas as pd
from als_compression import AlsCompressor, AlsParser

# Create a DataFrame
df = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'score': [95, 87, 92, 88, 91]
})

# Compress DataFrame
compressor = AlsCompressor()
als = compressor.compress_dataframe(df)
print(f"Compressed DataFrame: {als}")

# Decompress back to DataFrame
parser = AlsParser()
df_result = parser.to_dataframe(als)
print(f"Decompressed DataFrame:\n{df_result}")
```

### Working with numpy Arrays

```python
import numpy as np
from als_compression import AlsCompressor, AlsParser

# Create a numpy array
arr = np.array([
    [1, 10, 100],
    [2, 20, 200],
    [3, 30, 300],
    [4, 40, 400]
])

# Compress array with column names
compressor = AlsCompressor()
als = compressor.compress_array(arr, column_names=['a', 'b', 'c'])
print(f"Compressed array: {als}")

# Decompress back to array
parser = AlsParser()
arr_result = parser.to_array(als)
print(f"Decompressed array:\n{arr_result}")
```

### Custom Configuration

```python
from als_compression import AlsCompressor

# Create compressor with custom settings
compressor = AlsCompressor.with_config(
    ctx_fallback_threshold=1.5,  # Higher threshold for CTX fallback
    min_pattern_length=4,         # Require longer patterns
    parallelism=4                 # Use 4 threads
)

csv_data = "id,value\n" + "\n".join(f"{i},{i*10}" for i in range(1, 1001))
als = compressor.compress_csv(csv_data)
print(f"Compressed with custom config: {als[:100]}...")
```

## API Reference

### AlsCompressor

Main class for compressing data to ALS format.

#### Methods

- `__init__()`: Create a new compressor with default configuration
- `with_config(ctx_fallback_threshold=None, min_pattern_length=None, parallelism=None)`: Create a compressor with custom configuration
- `compress_csv(csv_data: str) -> str`: Compress CSV data to ALS format
- `compress_json(json_data: str) -> str`: Compress JSON data to ALS format
- `compress_dataframe(dataframe: pd.DataFrame) -> str`: Compress a pandas DataFrame to ALS format
- `compress_array(array: np.ndarray, column_names: list[str] = None) -> str`: Compress a numpy array to ALS format

### AlsParser

Main class for parsing ALS format and converting to other formats.

#### Methods

- `__init__()`: Create a new parser with default configuration
- `with_config(parallelism=None)`: Create a parser with custom configuration
- `to_csv(als_data: str) -> str`: Parse ALS and convert to CSV
- `to_json(als_data: str) -> str`: Parse ALS and convert to JSON
- `to_dataframe(als_data: str) -> pd.DataFrame`: Parse ALS and convert to pandas DataFrame
- `to_array(als_data: str) -> np.ndarray`: Parse ALS and convert to numpy array

## Error Handling

The library raises Python exceptions for errors:

```python
from als_compression import AlsParser

parser = AlsParser()

try:
    result = parser.to_csv("invalid als format")
except ValueError as e:
    print(f"Parsing error: {e}")
except RuntimeError as e:
    print(f"Runtime error: {e}")
```

## Performance Tips

1. **Use parallelism for large datasets**: Set `parallelism` parameter when creating compressor/parser
2. **Reuse compressor/parser instances**: Creating instances is cheap, but reusing them avoids repeated initialization
3. **Use streaming for very large files**: For files larger than available RAM, consider processing in chunks

## Requirements

- Python 3.7 or later
- pandas (optional, for DataFrame support)
- numpy (optional, for array support)

## License

Apache-2.0
