#!/usr/bin/env python3
"""
Example usage of the ALS compression library Python bindings.

This script demonstrates basic compression and decompression operations
using the als_compression Python module.

To run this example:
1. Build the Python module: maturin develop --features python
2. Run the script: python examples/python_example.py
"""

def basic_csv_example():
    """Demonstrate basic CSV compression and decompression."""
    from als_compression import AlsCompressor, AlsParser
    
    print("=" * 60)
    print("Basic CSV Example")
    print("=" * 60)
    
    # Create compressor and parser
    compressor = AlsCompressor()
    parser = AlsParser()
    
    # Sample CSV data with patterns
    csv_data = """id,name,status
1,Alice,active
2,Bob,active
3,Charlie,active
4,David,active
5,Eve,active"""
    
    print("\nOriginal CSV:")
    print(csv_data)
    
    # Compress
    als = compressor.compress_csv(csv_data)
    print(f"\nCompressed ALS:")
    print(als)
    
    # Calculate compression ratio
    original_size = len(csv_data)
    compressed_size = len(als)
    ratio = original_size / compressed_size
    print(f"\nCompression ratio: {ratio:.2f}x")
    print(f"Original size: {original_size} bytes")
    print(f"Compressed size: {compressed_size} bytes")
    
    # Decompress
    csv_result = parser.to_csv(als)
    print(f"\nDecompressed CSV:")
    print(csv_result)
    
    # Verify round-trip
    assert csv_data.strip() == csv_result.strip(), "Round-trip failed!"
    print("\n✓ Round-trip successful!")


def json_example():
    """Demonstrate JSON compression and decompression."""
    from als_compression import AlsCompressor, AlsParser
    import json
    
    print("\n" + "=" * 60)
    print("JSON Example")
    print("=" * 60)
    
    compressor = AlsCompressor()
    parser = AlsParser()
    
    # Sample JSON data
    data = [
        {"id": 1, "name": "Alice", "score": 95},
        {"id": 2, "name": "Bob", "score": 87},
        {"id": 3, "name": "Charlie", "score": 92},
        {"id": 4, "name": "David", "score": 88},
        {"id": 5, "name": "Eve", "score": 91}
    ]
    json_data = json.dumps(data)
    
    print("\nOriginal JSON:")
    print(json.dumps(data, indent=2))
    
    # Compress
    als = compressor.compress_json(json_data)
    print(f"\nCompressed ALS:")
    print(als)
    
    # Decompress
    json_result = parser.to_json(als)
    result_data = json.loads(json_result)
    
    print(f"\nDecompressed JSON:")
    print(json.dumps(result_data, indent=2))
    
    # Verify round-trip
    assert data == result_data, "Round-trip failed!"
    print("\n✓ Round-trip successful!")


def dataframe_example():
    """Demonstrate pandas DataFrame compression."""
    try:
        import pandas as pd
        from als_compression import AlsCompressor, AlsParser
    except ImportError:
        print("\n" + "=" * 60)
        print("DataFrame Example (SKIPPED - pandas not installed)")
        print("=" * 60)
        return
    
    print("\n" + "=" * 60)
    print("DataFrame Example")
    print("=" * 60)
    
    compressor = AlsCompressor()
    parser = AlsParser()
    
    # Create a DataFrame with patterns
    df = pd.DataFrame({
        'id': range(1, 11),
        'category': ['A', 'B'] * 5,
        'value': [10, 20, 30, 40, 50] * 2
    })
    
    print("\nOriginal DataFrame:")
    print(df)
    
    # Compress
    als = compressor.compress_dataframe(df)
    print(f"\nCompressed ALS:")
    print(als)
    
    # Decompress
    df_result = parser.to_dataframe(als)
    print(f"\nDecompressed DataFrame:")
    print(df_result)
    
    # Verify round-trip
    pd.testing.assert_frame_equal(df, df_result)
    print("\n✓ Round-trip successful!")


def array_example():
    """Demonstrate numpy array compression."""
    try:
        import numpy as np
        from als_compression import AlsCompressor, AlsParser
    except ImportError:
        print("\n" + "=" * 60)
        print("Array Example (SKIPPED - numpy not installed)")
        print("=" * 60)
        return
    
    print("\n" + "=" * 60)
    print("Array Example")
    print("=" * 60)
    
    compressor = AlsCompressor()
    parser = AlsParser()
    
    # Create an array with patterns
    arr = np.array([
        [1, 10, 100],
        [2, 20, 200],
        [3, 30, 300],
        [4, 40, 400],
        [5, 50, 500]
    ])
    
    print("\nOriginal array:")
    print(arr)
    
    # Compress with column names
    als = compressor.compress_array(arr, column_names=['a', 'b', 'c'])
    print(f"\nCompressed ALS:")
    print(als)
    
    # Decompress
    arr_result = parser.to_array(als)
    print(f"\nDecompressed array:")
    print(arr_result)
    
    # Verify round-trip
    np.testing.assert_array_equal(arr, arr_result)
    print("\n✓ Round-trip successful!")


def custom_config_example():
    """Demonstrate custom configuration."""
    from als_compression import AlsCompressor, AlsParser
    
    print("\n" + "=" * 60)
    print("Custom Configuration Example")
    print("=" * 60)
    
    # Create compressor with custom settings
    compressor = AlsCompressor.with_config(
        ctx_fallback_threshold=1.5,
        min_pattern_length=4,
        parallelism=2
    )
    
    parser = AlsParser.with_config(parallelism=2)
    
    # Generate data with patterns
    csv_data = "id,value\n" + "\n".join(f"{i},{i*10}" for i in range(1, 101))
    
    print("\nCompressing 100 rows with custom config...")
    als = compressor.compress_csv(csv_data)
    
    print(f"Compressed size: {len(als)} bytes")
    print(f"First 100 characters: {als[:100]}...")
    
    # Decompress
    csv_result = parser.to_csv(als)
    
    # Verify
    assert csv_data.strip() == csv_result.strip(), "Round-trip failed!"
    print("\n✓ Round-trip successful with custom config!")


def main():
    """Run all examples."""
    print("\n" + "=" * 60)
    print("ALS Compression Library - Python Bindings Examples")
    print("=" * 60)
    
    try:
        basic_csv_example()
        json_example()
        dataframe_example()
        array_example()
        custom_config_example()
        
        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
