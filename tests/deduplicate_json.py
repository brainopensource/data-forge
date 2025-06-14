#!/usr/bin/env python3
"""
Independent script to deduplicate JSON data using Polars.

This script reads a JSON file with the structure:
{
    "@odata.context": "...",
    "value": [array of records]
}

It removes duplicates based on key fields and saves the cleaned data.
"""

import json
import polars as pl
from pathlib import Path
import sys
import time


def load_json_data(file_path: Path) -> dict:
    """Load JSON data from file."""
    print(f"Loading data from {file_path}...")
    start_time = time.time()
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    load_time = time.time() - start_time
    print(f"✓ Data loaded in {load_time:.2f} seconds")
    return data


def deduplicate_data(data: dict) -> dict:
    """Remove duplicates from the data using Polars."""
    print("Starting deduplication process...")
    start_time = time.time()
    
    # Extract the records array
    records = data.get('value', [])
    original_count = len(records)
    print(f"Original record count: {original_count:,}")
    
    if not records:
        print("No records found in the data.")
        return data
    
    # Convert to Polars DataFrame
    print("Converting to Polars DataFrame...")
    df = pl.DataFrame(records)
    
    # Print column info
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {df.columns}")
    
    # Define key columns for deduplication
    # Using a combination of fields that should make each record unique
    key_columns = [
        'field_code',
        'well_code', 
        'production_period'
    ]
    
    # Check if key columns exist
    available_keys = [col for col in key_columns if col in df.columns]
    if not available_keys:
        print("Warning: None of the expected key columns found. Using all columns for deduplication.")
        available_keys = df.columns
    
    print(f"Using columns for deduplication: {available_keys}")
    
    # Remove duplicates
    print("Removing duplicates...")
    df_dedup = df.unique(subset=available_keys, keep='first')
    
    final_count = len(df_dedup)
    removed_count = original_count - final_count
    
    print(f"✓ Deduplication completed!")
    print(f"  - Original records: {original_count:,}")
    print(f"  - Final records: {final_count:,}")
    print(f"  - Duplicates removed: {removed_count:,}")
    print(f"  - Reduction: {(removed_count/original_count)*100:.2f}%")
    
    # Convert back to records
    dedup_records = df_dedup.to_dicts()
    
    # Create new data structure
    dedup_data = data.copy()
    dedup_data['value'] = dedup_records
    
    dedup_time = time.time() - start_time
    print(f"✓ Deduplication completed in {dedup_time:.2f} seconds")
    
    return dedup_data


def save_json_data(data: dict, file_path: Path):
    """Save data to JSON file."""
    print(f"Saving deduplicated data to {file_path}...")
    start_time = time.time()
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    save_time = time.time() - start_time
    print(f"✓ Data saved in {save_time:.2f} seconds")


def main():
    """Main execution function."""
    # File paths
    input_file = Path("external/mocked_response_100K.json")
    output_file = Path("external/mocked_response_100K_dedup.json")

    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file {input_file} not found!")
        sys.exit(1)
    
    try:
        # Load data
        data = load_json_data(input_file)
        
        # Deduplicate
        dedup_data = deduplicate_data(data)
        
        # Save results
        save_json_data(dedup_data, output_file)
        
        # Final summary
        print("\n" + "="*50)
        print("DEDUPLICATION COMPLETE!")
        print(f"Input file: {input_file}")
        print(f"Output file: {output_file}")
        print("="*50)
        
    except Exception as e:
        print(f"Error during processing: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
