#!/usr/bin/env python3
"""
Script to create a JSON file with duplicates for testing deduplication edge cases.

This script creates a test file with:
- 5,000 unique records
- 5,000 duplicates (one duplicate for each unique record)
- 1 additional duplicate of one unique record (making one record appear 3 times total)
- Total: 10,001 records
- Expected duplicates detected: 5,001 (5,000 + 1 extra)
"""

import json
import random
import copy
from pathlib import Path
from datetime import datetime, timedelta
import sys


def load_original_data(file_path: Path) -> dict:
    """Load the original JSON data."""
    print(f"Loading original data from {file_path}...")
    
    if not file_path.exists():
        print(f"Error: Original file {file_path} not found!")
        sys.exit(1)
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    original_records = data.get('value', [])
    print(f"✓ Loaded {len(original_records)} original records")
    return data


def generate_variations(base_record: dict, variations_count: int) -> list:
    """Generate variations of a base record to create unique records."""
    variations = []
    
    for i in range(variations_count):
        # Create a copy of the base record
        variation = copy.deepcopy(base_record)
        
        # Modify key fields to make it unique
        variation['field_code'] = random.randint(1, 10000)  # Increased range to avoid collisions
        variation['_field_name'] = f"Field_number_{variation['field_code']}"
        variation['well_code'] = random.randint(1000, 99999)  # Increased range
        variation['_well_reference'] = f"{random.randint(1,9)}-{chr(random.randint(65,90))}-{random.randint(1,9)}-BA"
        variation['well_name'] = variation['_well_reference']
        
        # Vary production period (random date between 1947 and 2023)
        start_date = datetime(1947, 1, 1)
        end_date = datetime(2023, 12, 31)
        random_date = start_date + timedelta(
            days=random.randint(0, (end_date - start_date).days)
        )
        variation['production_period'] = random_date.strftime("%Y-%m-01T00:00:00+00:00")
        
        # Vary production values
        variation['days_on_production'] = random.randint(1, 31)
        variation['oil_production_kbd'] = round(random.uniform(0, 10), 8)
        variation['gas_production_mmcfd'] = round(random.uniform(0, 100), 7)
        variation['liquids_production_kbd'] = round(random.uniform(0, 5), 8)
        variation['water_production_kbd'] = round(random.uniform(0, 20), 8)
        
        # Keep some fields the same for realism
        variation['data_source'] = base_record.get('data_source', 'Brazil - Agência Nacional do Petróleo (ANP)')
        variation['partition_0'] = 'latest'
        
        variations.append(variation)
    
    return variations


def create_exact_duplicates(unique_records: list) -> tuple:
    """Create exactly one duplicate for each unique record, plus one extra duplicate."""
    # Create one duplicate for each unique record
    duplicates = []
    for record in unique_records:
        duplicate = copy.deepcopy(record)
        duplicates.append(duplicate)
    
    # Create one additional duplicate of the first record (making it appear 3 times total)
    extra_duplicate = copy.deepcopy(unique_records[0])
    duplicates.append(extra_duplicate)
    
    return duplicates, unique_records[0]  # Return the record that appears 3 times


def main():
    """Main execution function."""
    # File paths
    input_file = Path("external/mocked_response.json")
    output_file = Path("external/mocked_response_duplicates.json")
    
    # Target counts
    target_unique = 5000
    target_duplicates = 5001  # 5000 + 1 extra
    target_total = 10001
    
    print("="*60)
    print("CREATING JSON FILE WITH EXACT DUPLICATES FOR EDGE CASE TESTING")
    print("="*60)
    print(f"Target unique records: {target_unique:,}")
    print(f"Target duplicate records: {target_duplicates:,}")
    print(f"Target total records: {target_total:,}")
    print(f"One record will appear 3 times (1 original + 2 duplicates)")
    print(f"All other records will appear 2 times (1 original + 1 duplicate)")
    print()
    
    # Load original data
    original_data = load_original_data(input_file)
    original_records = original_data.get('value', [])
    
    if not original_records:
        print("Error: No records found in original data!")
        sys.exit(1)
    
    # Generate exactly 5000 unique records
    print(f"Generating {target_unique:,} unique records...")
    unique_records = []
    
    # Use each original record as a template to generate variations
    records_per_template = target_unique // len(original_records)
    remaining_records = target_unique % len(original_records)
    
    for i, original_record in enumerate(original_records):
        # Generate variations for this template
        variations_count = records_per_template
        if i < remaining_records:
            variations_count += 1
        
        variations = generate_variations(original_record, variations_count)
        unique_records.extend(variations)
    
    # Ensure we have exactly 5000 unique records
    unique_records = unique_records[:target_unique]
    print(f"✓ Generated exactly {len(unique_records):,} unique records")
    
    # Create exact duplicates
    print(f"Creating {target_duplicates:,} duplicate records...")
    duplicate_records, triple_record = create_exact_duplicates(unique_records)
    print(f"✓ Created {len(duplicate_records):,} duplicate records")
    print(f"✓ Record with field_code={triple_record['field_code']}, well_code={triple_record['well_code']} will appear 3 times")
    
    # Combine and shuffle
    all_records = unique_records + duplicate_records
    random.shuffle(all_records)
    
    print(f"✓ Total records created: {len(all_records):,}")
    
    # Verify the counts
    from collections import defaultdict
    composite_keys = defaultdict(int)
    for record in all_records:
        key = (record['field_code'], record['well_code'], record['production_period'])
        composite_keys[key] += 1
    
    unique_count = len(composite_keys)
    records_appearing_twice = sum(1 for count in composite_keys.values() if count == 2)
    records_appearing_thrice = sum(1 for count in composite_keys.values() if count == 3)
    total_duplicates = sum(count - 1 for count in composite_keys.values())
    
    print(f"\nVerification:")
    print(f"✓ Unique composite keys: {unique_count:,}")
    print(f"✓ Records appearing exactly 2 times: {records_appearing_twice:,}")
    print(f"✓ Records appearing exactly 3 times: {records_appearing_thrice:,}")
    print(f"✓ Total duplicates that will be detected: {total_duplicates:,}")
    
    # Create output data structure
    output_data = copy.deepcopy(original_data)
    output_data['value'] = all_records
    
    # Save to file
    print(f"\nSaving data to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    # Calculate file size
    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    
    # Final summary
    print("\n" + "="*60)
    print("FILE CREATION COMPLETE!")
    print("="*60)
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"File size: {file_size_mb:.2f} MB")
    print(f"Total records: {len(all_records):,}")
    print(f"Unique records: {unique_count:,}")
    print(f"Expected duplicates detected: {total_duplicates:,}")
    print(f"Expected records inserted: {unique_count:,}")
    print("="*60)
    
    # Edge case summary
    print("\nEDGE CASE TEST SUMMARY:")
    print(f"✓ This file tests the edge case where one record appears 3 times")
    print(f"✓ Expected log: 'skipped {total_duplicates} duplicates'")
    print(f"✓ Expected log: '{unique_count} records inserted, {total_duplicates} duplicates removed'")
    print(f"✓ The difference ({total_duplicates} vs {total_duplicates-1}) tests the counting logic")


if __name__ == "__main__":
    main()
