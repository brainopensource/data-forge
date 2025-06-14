import json
from collections import defaultdict
from pathlib import Path

def verify_duplicates(file_path: str = "external/mocked_response_duplicates.json"):
    # Load the JSON file
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    records = data.get('value', [])
    total_records = len(records)
    
    # Create composite keys and count occurrences
    composite_keys = defaultdict(list)
    for idx, record in enumerate(records):
        key = (
            record.get('field_code'),
            record.get('well_code'),
            record.get('production_period')
        )
        composite_keys[key].append(idx)
    
    # Count duplicates
    duplicates = {key: indices for key, indices in composite_keys.items() if len(indices) > 1}
    total_duplicates = sum(len(indices) - 1 for indices in duplicates.values())
    
    # Print results
    print(f"Total records in file: {total_records}")
    print(f"Unique records: {len(composite_keys)}")
    print(f"Total duplicates: {total_duplicates}")
    print(f"Records with duplicates: {len(duplicates)}")
    
    # Print some example duplicates
    if duplicates:
        print("\nExample duplicates:")
        for key, indices in list(duplicates.items())[:3]:
            print(f"\nComposite key: {key}")
            print(f"Found at indices: {indices}")
            for idx in indices:
                print(f"Record {idx}: {records[idx]}")

if __name__ == "__main__":
    verify_duplicates()
