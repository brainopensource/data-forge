import polars as pl
import json
from pathlib import Path
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any
from datetime import datetime, timedelta


json_path = Path(__file__).parent.parent / "external" / "mocked_response_10K.json"
parquet_path = Path(__file__).parent.parent / "data" / "well_production_data_100K.parquet"

SCHEMA_NAME = "well_production"  # Change as needed
N_ROWS = 100000  # 100 thousand

def generate_test_data(size: int) -> List[Dict[str, Any]]:
    """Generate test data with unique composite primary keys."""
    data = []
    base_date = datetime(2010, 1, 1)

    for i in range(size):
        prod_date = base_date + timedelta(hours=i)
        record = {
            "id": str(uuid.uuid4()),
            "created_at": datetime.now(),
            "version": 1,
            "field_code": i % 1000,
            "_field_name": f"Field_{i % 1000}",
            "well_code": i % 100,
            "_well_reference": f"WELL_REF_{i % 100:03d}",
            "well_name": f"Well_{i % 100}",
            "production_period": prod_date.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "days_on_production": 30,
            "oil_production_kbd": round(100.0 + (i * 0.1), 2),
            "gas_production_mmcfd": round(50.0 + (i * 0.05), 2),
            "liquids_production_kbd": round(25.0 + (i * 0.025), 2),
            "water_production_kbd": round(75.0 + (i * 0.075), 2),
            "data_source": "performance_test",
            "source_data": json.dumps({"test": f"data_{i}"}),
            "partition_0": f"partition_{i % 10}"
        }
        data.append(record)
    return data


def main():
    # Generate a large dataset for stress testing

    print(f"Generating {N_ROWS:,} records.")
    records = generate_test_data(N_ROWS)
    df = pl.DataFrame(records)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(parquet_path)
    print(f"Saved {len(df)} records to {parquet_path}")

if __name__ == "__main__":
    main()
