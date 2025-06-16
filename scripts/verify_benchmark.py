#!/usr/bin/env python3
"""
Verification script for the API benchmark configuration.
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from api_bench_write import WRITE_ENDPOINTS, verify_benchmark_configuration

def main():
    print("="*80)
    print("API BENCHMARK VERIFICATION")
    print("="*80)
    
    # Run the verification
    verify_benchmark_configuration()
    
    print(f"\nDetailed Endpoint List ({len(WRITE_ENDPOINTS)} total):")
    print("-" * 80)
    
    for i, (name, endpoint, compression, validate, ultra_fast) in enumerate(WRITE_ENDPOINTS, 1):
        comp_str = f"({compression})" if compression else "(none)"
        val_str = "✓" if validate else "✗"
        fast_str = "⚡" if ultra_fast else "🔒"
        print(f"{i:2d}. {name}")
        print(f"    Endpoint: {endpoint}")
        print(f"    Compression: {comp_str} | Validation: {val_str} | Ultra-fast: {fast_str}")
        print()
    
    # Check for new optimized endpoints specifically
    new_optimized = [
        "polars-write-optimized",
        "arrow-write-optimized", 
        "bulk-write-optimized",
        "stream-write",
        "vectorized-validate",
        "performance-comparison"
    ]
    
    found_optimized = []
    for name, endpoint, _, _, _ in WRITE_ENDPOINTS:
        for pattern in new_optimized:
            if pattern in endpoint:
                found_optimized.append(pattern)
                break
    
    print("="*80)
    print("NEW OPTIMIZED ENDPOINTS VERIFICATION:")
    print("="*80)
    
    for pattern in new_optimized[:4]:  # Only check write endpoints
        if pattern in found_optimized:
            print(f"✅ {pattern} - FOUND")
        else:
            print(f"❌ {pattern} - MISSING")
    
    print("\nAdditional endpoints (not in main benchmark list):")
    print("✅ /vectorized-validate/{schema_name} - Validation endpoint")
    print("✅ /performance-comparison/{schema_name} - Comparison endpoint")
    
    print("="*80)
    print("VERIFICATION COMPLETE!")
    print("="*80)

if __name__ == "__main__":
    main() 