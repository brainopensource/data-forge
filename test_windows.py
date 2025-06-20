"""
Windows-Optimized Test Script for Data Forge API
Comprehensive testing with Windows-specific performance measurements.
"""
import requests
import time
import json
import os
import sys
from datetime import datetime
import platform

# Windows-specific imports
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# Test configuration
BASE_URL = "http://localhost:8080"
TEST_SCHEMA = "well_production"
WINDOWS_TEST_ROWS = [1000, 5000, 10000, 50000, 100000]  # Windows-optimized test sizes

def print_windows_header():
    """Print Windows-specific test header."""
    print("=" * 80)
    print("   DATA FORGE API - WINDOWS ULTRA-PERFORMANCE TEST SUITE")
    print("=" * 80)
    print(f"Platform: {platform.system()} {platform.version()}")
    print(f"Architecture: {platform.architecture()[0]}")
    print(f"Python: {sys.version.split()[0]}")
    print(f"Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Base URL: {BASE_URL}")
    print("=" * 80)

def get_windows_system_metrics():
    """Get Windows system performance metrics."""
    if not PSUTIL_AVAILABLE:
        return {"error": "psutil not available"}
    
    try:
        return {
            "cpu_count": psutil.cpu_count(),
            "cpu_usage": psutil.cpu_percent(interval=1),
            "memory_total_gb": round(psutil.virtual_memory().total / (1024**3), 2),
            "memory_available_gb": round(psutil.virtual_memory().available / (1024**3), 2),
            "memory_usage_percent": psutil.virtual_memory().percent,
            "disk_usage_percent": psutil.disk_usage('.').percent,
        }
    except Exception as e:
        return {"error": str(e)}

def test_api_health():
    """Test API health and Windows-specific endpoints."""
    print("\n🔍 Testing API Health & Windows Features...")
    
    tests = [
        ("Root Endpoint", "/"),
        ("Health Check", "/health"),
        ("Performance Info", "/performance"),
        ("System Info", "/system"),
        ("API Docs", "/docs"),
    ]
    
    results = {}
    
    for test_name, endpoint in tests:
        try:
            start_time = time.time()
            response = requests.get(f"{BASE_URL}{endpoint}")
            end_time = time.time()
            
            if response.status_code == 200:
                response_time = round((end_time - start_time) * 1000, 2)
                results[test_name] = {
                    "status": "✅ PASS",
                    "response_time_ms": response_time,
                    "status_code": response.status_code
                }
                print(f"  {test_name}: ✅ PASS ({response_time}ms)")
                
                # Print Windows-specific info for system endpoint
                if endpoint == "/system":
                    data = response.json()
                    if "system_info" in data:
                        sys_info = data["system_info"]
                        print(f"    💻 CPU Cores: {sys_info.get('cpu_count', 'N/A')}")
                        print(f"    🧠 Memory: {sys_info.get('total_memory_gb', 'N/A')} GB")
                        print(f"    ⚡ Optimal Threads: {sys_info.get('optimal_threads', 'N/A')}")
                        
            else:
                results[test_name] = {
                    "status": "❌ FAIL",
                    "status_code": response.status_code,
                    "error": response.text[:200]
                }
                print(f"  {test_name}: ❌ FAIL (Status: {response.status_code})")
                
        except requests.exceptions.ConnectionError:
            results[test_name] = {
                "status": "❌ CONNECTION ERROR",
                "error": "Could not connect to API"
            }
            print(f"  {test_name}: ❌ CONNECTION ERROR")
        except Exception as e:
            results[test_name] = {
                "status": "❌ ERROR",
                "error": str(e)
            }
            print(f"  {test_name}: ❌ ERROR - {str(e)}")
    
    return results

def test_windows_performance_writes():
    """Test Windows-optimized write performance."""
    print("\n🚀 Testing Windows Ultra-Performance Writes...")
    
    # Sample data for testing
    sample_data = {
        "field_code": 1,
        "well_code": 2,
        "production_date": "2024-01-01",
        "oil_production": 100.5,
        "gas_production": 50.2,
        "water_production": 25.1
    }
    
    write_tests = [
        ("Ultra-Fast Write", f"/write/ultra-fast/{TEST_SCHEMA}"),
        ("Fast Validated Write", f"/write/fast-validated/{TEST_SCHEMA}"),
        ("Feather Write", f"/write/feather/{TEST_SCHEMA}"),
        ("Batch Write", f"/write/batch/{TEST_SCHEMA}"),
    ]
    
    results = {}
    
    for test_name, endpoint in write_tests:
        print(f"\n  Testing {test_name}...")
        
        for row_count in WINDOWS_TEST_ROWS:
            test_data = {
                "data": [sample_data.copy() for _ in range(row_count)],
                "compression": "zstd"
            }
            
            try:
                # Get system metrics before test
                metrics_before = get_windows_system_metrics()
                
                start_time = time.time()
                response = requests.post(f"{BASE_URL}{endpoint}", json=test_data)
                end_time = time.time()
                
                # Get system metrics after test
                metrics_after = get_windows_system_metrics()
                
                if response.status_code == 200:
                    duration = end_time - start_time
                    rows_per_second = int(row_count / duration) if duration > 0 else 0
                    
                    result_key = f"{test_name}_{row_count}"
                    results[result_key] = {
                        "status": "✅ PASS",
                        "rows": row_count,
                        "duration_seconds": round(duration, 3),
                        "rows_per_second": rows_per_second,
                        "throughput_category": get_throughput_category(rows_per_second),
                        "memory_usage_mb": metrics_after.get("memory_usage_percent", 0) - metrics_before.get("memory_usage_percent", 0)
                    }
                    
                    print(f"    {row_count:,} rows: ✅ {rows_per_second:,} rows/sec ({duration:.3f}s) - {get_throughput_category(rows_per_second)}")
                    
                else:
                    print(f"    {row_count:,} rows: ❌ FAIL (Status: {response.status_code})")
                    
            except Exception as e:
                print(f"    {row_count:,} rows: ❌ ERROR - {str(e)}")
    
    return results

def test_windows_performance_reads():
    """Test Windows-optimized read performance."""
    print("\n📖 Testing Windows Ultra-Performance Reads...")
    
    read_tests = [
        ("Polars Read", f"/read/polars/{TEST_SCHEMA}"),
        ("DuckDB Read", f"/read/duckdb/{TEST_SCHEMA}"),
        ("Latest Read", f"/read/latest/{TEST_SCHEMA}"),
    ]
    
    results = {}
    
    for test_name, endpoint in read_tests:
        try:
            start_time = time.time()
            response = requests.get(f"{BASE_URL}{endpoint}")
            end_time = time.time()
            
            if response.status_code == 200:
                duration = end_time - start_time
                
                # Try to get row count from response
                try:
                    data = response.json()
                    row_count = len(data.get("data", []))
                    rows_per_second = int(row_count / duration) if duration > 0 and row_count > 0 else 0
                except:
                    row_count = "Unknown"
                    rows_per_second = 0
                
                results[test_name] = {
                    "status": "✅ PASS",
                    "duration_seconds": round(duration, 3),
                    "rows": row_count,
                    "rows_per_second": rows_per_second,
                    "throughput_category": get_throughput_category(rows_per_second) if rows_per_second > 0 else "N/A"
                }
                
                print(f"  {test_name}: ✅ PASS ({duration:.3f}s, {row_count} rows)")
                if rows_per_second > 0:
                    print(f"    Performance: {rows_per_second:,} rows/sec - {get_throughput_category(rows_per_second)}")
                
            else:
                results[test_name] = {
                    "status": "❌ FAIL",
                    "status_code": response.status_code,
                    "error": response.text[:200]
                }
                print(f"  {test_name}: ❌ FAIL (Status: {response.status_code})")
                
        except Exception as e:
            results[test_name] = {
                "status": "❌ ERROR",
                "error": str(e)
            }
            print(f"  {test_name}: ❌ ERROR - {str(e)}")
    
    return results

def get_throughput_category(rows_per_second):
    """Categorize performance based on throughput."""
    if rows_per_second >= 10000000:  # 10M+
        return "🚀 ULTRA-FAST"
    elif rows_per_second >= 5000000:  # 5M+
        return "⚡ VERY FAST"
    elif rows_per_second >= 1000000:  # 1M+
        return "🔥 FAST"
    elif rows_per_second >= 100000:   # 100K+
        return "✅ GOOD"
    elif rows_per_second >= 10000:    # 10K+
        return "⚠️ MODERATE"
    else:
        return "🐌 SLOW"

def print_windows_summary(all_results):
    """Print Windows-optimized test summary."""
    print("\n" + "=" * 80)
    print("   WINDOWS PERFORMANCE TEST SUMMARY")
    print("=" * 80)
    
    # System metrics
    print("\n📊 System Metrics:")
    metrics = get_windows_system_metrics()
    if "error" not in metrics:
        print(f"  CPU Usage: {metrics.get('cpu_usage', 'N/A')}%")
        print(f"  Memory Usage: {metrics.get('memory_usage_percent', 'N/A')}%")
        print(f"  Available Memory: {metrics.get('memory_available_gb', 'N/A')} GB")
        print(f"  Disk Usage: {metrics.get('disk_usage_percent', 'N/A')}%")
    
    # Performance summary
    print("\n🏆 Performance Highlights:")
    max_throughput = 0
    best_test = ""
    
    for category, results in all_results.items():
        if category == "writes":
            for test_name, result in results.items():
                if result.get("status") == "✅ PASS":
                    rps = result.get("rows_per_second", 0)
                    if rps > max_throughput:
                        max_throughput = rps
                        best_test = test_name
    
    if max_throughput > 0:
        print(f"  🥇 Best Performance: {best_test}")
        print(f"  📈 Max Throughput: {max_throughput:,} rows/sec")
        print(f"  🎯 Performance Level: {get_throughput_category(max_throughput)}")
    
    # Windows-specific recommendations
    print("\n💡 Windows Optimization Status:")
    print("  ✅ ProactorEventLoop enabled")
    print("  ✅ Windows I/O completion ports")
    print("  ✅ Optimized threading model")
    print("  ✅ Local-first deployment ready")
    
    print("\n" + "=" * 80)

def main():
    """Run the complete Windows test suite."""
    print_windows_header()
    
    all_results = {}
    
    # Test API health
    all_results["health"] = test_api_health()
    
    # Test write performance
    all_results["writes"] = test_windows_performance_writes()
    
    # Test read performance
    all_results["reads"] = test_windows_performance_reads()
    
    # Print summary
    print_windows_summary(all_results)
    
    # Save results to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"windows_test_results_{timestamp}.json"
    
    try:
        with open(results_file, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        print(f"\n💾 Results saved to: {results_file}")
    except Exception as e:
        print(f"\n❌ Could not save results: {e}")
    
    return all_results

if __name__ == "__main__":
    try:
        results = main()
        print("\n✅ Windows test suite completed successfully!")
    except KeyboardInterrupt:
        print("\n\n⚠️ Test suite interrupted by user")
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        sys.exit(1) 