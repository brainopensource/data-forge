"""
Tests for the professional startup and health monitoring system.
Demonstrates how the modular approach makes testing easier.
"""
import pytest
import asyncio
from unittest.mock import patch, MagicMock

from app.core.startup import StartupManager, startup_manager, initialize_for_testing
from app.core.health import HealthMonitor, health_monitor


class TestStartupManager:
    """Test the professional startup manager."""
    
    @pytest.fixture
    def manager(self):
        """Create a fresh startup manager for testing."""
        return StartupManager()
    
    async def test_startup_manager_initialization(self, manager):
        """Test that startup manager initializes properly."""
        assert manager.initialization_status == {}
        assert manager.startup_metrics == {}
    
    async def test_directory_initialization(self, manager):
        """Test directory initialization step."""
        with patch('app.core.startup.ensure_directories') as mock_ensure_dirs:
            await manager._initialize_directories()
            
            mock_ensure_dirs.assert_called_once()
            assert manager.initialization_status['directories'] is True
            assert 'directory_init_time' in manager.startup_metrics
    
    async def test_async_environment_configuration(self, manager):
        """Test async environment configuration."""
        with patch('asyncio.set_event_loop_policy') as mock_set_policy:
            await manager._configure_async_environment()
            
            # Should attempt to set Windows policy
            mock_set_policy.assert_called_once()
            assert manager.initialization_status['async_optimization'] is True
            assert 'async_config_time' in manager.startup_metrics
    
    async def test_memory_optimization_configuration(self, manager):
        """Test memory optimization configuration."""
        with patch('pyarrow.set_memory_pool') as mock_set_pool:
            await manager._configure_memory_optimization()
            
            mock_set_pool.assert_called_once()
            assert manager.initialization_status['arrow_memory'] is True
            assert 'memory_config_time' in manager.startup_metrics
    
    async def test_libraries_configuration(self, manager):
        """Test libraries configuration."""
        with patch('app.core.startup.apply_windows_optimizations') as mock_apply:
            await manager._configure_libraries()
            
            mock_apply.assert_called_once()
            assert manager.initialization_status['polars_optimization'] is True
            assert 'library_config_time' in manager.startup_metrics
    
    async def test_database_configuration(self, manager):
        """Test database configuration."""
        with patch('duckdb.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            await manager._configure_database()
            
            mock_connect.assert_called_once_with(":memory:")
            assert manager.initialization_status['duckdb_optimization'] is True
            assert 'database_config_time' in manager.startup_metrics
    
    async def test_full_initialization_success(self, manager):
        """Test full initialization process."""
        with patch.multiple(
            'app.core.startup',
            ensure_directories=MagicMock(),
            apply_windows_optimizations=MagicMock(),
        ), patch('asyncio.set_event_loop_policy'), \
           patch('pyarrow.set_memory_pool'), \
           patch('duckdb.connect'):
            
            result = await manager.initialize_application()
            
            assert result['status'] == 'success'
            assert 'metrics' in result
            assert 'initialization_status' in result
            assert 'total_initialization_time' in result['metrics']
    
    async def test_initialization_failure_handling(self, manager):
        """Test that initialization failures are handled properly."""
        with patch('app.core.startup.ensure_directories', side_effect=Exception("Test error")):
            result = await manager.initialize_application()
            
            assert result['status'] == 'failed'
            assert 'error' in result
            assert 'partial_initialization' in result


class TestHealthMonitor:
    """Test the health monitoring system."""
    
    @pytest.fixture
    def monitor(self):
        """Create a fresh health monitor for testing."""
        return HealthMonitor()
    
    async def test_health_monitor_initialization(self, monitor):
        """Test that health monitor initializes properly."""
        assert hasattr(monitor, 'start_time')
        assert hasattr(monitor, 'health_checks')
    
    async def test_quick_health_check(self, monitor):
        """Test quick health check functionality."""
        with patch('psutil.cpu_percent', return_value=50.0), \
             patch('psutil.virtual_memory') as mock_memory:
            
            mock_memory.return_value.percent = 60.0
            
            result = await monitor.get_quick_health()
            
            assert result['status'] == 'healthy'
            assert 'cpu_usage_percent' in result
            assert 'memory_usage_percent' in result
            assert 'uptime_seconds' in result
    
    async def test_system_resources_check(self, monitor):
        """Test system resources health check."""
        with patch('psutil.cpu_percent', return_value=50.0), \
             patch('psutil.virtual_memory') as mock_memory:
            
            mock_memory.return_value.percent = 60.0
            mock_memory.return_value.available = 8 * 1024**3  # 8GB
            
            result = await monitor._check_system_resources()
            
            assert result['healthy'] is True
            assert result['cpu_usage_percent'] == 50.0
            assert result['memory_usage_percent'] == 60.0
    
    async def test_library_status_check(self, monitor):
        """Test library status health check."""
        # This test will use real libraries since they should be available
        result = await monitor._check_library_status()
        
        assert 'healthy' in result
        assert 'libraries' in result
        assert 'polars' in result['libraries']
        assert 'pyarrow' in result['libraries']
        assert 'duckdb' in result['libraries']
    
    async def test_comprehensive_health_check(self, monitor):
        """Test comprehensive health check."""
        with patch.object(monitor, '_check_system_resources', return_value={'healthy': True}), \
             patch.object(monitor, '_check_disk_space', return_value={'healthy': True}), \
             patch.object(monitor, '_check_memory_usage', return_value={'healthy': True}), \
             patch.object(monitor, '_check_library_status', return_value={'healthy': True}), \
             patch.object(monitor, '_check_database_connectivity', return_value={'healthy': True}), \
             patch.object(monitor, '_check_startup_status', return_value={'healthy': True}):
            
            result = await monitor.get_comprehensive_health()
            
            assert result['status'] == 'healthy'
            assert 'checks' in result
            assert len(result['checks']) == 6  # All health checks


class TestIntegration:
    """Integration tests for the startup and health systems."""
    
    async def test_startup_and_health_integration(self):
        """Test that startup manager and health monitor work together."""
        # This test demonstrates the integration between systems
        
        # Use the convenience function for testing
        with patch.multiple(
            'app.core.startup',
            ensure_directories=MagicMock(),
            apply_windows_optimizations=MagicMock(),
        ), patch('asyncio.set_event_loop_policy'), \
           patch('pyarrow.set_memory_pool'), \
           patch('duckdb.connect'):
            
            startup_result = await initialize_for_testing()
            
            # Should have successful startup
            assert startup_result['status'] == 'success'
            
            # Health monitor should be able to check startup status
            health_result = await health_monitor.get_comprehensive_health()
            
            assert 'checks' in health_result
            assert 'startup_status' in health_result['checks']


if __name__ == "__main__":
    # Run a simple demonstration
    async def demonstrate_new_architecture():
        """Demonstrate the new professional architecture."""
        print("🚀 Demonstrating Professional Startup Architecture")
        print("=" * 50)
        
        # Show startup process
        print("1. Testing Startup Manager...")
        manager = StartupManager()
        
        with patch.multiple(
            'app.core.startup',
            ensure_directories=MagicMock(),
            apply_windows_optimizations=MagicMock(),
        ), patch('asyncio.set_event_loop_policy'), \
           patch('pyarrow.set_memory_pool'), \
           patch('duckdb.connect'):
            
            result = await manager.initialize_application()
            print(f"   ✅ Startup Result: {result['status']}")
            print(f"   📊 Initialization Time: {result['metrics'].get('total_initialization_time', 0):.3f}s")
        
        # Show health monitoring
        print("\n2. Testing Health Monitor...")
        monitor = HealthMonitor()
        
        health = await monitor.get_quick_health()
        print(f"   ✅ Health Status: {health['status']}")
        print(f"   ⏱️  Uptime: {health['uptime_seconds']:.1f}s")
        
        print("\n🎉 Professional Architecture Demonstration Complete!")
        print("\n📋 Key Benefits:")
        print("   • Modular and testable components")
        print("   • Comprehensive error handling")
        print("   • Detailed metrics and monitoring")
        print("   • Separation of concerns")
        print("   • Easy to extend and maintain")
    
    # Run the demonstration
    asyncio.run(demonstrate_new_architecture()) 