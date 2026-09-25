"""Unit tests for controllers."""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock

from frontend.controllers.main_window_controller import MainWindowController
from frontend.controllers.ui_controller import UIController
from frontend.utils.error_handler import ErrorHandler


class TestMainWindowController:
    """Test cases for MainWindowController."""
    
    @pytest.fixture
    def mock_root(self):
        """Create mock root window."""
        mock_root = Mock()
        mock_root.title = Mock()
        mock_root.geometry = Mock()
        mock_root.minsize = Mock()
        mock_root.iconbitmap = Mock()
        mock_root.protocol = Mock()
        return mock_root
    
    @pytest.fixture
    def controller(self, mock_root):
        """Create controller with mocked dependencies."""
        return MainWindowController(mock_root, "Test App")
    
    def test_controller_creation(self, controller):
        """Test controller can be created."""
        assert controller is not None
        assert controller.title == "Test App"
        assert controller.default_geometry == "1400x900"
    
    def test_setup_window(self, controller, mock_root):
        """Test window setup."""
        controller.setup_window()
        
        # Verify basic setup calls were made
        mock_root.title.assert_called_once_with("Test App")
        mock_root.geometry.assert_called_once_with("1400x900")
    
    def test_center_window(self, controller, mock_root):
        """Test window centering."""
        # Mock screen dimensions
        mock_root.winfo_screenwidth.return_value = 1920
        mock_root.winfo_screenheight.return_value = 1080
        
        controller.center_window()
        
        # Should call geometry with centered position
        assert mock_root.geometry.called
    
    def test_set_close_callback(self, controller):
        """Test setting close callback."""
        callback = Mock()
        controller.set_close_callback(callback)
        assert controller._close_callback == callback


class TestUIController:
    """Test cases for UIController."""
    
    @pytest.fixture
    def mock_app(self):
        """Create mock app instance."""
        mock_app = Mock()
        mock_app.status = Mock()
        return mock_app
    
    @pytest.fixture
    def controller(self, mock_app):
        """Create controller with mocked dependencies."""
        return UIController(mock_app)
    
    def test_controller_creation(self, controller):
        """Test controller can be created."""
        assert controller is not None
        assert controller.status_text == "Ready"
        assert controller.progress_value == 0.0
    
    def test_update_status(self, controller, mock_app):
        """Test status update."""
        controller.update_status("Test message")
        assert controller.status_text == "Test message"
    
    def test_update_progress(self, controller):
        """Test progress update."""
        controller.update_progress(0.5, "Half complete")
        assert controller.progress_value == 0.5
    
    def test_show_message(self, controller):
        """Test message display."""
        # This should not raise an exception
        controller.show_message("Test title", "Test message")
    
    def test_show_error(self, controller):
        """Test error display."""
        # This should not raise an exception
        controller.show_error("Test error")
    
    def test_ask_confirmation(self, controller):
        """Test confirmation dialog."""
        # This should not raise an exception
        result = controller.ask_confirmation("Test question")
        assert isinstance(result, bool)


if __name__ == "__main__":
    pytest.main([__file__])
