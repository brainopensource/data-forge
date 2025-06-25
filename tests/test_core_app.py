import pytest
from fastapi.testclient import TestClient

def test_read_root(client: TestClient):
    """
    Test the root endpoint to ensure the API is running and returns basic info.
    """
    response = client.get("/")
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["message"] == "Data Forge API - Windows Ultra Performance Mode"
    assert "version" in json_response
    assert "platform" in json_response

def test_health_check(client: TestClient):
    """
    Test the health check endpoint to confirm the API is healthy.
    """
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_performance_info(client: TestClient):
    """
    Test the performance info endpoint to check Windows-specific configurations.
    """
    response = client.get("/performance")
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["performance_mode"] == "windows-ultra-fast"
    assert "optimizations" in json_response
    assert "windows_features" in json_response

def test_system_info(client: TestClient):
    """
    Test the system info endpoint to verify system details are exposed.
    """
    response = client.get("/system")
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["platform"] == "Windows"
    assert "system_info" in json_response
    assert "optimizations_applied" in json_response 