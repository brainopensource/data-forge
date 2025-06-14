from pydantic import BaseModel

class APILimits(BaseModel):
    """Configuration for API limits and constraints - Optimized for PERFORMANCE TESTING"""
   
    # System resource limits
    MAX_MEMORY_BUFFER_MB: int = 8192    # 8GB buffer for large operations
    PARALLEL_WORKER_THREADS: int = 4    # Increased for better concurrency

# Global instance
api_limits = APILimits() 