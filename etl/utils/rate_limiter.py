import time
import threading
from collections import deque
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class APIRateLimiter:
    """
    Intelligent rate limiter for free API tiers that optimizes request timing
    while respecting rate limits.
    """
    
    def __init__(self, max_requests_per_minute=8, burst_limit=3):
        self.max_requests_per_minute = max_requests_per_minute
        self.burst_limit = burst_limit
        self.request_times = deque()
        self.lock = threading.Lock()
        
        # Calculate optimal request intervals
        self.min_interval = 60.0 / max_requests_per_minute  # Minimum seconds between requests
        self.burst_interval = 60.0 / (max_requests_per_minute * 2)  # Faster for bursts
        
        logger.info(f"Rate limiter initialized: {max_requests_per_minute} req/min, "
                   f"min_interval={self.min_interval:.2f}s, burst_interval={self.burst_interval:.2f}s")
    
    def wait_if_needed(self, priority="normal"):
        """
        Wait if necessary to respect rate limits.
        
        Args:
            priority: "high", "normal", "low" - affects wait strategy
        """
        with self.lock:
            now = time.time()
            
            # Clean old requests (older than 1 minute)
            while self.request_times and now - self.request_times[0] > 60:
                self.request_times.popleft()
            
            # Check if we can make a request immediately
            if len(self.request_times) < self.burst_limit:
                self.request_times.append(now)
                return 0  # No wait needed
            
            # Calculate wait time based on priority and current load
            if priority == "high":
                # High priority: wait minimum time
                wait_time = max(0, self.min_interval - (now - self.request_times[-1]))
            elif priority == "low":
                # Low priority: wait longer to allow high priority requests
                wait_time = max(0, self.min_interval * 1.5 - (now - self.request_times[-1]))
            else:
                # Normal priority: adaptive wait
                recent_requests = len([t for t in self.request_times if now - t < 30])
                if recent_requests >= self.max_requests_per_minute // 2:
                    wait_time = self.min_interval
                else:
                    wait_time = max(0, self.burst_interval - (now - self.request_times[-1]))
            
            if wait_time > 0:
                logger.debug(f"Rate limit: waiting {wait_time:.2f}s (priority: {priority}, "
                           f"recent requests: {len(self.request_times)})")
                time.sleep(wait_time)
            
            self.request_times.append(time.time())
            return wait_time
    
    def get_stats(self):
        """Get current rate limiting statistics."""
        with self.lock:
            now = time.time()
            recent_requests = len([t for t in self.request_times if now - t < 60])
            return {
                "recent_requests": recent_requests,
                "max_requests_per_minute": self.max_requests_per_minute,
                "available_requests": max(0, self.max_requests_per_minute - recent_requests),
                "utilization_percent": (recent_requests / self.max_requests_per_minute) * 100
            }

class AdaptiveRetryStrategy:
    """
    Adaptive retry strategy that learns from API responses and adjusts timing.
    """
    
    def __init__(self, base_delay=1.0, max_delay=60.0, max_retries=3):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.error_counts = {}  # Track errors by type
        self.success_times = deque(maxlen=100)  # Track successful request times
    
    def get_retry_delay(self, attempt, error_type=None):
        """
        Calculate adaptive retry delay based on attempt number and error type.
        
        Args:
            attempt: Current retry attempt (1-based)
            error_type: Type of error ("rate_limit", "timeout", "server_error", etc.)
        
        Returns:
            Delay in seconds before next retry
        """
        if attempt > self.max_retries:
            return 0  # No more retries
        
        # Base exponential backoff
        delay = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
        
        # Adjust based on error type
        if error_type == "rate_limit":
            # Rate limit errors: wait longer
            delay = max(delay, 60.0)  # Minimum 60 seconds for rate limits
        elif error_type == "timeout":
            # Timeout errors: moderate increase
            delay = delay * 1.5
        elif error_type == "server_error":
            # Server errors: longer delay
            delay = delay * 2.0
        
        # Adjust based on recent success rate
        if len(self.success_times) > 10:
            recent_success_rate = len([t for t in self.success_times 
                                     if time.time() - t < 300]) / len(self.success_times)
            if recent_success_rate < 0.5:
                delay = delay * 1.5  # Increase delay if success rate is low
        
        logger.debug(f"Adaptive retry delay: attempt={attempt}, error_type={error_type}, "
                    f"delay={delay:.2f}s")
        return delay
    
    def record_success(self):
        """Record a successful request."""
        self.success_times.append(time.time())
    
    def record_error(self, error_type):
        """Record an error for adaptive learning."""
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1

# Global rate limiter instance
rate_limiter = APIRateLimiter(max_requests_per_minute=8, burst_limit=3)
retry_strategy = AdaptiveRetryStrategy(base_delay=2.0, max_delay=120.0, max_retries=3) 