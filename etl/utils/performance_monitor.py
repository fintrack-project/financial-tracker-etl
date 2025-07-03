import time
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """
    Performance monitoring utility to track system performance metrics
    and provide insights for optimization.
    """
    
    def __init__(self):
        self.metrics = defaultdict(lambda: {
            'count': 0,
            'total_time': 0.0,
            'min_time': float('inf'),
            'max_time': 0.0,
            'recent_times': deque(maxlen=100),
            'errors': 0,
            'last_updated': None
        })
        self.lock = threading.Lock()
        self.start_time = time.time()
        
        logger.info("Performance monitor initialized")
    
    def start_timer(self, operation: str) -> str:
        """
        Start timing an operation.
        
        Args:
            operation: Name of the operation being timed
            
        Returns:
            Timer ID for stopping the timer
        """
        timer_id = f"{operation}_{int(time.time() * 1000)}"
        with self.lock:
            self.metrics[operation]['last_updated'] = datetime.now()
        return timer_id
    
    def stop_timer(self, timer_id: str, success: bool = True):
        """
        Stop timing an operation and record metrics.
        
        Args:
            timer_id: Timer ID from start_timer
            success: Whether the operation was successful
        """
        operation = timer_id.split('_')[0]
        duration = time.time() - (int(timer_id.split('_')[1]) / 1000)
        
        with self.lock:
            metric = self.metrics[operation]
            metric['count'] += 1
            metric['total_time'] += duration
            metric['min_time'] = min(metric['min_time'], duration)
            metric['max_time'] = max(metric['max_time'], duration)
            metric['recent_times'].append(duration)
            metric['last_updated'] = datetime.now()
            
            if not success:
                metric['errors'] += 1
    
    def get_metrics(self, operation: str = None) -> Dict[str, Any]:
        """
        Get performance metrics for an operation or all operations.
        
        Args:
            operation: Specific operation name, or None for all operations
            
        Returns:
            Dictionary containing performance metrics
        """
        with self.lock:
            if operation:
                return self._calculate_metrics(operation, self.metrics[operation])
            else:
                return {
                    op: self._calculate_metrics(op, metric)
                    for op, metric in self.metrics.items()
                }
    
    def _calculate_metrics(self, operation: str, metric: Dict) -> Dict[str, Any]:
        """Calculate derived metrics from raw data."""
        if metric['count'] == 0:
            return {
                'operation': operation,
                'count': 0,
                'avg_time': 0.0,
                'min_time': 0.0,
                'max_time': 0.0,
                'error_rate': 0.0,
                'recent_avg': 0.0,
                'last_updated': None
            }
        
        avg_time = metric['total_time'] / metric['count']
        error_rate = metric['errors'] / metric['count'] * 100
        
        # Calculate recent average (last 10 operations)
        recent_times = list(metric['recent_times'])[-10:]
        recent_avg = sum(recent_times) / len(recent_times) if recent_times else 0.0
        
        return {
            'operation': operation,
            'count': metric['count'],
            'avg_time': avg_time,
            'min_time': metric['min_time'] if metric['min_time'] != float('inf') else 0.0,
            'max_time': metric['max_time'],
            'error_rate': error_rate,
            'recent_avg': recent_avg,
            'last_updated': metric['last_updated']
        }
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get overall system performance statistics."""
        with self.lock:
            total_operations = sum(metric['count'] for metric in self.metrics.values())
            total_errors = sum(metric['errors'] for metric in self.metrics.values())
            total_time = sum(metric['total_time'] for metric in self.metrics.values())
            
            uptime = time.time() - self.start_time
            
            return {
                'uptime_seconds': uptime,
                'uptime_hours': uptime / 3600,
                'total_operations': total_operations,
                'total_errors': total_errors,
                'overall_error_rate': (total_errors / total_operations * 100) if total_operations > 0 else 0.0,
                'avg_operation_time': (total_time / total_operations) if total_operations > 0 else 0.0,
                'operations_per_second': total_operations / uptime if uptime > 0 else 0.0
            }
    
    def log_performance_summary(self):
        """Log a summary of current performance metrics."""
        system_stats = self.get_system_stats()
        metrics = self.get_metrics()
        
        logger.info("=== Performance Summary ===")
        logger.info(f"Uptime: {system_stats['uptime_hours']:.2f} hours")
        logger.info(f"Total Operations: {system_stats['total_operations']}")
        logger.info(f"Overall Error Rate: {system_stats['overall_error_rate']:.2f}%")
        logger.info(f"Avg Operation Time: {system_stats['avg_operation_time']:.3f}s")
        logger.info(f"Operations/Second: {system_stats['operations_per_second']:.2f}")
        
        logger.info("=== Operation Details ===")
        for operation, metric in metrics.items():
            logger.info(f"{operation}: "
                       f"count={metric['count']}, "
                       f"avg={metric['avg_time']:.3f}s, "
                       f"recent_avg={metric['recent_avg']:.3f}s, "
                       f"error_rate={metric['error_rate']:.1f}%")
    
    def reset_metrics(self, operation: str = None):
        """Reset metrics for an operation or all operations."""
        with self.lock:
            if operation:
                self.metrics[operation] = {
                    'count': 0,
                    'total_time': 0.0,
                    'min_time': float('inf'),
                    'max_time': 0.0,
                    'recent_times': deque(maxlen=100),
                    'errors': 0,
                    'last_updated': None
                }
                logger.info(f"Reset metrics for operation: {operation}")
            else:
                self.metrics.clear()
                logger.info("Reset all performance metrics")

class APIPerformanceTracker:
    """
    Specialized tracker for API performance and rate limiting.
    """
    
    def __init__(self):
        self.api_calls = defaultdict(lambda: {
            'total_calls': 0,
            'successful_calls': 0,
            'failed_calls': 0,
            'rate_limit_hits': 0,
            'total_wait_time': 0.0,
            'last_call_time': None,
            'response_times': deque(maxlen=100)
        })
        self.lock = threading.Lock()
    
    def record_api_call(self, api_name: str, success: bool, response_time: float, 
                       rate_limited: bool = False, wait_time: float = 0.0):
        """Record an API call with its performance metrics."""
        with self.lock:
            metric = self.api_calls[api_name]
            metric['total_calls'] += 1
            metric['response_times'].append(response_time)
            metric['last_call_time'] = datetime.now()
            metric['total_wait_time'] += wait_time
            
            if success:
                metric['successful_calls'] += 1
            else:
                metric['failed_calls'] += 1
                
            if rate_limited:
                metric['rate_limit_hits'] += 1
    
    def get_api_stats(self, api_name: str = None) -> Dict[str, Any]:
        """Get API performance statistics."""
        with self.lock:
            if api_name:
                return self._calculate_api_metrics(api_name, self.api_calls[api_name])
            else:
                return {
                    name: self._calculate_api_metrics(name, metric)
                    for name, metric in self.api_calls.items()
                }
    
    def _calculate_api_metrics(self, api_name: str, metric: Dict) -> Dict[str, Any]:
        """Calculate API performance metrics."""
        total_calls = metric['total_calls']
        if total_calls == 0:
            return {
                'api_name': api_name,
                'total_calls': 0,
                'success_rate': 0.0,
                'avg_response_time': 0.0,
                'rate_limit_hit_rate': 0.0,
                'avg_wait_time': 0.0
            }
        
        success_rate = (metric['successful_calls'] / total_calls) * 100
        rate_limit_hit_rate = (metric['rate_limit_hits'] / total_calls) * 100
        avg_response_time = sum(metric['response_times']) / len(metric['response_times'])
        avg_wait_time = metric['total_wait_time'] / total_calls
        
        return {
            'api_name': api_name,
            'total_calls': total_calls,
            'success_rate': success_rate,
            'avg_response_time': avg_response_time,
            'rate_limit_hit_rate': rate_limit_hit_rate,
            'avg_wait_time': avg_wait_time,
            'last_call_time': metric['last_call_time']
        }

# Global instances
performance_monitor = PerformanceMonitor()
api_tracker = APIPerformanceTracker() 