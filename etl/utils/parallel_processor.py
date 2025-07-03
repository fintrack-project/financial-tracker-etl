import asyncio
import concurrent.futures
import threading
import time
from typing import List, Dict, Any, Callable
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class ParallelProcessor:
    """
    Parallel processor that respects API rate limits while maximizing throughput.
    Uses intelligent batching and priority queuing for optimal performance.
    """
    
    def __init__(self, max_workers=3, rate_limit_per_minute=8):
        self.max_workers = max_workers
        self.rate_limit_per_minute = rate_limit_per_minute
        self.request_semaphore = asyncio.Semaphore(max_workers)
        self.rate_limit_semaphore = asyncio.Semaphore(rate_limit_per_minute)
        self.priority_queues = {
            'high': [],
            'normal': [],
            'low': []
        }
        self.lock = threading.Lock()
        
        logger.info(f"Parallel processor initialized: {max_workers} workers, "
                   f"{rate_limit_per_minute} req/min rate limit")
    
    async def process_assets_parallel(self, assets: List[Dict[str, Any]], 
                                    fetch_function: Callable,
                                    priority: str = "normal") -> List[Dict[str, Any]]:
        """
        Process assets in parallel while respecting rate limits.
        
        Args:
            assets: List of assets to process
            fetch_function: Function to fetch data for each asset
            priority: Priority level for this batch
        
        Returns:
            List of processed results
        """
        if not assets:
            return []
        
        logger.info(f"Processing {len(assets)} assets in parallel with priority: {priority}")
        
        # Group assets by priority for optimal processing
        tasks = []
        for asset in assets:
            asset_priority = asset.get('priority', priority)
            task = self._create_fetch_task(asset, fetch_function, asset_priority)
            tasks.append(task)
        
        # Execute tasks with rate limiting
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and return successful results
        successful_results = []
        failed_assets = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_assets.append(assets[i])
                logger.error(f"Failed to process asset {assets[i]}: {result}")
            else:
                successful_results.append(result)
        
        if failed_assets:
            logger.warning(f"{len(failed_assets)} assets failed out of {len(assets)}")
        
        return successful_results
    
    async def _create_fetch_task(self, asset: Dict[str, Any], 
                                fetch_function: Callable, 
                                priority: str) -> Dict[str, Any]:
        """
        Create an async task for fetching asset data with rate limiting.
        """
        async with self.request_semaphore:
            # Apply rate limiting
            await self._rate_limit_wait(priority)
            
            try:
                # Execute the fetch function
                result = await asyncio.get_event_loop().run_in_executor(
                    None, fetch_function, asset
                )
                return result
            except Exception as e:
                logger.error(f"Error fetching data for asset {asset}: {e}")
                raise
    
    async def _rate_limit_wait(self, priority: str):
        """
        Wait if necessary to respect rate limits.
        """
        async with self.rate_limit_semaphore:
            # Calculate wait time based on priority
            if priority == "high":
                wait_time = 0.1  # Minimal wait for high priority
            elif priority == "low":
                wait_time = 1.0  # Longer wait for low priority
            else:
                wait_time = 0.5  # Normal wait
            
            if wait_time > 0:
                await asyncio.sleep(wait_time)
    
    def process_assets_batched(self, assets: List[Dict[str, Any]], 
                             fetch_function: Callable,
                             batch_size: int = 10) -> List[Dict[str, Any]]:
        """
        Process assets in batches to optimize API usage.
        
        Args:
            assets: List of assets to process
            fetch_function: Function to fetch data for each asset
            batch_size: Number of assets to process in each batch
        
        Returns:
            List of processed results
        """
        all_results = []
        
        for i in range(0, len(assets), batch_size):
            batch = assets[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(assets) + batch_size - 1)//batch_size}")
            
            # Process batch with rate limiting
            batch_results = self._process_batch_sync(batch, fetch_function)
            all_results.extend(batch_results)
            
            # Small delay between batches to respect rate limits
            if i + batch_size < len(assets):
                time.sleep(0.5)
        
        return all_results
    
    def _process_batch_sync(self, batch: List[Dict[str, Any]], 
                           fetch_function: Callable) -> List[Dict[str, Any]]:
        """
        Process a batch of assets synchronously with limited concurrency.
        """
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_asset = {
                executor.submit(fetch_function, asset): asset 
                for asset in batch
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_asset):
                asset = future_to_asset[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error processing asset {asset}: {e}")
        
        return results

class PriorityQueue:
    """
    Priority queue for managing asset processing order.
    """
    
    def __init__(self):
        self.queues = {
            'high': [],
            'normal': [],
            'low': []
        }
        self.lock = threading.Lock()
    
    def add_asset(self, asset: Dict[str, Any], priority: str = "normal"):
        """Add an asset to the appropriate priority queue."""
        with self.lock:
            self.queues[priority].append(asset)
    
    def get_next_batch(self, batch_size: int = 10) -> List[Dict[str, Any]]:
        """Get the next batch of assets, prioritizing high priority items."""
        with self.lock:
            batch = []
            
            # First, get high priority items
            while len(batch) < batch_size and self.queues['high']:
                batch.append(self.queues['high'].pop(0))
            
            # Then, get normal priority items
            while len(batch) < batch_size and self.queues['normal']:
                batch.append(self.queues['normal'].pop(0))
            
            # Finally, get low priority items
            while len(batch) < batch_size and self.queues['low']:
                batch.append(self.queues['low'].pop(0))
            
            return batch
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Get statistics about queue lengths."""
        with self.lock:
            return {
                'high': len(self.queues['high']),
                'normal': len(self.queues['normal']),
                'low': len(self.queues['low'])
            }

# Global instances
parallel_processor = ParallelProcessor(max_workers=3, rate_limit_per_minute=8)
priority_queue = PriorityQueue() 