import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from concurrent.futures import ThreadPoolExecutor, as_completed
import itertools
from typing import List, Tuple, Dict, Optional
from datetime import datetime, UTC
from backend.utils.job_search import process_job_search
from backend.utils.logger import logger
import time

#!/usr/bin/env python3

# Configuration
JOB_TITLES = [
    "AI Engineer",
    "Applied Data Scientist",
    "Machine Learning Engineer",
    "Applied Data Scientist",
    "Prompt Engineer",
    "NLP Engineer",
]

LOCATIONS = [
    "Palo Alto CA",
    "Mountain View CA",
    "San Jose Santa Clara CA",
    "San Francisco CA",
    "Remote",
]

MAX_WORKERS = 3  # Conservative number of concurrent threads
RATE_LIMIT = 1.0  # More conservative rate limit (1 second)
MAX_PAGE_DEPTH = 3  # Consistent with previous testing

# Add the project root to the Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

def run_search(args: Tuple[str, str]) -> Optional[Dict]:
    """
    Execute a single search with job title and location.
    
    Args:
        args: Tuple of (job_title, location)
        
    Returns:
        Dict containing search results or None if failed
    """
    job_title, location = args
    start_time = datetime.now(UTC)
    
    try:
        logger.info("Starting search", 
                   job_title=job_title, 
                   location=location)
        
        total_jobs, pages, success = process_job_search(
            job_title=job_title,
            job_location=location,
            max_page_depth=MAX_PAGE_DEPTH
        )
        
        duration = (datetime.now(UTC) - start_time).total_seconds()
        
        return {
            "job_title": job_title,
            "location": location,
            "total_jobs": total_jobs,
            "pages_processed": pages,
            "success": success,
            "duration_seconds": duration,
            "timestamp": datetime.now(UTC).isoformat()
        }
        
    except Exception as e:
        logger.error("Search failed", 
                    job_title=job_title, 
                    location=location, 
                    error=str(e),
                    duration_seconds=(datetime.now(UTC) - start_time).total_seconds())
        return None

def main():
    """
    Run batch job searches using ThreadPoolExecutor with rate limiting.
    """
    start_time = datetime.now(UTC)
    
    # Create all search combinations
    search_pairs = list(itertools.product(JOB_TITLES, LOCATIONS))
    total_searches = len(search_pairs)
    
    logger.info("Starting batch search", 
                total_searches=total_searches,
                job_titles=len(JOB_TITLES),
                locations=len(LOCATIONS),
                max_workers=MAX_WORKERS,
                rate_limit_seconds=RATE_LIMIT)

    successful_searches = 0
    failed_searches = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all searches
        futures = {}
        for pair in search_pairs:
            # Add small delay between submissions for rate limiting
            time.sleep(RATE_LIMIT)
            futures[executor.submit(run_search, pair)] = pair
        
        # Process results as they complete
        for future in as_completed(futures):
            job_title, location = futures[future]
            try:
                result = future.result()
                if result:
                    successful_searches += 1
                    logger.info("Search completed successfully", 
                              job_title=job_title,
                              location=location,
                              jobs_found=result["total_jobs"])
                else:
                    failed_searches += 1
                    logger.warning("Search completed with no results",
                                 job_title=job_title,
                                 location=location)
            except Exception as e:
                failed_searches += 1
                logger.error("Search failed unexpectedly",
                           job_title=job_title,
                           location=location,
                           error=str(e))

    duration = (datetime.now(UTC) - start_time).total_seconds()
    logger.info("Batch search completed", 
                successful_searches=successful_searches,
                failed_searches=failed_searches,
                total_duration_seconds=duration,
                searches_per_second=total_searches/duration)

if __name__ == "__main__":
    main() 