"""
Workflow for fetching job listings and storing them in MongoDB.
Handles batch processing of multiple job searches with rate limiting.
"""

from typing import List, Tuple, Optional
from datetime import datetime, UTC
import asyncio
import random
from itertools import product
from tqdm import tqdm
from logfire import Logfire
from backend.utils.job_search import process_job_search

# Initialize logging
logger = Logfire()

# Job search configurations
JOB_TITLES: List[str] = [
    "AI Engineer", "Applied Data Scientist", "Machine Learning Engineer",
    "Technical Program Manager", "Solutions Engineer",
]

# Bay Area locations
JOB_LOCATIONS: List[str] = [
    "Palo Alto CA", "Mountain View CA", "Sunnyvale CA", "San Jose CA",
    "San Francisco CA", "Fremont CA", "Redwood City CA", "Remote"   
]

# Major tech hubs for comparison
TECH_HUBS: List[str] = [
    "Seattle WA", "Austin TX", "Dallas TX", "Miami FL", "New York NY",
    "Denver CO", "Charlotte NC", "Northern Virginia"
]

def run_job_search_workflow(
    job_titles: Optional[List[str]] = None,
    local_locations: Optional[List[str]] = None,
    tech_hubs: Optional[List[str]] = None,
    include_tech_hubs: bool = True,
    max_concurrent: int = 3,
    calls_per_minute: int = 30,
    max_retries: int = 3,
    max_pages: int = 2
) -> None:
    """
    Run the job search workflow with local and tech hub locations.
    
    Args:
        job_titles: List of job titles to search for
        local_locations: List of local locations to search in
        tech_hubs: List of tech hub locations to include
        include_tech_hubs: Whether to include tech hub locations
        max_concurrent: Maximum concurrent searches
        calls_per_minute: API rate limit
        max_retries: Maximum retries per search
        max_pages: Maximum pages to fetch per search
    """
    titles = job_titles if job_titles is not None else JOB_TITLES
    locations = local_locations if local_locations is not None else JOB_LOCATIONS
    
    # Combine local locations with tech hubs if requested
    if include_tech_hubs:
        hub_locations = tech_hubs if tech_hubs is not None else TECH_HUBS
        locations = locations + hub_locations
    
    start_time = datetime.now(UTC)
    logger.info(f"Starting job search workflow at {start_time}")
    logger.info(f"Searching {len(titles)} job titles across {len(locations)} locations")
    
    successful, failed = asyncio.run(process_job_searches(
        titles,
        locations,
        max_concurrent=max_concurrent,
        calls_per_minute=calls_per_minute,
        max_retries=max_retries,
        max_pages=max_pages
    ))
    
    end_time = datetime.now(UTC)
    duration = end_time - start_time
    
    logger.info(f"Workflow completed in {duration}")
    logger.info(f"Successful searches: {successful}")
    logger.info(f"Failed searches: {failed}")

if __name__ == "__main__":
    # Example usage with both local and tech hub locations
    run_job_search_workflow(
        max_concurrent=3,
        calls_per_minute=30,
        max_retries=3,
        max_pages=2,
        include_tech_hubs=True  # Set to False to only search local locations
    ) 