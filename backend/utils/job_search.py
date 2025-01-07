from typing import List, Optional, Dict, Tuple
from datetime import datetime, UTC
import requests
import os
from dotenv import load_dotenv
from backend.utils.logger import logger
from backend.database.mongodb_jobfocus import get_jobs_collection, get_searches_collection
from backend.models.job_search_models import JobSearchResponse, JobSearchDocument
from pydantic import ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def fetch_jobs_from_api(
    job_title: str,
    job_location: Optional[str] = None,
    search_location: Optional[str] = None,
    next_page_token: Optional[str] = None
) -> Optional[Dict]:
    """
    Fetch raw job data from SearchAPI.io.
    
    Args:
        job_title (str): The job title or search query
        job_location (Optional[str]): Location to include in search query
        search_location (Optional[str]): Location parameter for API geotargeting
        next_page_token (Optional[str]): Token for pagination
        
    Returns:
        Optional[Dict]: Raw API response data or None if request fails
        
    Raises:
        requests.RequestException: If API request fails
    """
    load_dotenv()
    
    api_key = os.getenv('SEARCH_API_KEY')
    if not api_key:
        logger.error("SEARCH_API_KEY not found in environment variables")
        return None
    
    url = "https://www.searchapi.io/api/v1/search"
    params = {
        "engine": "google_jobs",
        "q": f"{job_title} {job_location}" if job_location else job_title,
        "api_key": api_key,
        "gl": "us",  # Country code for United States
        "hl": "en",  # Language code for English
    }
    
    if search_location:
        params['location'] = search_location
    if next_page_token:
        params['next_page_token'] = next_page_token

    try:
        logger.info("Fetching jobs", query=params['q'])
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        return response.json()
        
    except requests.RequestException as e:
        logger.error("API request failed", error=str(e))
        raise

def parse_jobs_response(raw_response: Dict) -> Optional[JobSearchResponse]:
    """
    Parse raw API response into structured job data.
    
    Args:
        raw_response (Dict): Raw API response data
        
    Returns:
        Optional[JobSearchResponse]: Parsed job response object or None if parsing fails
        
    Notes:
        - Stores pagination token as '_next_page_token' attribute
        - Validates response against JobSearchResponse model
    """
    try:
        # Remove pagination from raw response before parsing
        response_data = raw_response.copy()
        pagination_token = response_data.pop('pagination', {}).get('next_page_token')
        
        # Store pagination token in the function's return context
        result = JobSearchResponse(**response_data)
        # Attach pagination token as a simple attribute (won't be stored in MongoDB)
        setattr(result, '_next_page_token', pagination_token)
        return result
        
    except Exception as e:
        logger.error(f"Error parsing job response: {str(e)}")
        if isinstance(e, ValidationError):
            logger.error(f"Validation errors: {e.errors()}")
        return None

def store_jobs_in_db(parsed_response: JobSearchResponse, search_id: Optional[str] = None) -> bool:
    """Store jobs in MongoDB, updating existing search if search_id provided"""
    start_time = datetime.now(UTC)
    try:
        job_collection = get_jobs_collection()
        search_collection = get_searches_collection()
        
        successful_jobs = 0
        job_ids = []
        
        for job in parsed_response.jobs:
            try:
                # Convert to dict and ensure URLs are strings
                job_dict = job.model_dump()
                job_dict["apply_link"] = str(job.apply_link)
                job_dict["sharing_link"] = str(job.sharing_link)
                job_dict["apply_links"] = [{"link": str(link.link), "source": link.source} for link in job.apply_links]
                job_dict["search_id"] = search_id or parsed_response.search_metadata.id
                job_dict["created_at"] = datetime.now(UTC)
                
                result = job_collection.update_one(
                    {
                        "title": job.title,
                        "company_name": job.company_name,
                        "location": job.location,
                        "apply_link": str(job.apply_link)
                    },
                    {"$set": job_dict},
                    upsert=True
                )
                
                if result.upserted_id:
                    job_ids.append(str(result.upserted_id))
                
                successful_jobs += 1
                
            except Exception as e:
                logger.error("Error storing job", 
                            job_title=job.title,
                            error=str(e))
                continue
        
        logger.info("Storage metrics", 
                   processed=len(parsed_response.jobs),
                   stored=successful_jobs,
                   duration=str(datetime.now(UTC) - start_time))
        return successful_jobs > 0
        
    except Exception as db_error:
        logger.error("Database operation error", error=str(db_error))
        return False

def process_job_search(
    job_title: str,
    job_location: Optional[str] = None,
    max_page_depth: int = 1
) -> Tuple[int, int, bool]:
    """
    Fetch and store jobs from SearchAPI.io with pagination support.

    Args:
        job_title: Search query for job title
        job_location: Geographic location filter (optional)
        max_page_depth: Maximum pages to fetch (default: 1)

    Returns:
        Tuple containing:
        - Number of jobs processed
        - Pages successfully fetched
        - Storage success status

    Example:
        >>> total, pages, success = process_job_search("Python Developer", "New York")
    """
    total_jobs = 0
    pages_processed = 0
    storage_success = True
    next_page_token = None
    search_info = None
    search_id = None

    logger.info("Starting job search", 
                job_title=job_title, 
                location=job_location, 
                max_pages=max_page_depth)
    
    while pages_processed < max_page_depth:
        try:
            raw_response = fetch_jobs_from_api(
                job_title=job_title,
                job_location=job_location,
                next_page_token=next_page_token
            )
            
            if raw_response is None:
                logger.error("Failed to fetch response from API")
                break
            
            if pages_processed == 0:
                search_info = raw_response.get('search_information', {})
                if 'search_metadata' not in raw_response:
                    logger.error("Invalid API response", error="missing search_metadata")
                    break
                search_id = raw_response['search_metadata']['id']
            else:
                raw_response['search_information'] = search_info
                raw_response['is_subsequent_page'] = True
            
            parsed_response = JobSearchResponse(**raw_response)
            
            if not store_jobs_in_db(parsed_response, search_id):
                storage_success = False
                logger.warning("Failed to store some jobs", search_id=search_id)
            
            total_jobs += len(parsed_response.jobs)
            next_page_token = raw_response.get('pagination', {}).get('next_page_token')
            
            logger.info("Page processed", 
                       jobs_found=len(parsed_response.jobs),
                       page_number=pages_processed + 1,
                       has_next_page=bool(next_page_token))
            
            if not next_page_token:
                logger.info("No more pages available")
                break
                
            pages_processed += 1
            
            if pages_processed >= max_page_depth:
                logger.info("Reached max page depth", max_depth=max_page_depth)
                break
                
        except Exception as e:
            logger.error("Error processing page", 
                        page_number=pages_processed + 1,
                        error=str(e))
            break
    
    logger.info("Job search completed", 
                total_jobs=total_jobs,
                pages_processed=pages_processed,
                storage_success=storage_success)
            
    return total_jobs, pages_processed, storage_success