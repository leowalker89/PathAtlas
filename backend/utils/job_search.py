from typing import List, Optional, Dict
from datetime import datetime, UTC
import requests
import os
from dotenv import load_dotenv
from logfire import Logfire
from backend.database.mongodb_jobfocus import get_jobs_collection, get_searches_collection
from backend.models.job_search_models import JobSearchResponse
from pydantic import ValidationError

# Initialize logging
logger = Logfire()

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
        logger.info(f"Fetching jobs for query: {params['q']}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        return response.json()
        
    except requests.RequestException as e:
        logger.error(f"Error fetching job data: {str(e)}")
        return None

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

def store_jobs_in_db(parsed_response: JobSearchResponse) -> bool:
    """
    Store job data in MongoDB collections.
    
    Args:
        parsed_response (JobSearchResponse): Validated job search response
        
    Returns:
        bool: True if storage successful, False otherwise
        
    Notes:
        - Stores search metadata in searches collection
        - Stores individual jobs in jobs collection with search reference
        - Uses upsert to avoid duplicates
    """
    try:
        jobs_collection = get_jobs_collection()
        searches_collection = get_searches_collection()
        current_time = datetime.now(UTC)
        
        # Store search metadata
        search_dict = parsed_response.model_dump(exclude={'jobs'})
        search_id = search_dict['search_metadata']['id']
        
        searches_collection.update_one(
            {'search_metadata.id': search_id},
            {'$set': search_dict},
            upsert=True
        )
        
        # Store individual jobs with reference to search
        for job in parsed_response.jobs:
            job_dict = job.model_dump()
            job_dict.update({
                'search_id': search_id,
                'search_query': parsed_response.search_parameters.q,
                'fetched_at': current_time,
                'search_location': parsed_response.search_information.detected_location
            })
            
            jobs_collection.update_one(
                {
                    'title': job.title,
                    'company_name': job.company_name,
                    'location': job.location,
                    'apply_link': job.apply_link
                },
                {'$set': job_dict},
                upsert=True
            )
        
        logger.info(f"Successfully stored search data and {len(parsed_response.jobs)} jobs in MongoDB")
        return True
        
    except Exception as e:
        logger.error(f"Error storing data in MongoDB: {str(e)}")
        return False

def process_job_search(
    job_title: str,
    job_location: Optional[str] = None,
    search_location: Optional[str] = None,
    next_page_token: Optional[str] = None,
) -> tuple[Optional[JobSearchResponse], bool]:
    """
    Execute complete job search workflow: fetch, parse, and store jobs.
    
    This function orchestrates the entire job search process by:
    1. Fetching raw data from the API
    2. Parsing and validating the response
    3. Storing results in MongoDB
    
    Args:
        job_title (str): Job title or search query
        job_location (Optional[str]): Location to include in search query
        search_location (Optional[str]): Location parameter for API
        next_page_token (Optional[str]): Token for pagination
        
    Returns:
        tuple[Optional[JobSearchResponse], bool]: Tuple containing:
            - Parsed job response (or None if failed)
            - Boolean indicating if storage was successful
            
    Example:
        >>> response, success = process_job_search(
        ...     job_title="Python Developer",
        ...     job_location="San Francisco"
        ... )
        >>> if success:
        ...     print(f"Found {len(response.jobs)} jobs")
    """
    raw_response = fetch_jobs_from_api(
        job_title=job_title,
        job_location=job_location,
        search_location=search_location,
        next_page_token=next_page_token
    )
    
    if not raw_response:
        return None, False
        
    parsed_response = parse_jobs_response(raw_response)
    if not parsed_response:
        return None, False
        
    return parsed_response, store_jobs_in_db(parsed_response)