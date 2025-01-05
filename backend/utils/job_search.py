from typing import List, Optional, Dict
from datetime import datetime, UTC
import requests
import os
from dotenv import load_dotenv
from logfire import Logfire
from backend.database.mongodb import get_jobs_collection, get_searches_collection
from backend.models.jobs_search_models import JobSearchResponse
from pydantic import ValidationError

# Initialize logging
logger = Logfire()

def fetch_job_data(
    job_title: str,
    job_location: Optional[str] = None,
    search_location: Optional[str] = None,
    next_page_token: Optional[str] = None
) -> Optional[Dict]:
    """
    Fetch job listings using SearchAPI.io's Google Jobs API
    
    Args:
        job_title (str): Job title or search query
        job_location (str, optional): Location to include in search query
        search_location (str, optional): Location parameter for API
        next_page_token (str, optional): Token for pagination
        
    Returns:
        Optional[Dict]: Parsed JSON response containing job listings or None if error occurs
        
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

def parse_job_response(raw_response: Dict) -> Optional[JobSearchResponse]:
    """
    Parse raw API response into JobSearchResponse model
    
    Args:
        raw_response (Dict): Raw JSON response from the API
        
    Returns:
        Optional[JobSearchResponse]: Parsed response or None if parsing fails
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

def fetch_and_parse_jobs(
    job_title: str,
    job_location: Optional[str] = None,
    search_location: Optional[str] = None,
    next_page_token: Optional[str] = None,
    search_level: int = 1  # Only used for logging and state tracking
) -> Optional[JobSearchResponse]:
    """
    Fetch and parse job listings
    
    Args:
        job_title (str): Job title or search query
        job_location (str, optional): Location to include in search query
        search_location (str, optional): Location parameter for API
        next_page_token (str, optional): Token for pagination
        search_level (int): Current level of search pagination (used for tracking only)
        
    Returns:
        Optional[JobSearchResponse]: Parsed job search response
    """
    raw_response = fetch_job_data(
        job_title=job_title,
        job_location=job_location,
        search_location=search_location,
        next_page_token=next_page_token
    )
    
    if not raw_response:
        return None
        
    return parse_job_response(raw_response)  # Remove search_level addition but keep type hints

def store_job_results(parsed_response: JobSearchResponse) -> bool:
    """
    Store job search results in MongoDB, splitting between searches and jobs collections
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