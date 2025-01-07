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
        
        # Store individual jobs
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
                print(f"Error storing job {job.title}: {str(e)}")
                continue
        
        # Handle search document
        if not parsed_response.is_subsequent_page:
            search_doc = JobSearchDocument(
                search_metadata=parsed_response.search_metadata,
                search_parameters=parsed_response.search_parameters,
                search_information=parsed_response.search_information,
                total_jobs=len(parsed_response.jobs),
                pages_processed=1,
                jobs=job_ids
            )
            search_dict = search_doc.model_dump()
            # Convert URLs in metadata to strings
            search_dict["search_metadata"]["request_url"] = str(search_doc.search_metadata.request_url)
            search_dict["search_metadata"]["html_url"] = str(search_doc.search_metadata.html_url)
            search_dict["search_metadata"]["json_url"] = str(search_doc.search_metadata.json_url)
            
            search_collection.insert_one(search_dict)
        else:
            search_collection.update_one(
                {"search_metadata.id": search_id},
                {
                    "$inc": {
                        "total_jobs": len(parsed_response.jobs),
                        "pages_processed": 1
                    },
                    "$push": {"jobs": {"$each": job_ids}},
                    "$set": {"updated_at": datetime.now(UTC)}
                }
            )
        
        print(f"Successfully stored {successful_jobs} jobs")
        logger.info(f"Pipeline metrics: "
                   f"processed={len(parsed_response.jobs)}, "
                   f"stored={successful_jobs}, "
                   f"duration={datetime.now(UTC) - start_time}")
        return successful_jobs > 0
        
    except Exception as db_error:
        print(f"Database operation error: {str(db_error)}")
        return False

def process_job_search(
    job_title: str,
    job_location: Optional[str] = None,
    max_page_depth: int = 1
) -> Tuple[int, int, bool]:
    total_jobs = 0
    pages_processed = 0
    storage_success = True
    next_page_token = None
    search_info = None
    search_id = None  # Track the search ID across pages
    
    while pages_processed < max_page_depth:
        try:
            raw_response = fetch_jobs_from_api(
                job_title=job_title,
                job_location=job_location,
                next_page_token=next_page_token
            )
            
            if raw_response is None:
                print("Failed to fetch response from API")
                break
            
            # Store search_information from first page
            if pages_processed == 0:
                search_info = raw_response.get('search_information', {})
                if 'search_metadata' not in raw_response:
                    print("Invalid API response: missing search_metadata")
                    break
                search_id = raw_response['search_metadata']['id']
            else:
                raw_response['search_information'] = search_info
                raw_response['is_subsequent_page'] = True
            
            parsed_response = JobSearchResponse(**raw_response)
            
            if not store_jobs_in_db(parsed_response, search_id):
                storage_success = False
            
            total_jobs += len(parsed_response.jobs)
            next_page_token = raw_response.get('pagination', {}).get('next_page_token')
            print(f"Found {len(parsed_response.jobs)} jobs on this page")
            print(f"Next page token: {next_page_token[:100] if next_page_token else 'None'}")
            
            if not next_page_token:
                print("No more pages available")
                break
                
            pages_processed += 1
            
            if pages_processed >= max_page_depth:
                print(f"Reached max page depth of {max_page_depth}")
                break
                
        except Exception as e:
            print(f"Error processing page {pages_processed + 1}: {str(e)}")
            break
            
    return total_jobs, pages_processed, storage_success