# Job Search Pipeline

## Overview
Pipeline for fetching, processing, and storing job listings.

## Components
- API Integration (SearchAPI.io)
- Data Validation (Pydantic)
- Storage (MongoDB)

## Key Functions
1. `process_job_search(job_title, job_location, max_page_depth)`
   - Main entry point for job searches
   - Handles pagination and multi-page results

2. `fetch_jobs_from_api()`
   - Retries: 3 attempts with exponential backoff
   - Rate limiting: 4-10 second delays between retries

3. `store_jobs_in_db()`
   - Deduplication via compound indexes
   - URL string conversion for MongoDB compatibility

## MongoDB Collections
- `job_listings`: Individual job posts
- `job_searches`: Search metadata and results

## Environment Variables
Required in `.env`:
- MONGODB_URI
- SEARCH_API_KEY 