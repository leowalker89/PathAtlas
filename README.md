# PathAtlas

PathAtlas helps individuals discover their ideal career path through comprehensive documentation of their professional journey. Unlike traditional resumes, PathAtlas creates detailed mappings of experiences, skills, and aspirations, enabling sophisticated matching with career opportunities that align with their true potential.

## Tech Stack
- MongoDB for data storage
  - Collections: job_listings, job_searches
  - Optimized indexes for deduplication and search
- LangSmith for LLM tracing
- Logfire for structured logging and monitoring
  - Configured with service-level tracing
  - Integrated with Pydantic for model validation logging
  - Environment-aware configuration
- SearchAPI.io for job search integration
  - Retries with exponential backoff
  - Rate limiting controls
- Various LLMs for parsing and analysis

## Key Components
### Job Search Pipeline
- Fetches, processes, and stores job listings
- Handles pagination and multi-page results
- Automatic deduplication via compound indexes
- URL string normalization for MongoDB compatibility

### Environment Setup
Required in `.env`:
- `MONGODB_URI`: Database connection string
- `SEARCH_API_KEY`: SearchAPI.io credentials
- `LOGFIRE_TOKEN`: Logging service token
- `ENVIRONMENT`: Deployment environment (development/production)

Currently in early development. More details coming soon.
