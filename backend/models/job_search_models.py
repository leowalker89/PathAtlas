from typing import List, Optional
from pydantic import BaseModel, Field, HttpUrl
from datetime import datetime

class SearchMetadata(BaseModel):
    """Metadata about the search request and response"""
    id: str = Field(..., min_length=1)
    status: str = Field(..., pattern="(?i)^(success|Success|error|pending)$")
    created_at: datetime
    request_time_taken: float = Field(..., ge=0)
    parsing_time_taken: float = Field(..., ge=0)
    total_time_taken: float = Field(..., ge=0)
    request_url: HttpUrl
    html_url: HttpUrl
    json_url: HttpUrl

class SearchParameters(BaseModel):
    """Parameters used for the search"""
    engine: str
    q: str = Field(..., min_length=1)  # search query
    google_domain: str
    hl: str  # language
    gl: str  # geography/location

class SearchInformation(BaseModel):
    """Information about the search results"""
    query_displayed: str
    detected_location: str

class JobHighlight(BaseModel):
    """Highlights/sections of a job posting"""
    title: str
    items: List[str]

class DetectedExtensions(BaseModel):
    """Detected metadata about the job"""
    posted_at: Optional[str] = None
    schedule: Optional[str] = None
    salary: Optional[str] = None
    health_insurance: Optional[bool] = None
    dental_insurance: Optional[bool] = None
    paid_time_off: Optional[bool] = None

class ApplyLink(BaseModel):
    """Links to apply for the job"""
    link: HttpUrl
    source: str

class JobListing(BaseModel):
    """Individual job posting"""
    position: int = Field(..., ge=1)
    title: str
    company_name: str
    location: str
    via: str
    description: str
    job_highlights: Optional[List[JobHighlight]] = None
    extensions: Optional[List[str]] = None
    detected_extensions: Optional[DetectedExtensions] = None
    apply_link: HttpUrl
    apply_links: List[ApplyLink]
    sharing_link: HttpUrl
    thumbnail: Optional[str] = None

class JobSearchResponse(BaseModel):
    """Complete response from the Google Jobs API"""
    search_metadata: SearchMetadata
    search_parameters: SearchParameters
    search_information: SearchInformation
    jobs: List[JobListing]
    pagination: Optional[dict] = None  # Add pagination info
    is_subsequent_page: bool = False   # Flag to indicate if this is a subsequent page

class JobSearchDocument(BaseModel):
    """MongoDB document for storing complete job search results"""
    search_metadata: SearchMetadata
    search_parameters: SearchParameters
    search_information: SearchInformation
    total_jobs: int = 0
    pages_processed: int = 0
    jobs: List[str] = []  # List of job IDs that were found in this search
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)