from typing import Optional, Dict, Literal
from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime
from models.job_description_models import JobDescription, GraderOutput

# Using Literal type for strict string values instead of Enum
ProcessingStatusType = Literal["extracting", "grading", "completed", "failed"]

class JobDescriptionProcessingState(BaseModel):
    """
    Manages the workflow state for processing job descriptions from raw postings to structured data.
    Handles the complete lifecycle of job description processing, including extraction, grading,
    and error handling while maintaining state and processing history.
    """
    model_config = ConfigDict(from_attributes=True)
    
    job_id: str = Field(
        description="Unique identifier for the job posting"
    )
    raw_job_data: Dict = Field(
        description="Original unprocessed job posting data in its raw form"
    )
    structured_job: Optional[JobDescription] = Field(
        default=None,
        description="Processed and structured version of the job description"
    )
    grader_output: Optional[GraderOutput] = Field(
        default=None,
        description="Results and metrics from the grading process"
    )
    attempts: int = Field(
        default=0,
        ge=0,
        description="Number of processing attempts made for this job"
    )
    max_attempts: int = Field(
        default=3,
        ge=1
    )
    status: ProcessingStatusType = Field(
        default="extracting",
        description="Current state in the processing workflow (extracting/grading/completed/failed)"
    )
    last_feedback: Optional[str] = Field(
        default=None,
        description="System feedback from the most recent processing attempt"
    )
    created_at: datetime
    updated_at: datetime
    error_message: Optional[str] = None