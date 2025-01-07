import logfire
import os
from dotenv import load_dotenv

def setup_logger():
    """
    Configure Logfire logger for PathAtlas.
    """
    load_dotenv()
    
    # Configure Logfire with project settings
    logfire.configure(
        token=os.getenv('LOGFIRE_TOKEN'),
        service_name="path-atlas",
        service_version="1.0.0",
        environment=os.getenv('ENVIRONMENT', 'development')
    )
    
    return logfire

# Create a global logger instance
logger = setup_logger() 