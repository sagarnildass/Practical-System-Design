"""
Main application entry point for the URL shortener service.
"""

import logging
import os
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from api import app, create_templates
from db import URLRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def initialize_app() -> FastAPI:
    """
    Initialize the FastAPI application
    
    Returns:
        FastAPI app: The initialized FastAPI application
    """
    logger.info("Initializing URL shortener application")
    
    # Ensure templates directory exists and create templates
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    if not os.path.exists(templates_dir):
        os.makedirs(templates_dir)
        logger.info(f"Created templates directory at {templates_dir}")
    
    # Create template files
    try:
        result = create_templates()
        logger.info("Created initial HTML templates")
    except Exception as e:
        logger.error(f"Error creating templates: {e}")
    
    # Initialize database schema
    try:
        URLRepository.initialize_db()
        logger.info("Database schema initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        # In production, you might want to raise the exception here to fail startup
        # For now, we'll log and continue to allow testing without DB
    
    # Mount static files if needed
    static_dir = os.path.join(os.path.dirname(__file__), 'static')
    if os.path.exists(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
    
    logger.info("URL shortener application initialized successfully")
    return app

def start():
    """Start the FastAPI application with Uvicorn"""
    # Initialize the application
    initialize_app()
    
    # Run with uvicorn
    uvicorn.run(
        "api:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("DEBUG", "False").lower() == "true",
        log_level="info"
    )

if __name__ == "__main__":
    start() 