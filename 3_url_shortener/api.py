"""
API endpoints for the URL shortener service.
"""

import time
import logging
from typing import Dict, Optional, Any, List
from fastapi import FastAPI, Request, Response, Depends, HTTPException, status
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, HttpUrl, validator, Field
import os
from shortener import URLShortener
from db import URLRepository
from config import BASE_URL, RATE_LIMIT, REDIRECT_TYPE

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="URL Shortener API",
    description="A production-ready URL shortening service",
    version="1.0.0"
)

# Add middleware
app.add_middleware(
    TrustedHostMiddleware, allowed_hosts=["*"]  # In production, restrict this
)

# Create in-memory rate limiting store
rate_limits = {}

# Setup templates directory
templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
os.makedirs(templates_dir, exist_ok=True)
templates = Jinja2Templates(directory=templates_dir)

# Define request and response models
class URLRequest(BaseModel):
    url: str = Field(..., description="The URL to shorten")
    
    @validator('url')
    def validate_url(cls, v):
        if not v:
            raise ValueError("URL cannot be empty")
        if len(v) > 2048:
            raise ValueError("URL is too long (max 2048 characters)")
        return v

class URLResponse(BaseModel):
    long_url: str
    short_url: str
    is_new: bool

class ErrorResponse(BaseModel):
    error: str
    message: str

class StatsResponse(BaseModel):
    short_url: str
    long_url: str
    clicks: int
    created_at: Optional[str] = None
    last_clicked: Optional[str] = None
    referrers: Optional[Dict[str, int]] = None
    browsers: Optional[Dict[str, int]] = None
    recent_clicks: Optional[List[Dict[str, Any]]] = None

# Create the template files
def create_templates():
    """Create basic HTML templates for the URL shortener"""
    # Create index.html
    with open(os.path.join(templates_dir, 'index.html'), 'w') as f:
        f.write("""
<!DOCTYPE html>
<html>
<head>
    <title>URL Shortener</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }
        .container {
            background-color: #f5f5f5;
            border-radius: 5px;
            padding: 20px;
            margin-top: 20px;
        }
        .form-group {
            margin-bottom: 15px;
        }
        input[type="text"] {
            width: 100%;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        button {
            background-color: #4CAF50;
            color: white;
            padding: 10px 15px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
        }
        .result {
            margin-top: 20px;
            display: none;
        }
        .short-url {
            font-weight: bold;
            color: #2196F3;
        }
    </style>
</head>
<body>
    <h1>URL Shortener</h1>
    <div class="container">
        <div class="form-group">
            <label for="long-url">Enter a long URL to shorten:</label>
            <input type="text" id="long-url" placeholder="https://example.com/very/long/url">
        </div>
        <button id="shorten-btn">Shorten URL</button>
        <div class="result" id="result">
            <h3>Shortened URL:</h3>
            <p>Your short URL is: <a href="#" id="short-url" class="short-url"></a></p>
        </div>
    </div>

    <script>
        document.getElementById('shorten-btn').addEventListener('click', function() {
            var longUrl = document.getElementById('long-url').value;
            
            if (!longUrl) {
                alert('Please enter a URL');
                return;
            }
            
            fetch('/api/v1/shorten', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    url: longUrl
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    alert(data.message);
                } else {
                    document.getElementById('short-url').href = data.short_url;
                    document.getElementById('short-url').textContent = data.short_url;
                    document.getElementById('result').style.display = 'block';
                }
            })
            .catch(error => {
                console.error('Error:', error);
                alert('An error occurred. Please try again.');
            });
        });
    </script>
</body>
</html>
        """)
    
    # Create 404.html
    with open(os.path.join(templates_dir, '404.html'), 'w') as f:
        f.write("""
<!DOCTYPE html>
<html>
<head>
    <title>404 - Not Found</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            text-align: center;
        }
        .error-container {
            margin-top: 50px;
        }
        h1 {
            font-size: 48px;
            color: #f44336;
        }
        .home-link {
            display: inline-block;
            margin-top: 20px;
            color: #2196F3;
            text-decoration: none;
        }
    </style>
</head>
<body>
    <div class="error-container">
        <h1>404</h1>
        <h2>Page Not Found</h2>
        <p>The short URL you requested could not be found.</p>
        <a href="/" class="home-link">Go to Homepage</a>
    </div>
</body>
</html>
        """)
    
    # Create 500.html
    with open(os.path.join(templates_dir, '500.html'), 'w') as f:
        f.write("""
<!DOCTYPE html>
<html>
<head>
    <title>500 - Server Error</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            text-align: center;
        }
        .error-container {
            margin-top: 50px;
        }
        h1 {
            font-size: 48px;
            color: #f44336;
        }
        .home-link {
            display: inline-block;
            margin-top: 20px;
            color: #2196F3;
            text-decoration: none;
        }
    </style>
</head>
<body>
    <div class="error-container">
        <h1>500</h1>
        <h2>Server Error</h2>
        <p>Something went wrong on our server. Please try again later.</p>
        <a href="/" class="home-link">Go to Homepage</a>
    </div>
</body>
</html>
        """)
    
    return {"message": "Templates created successfully"}

# Rate limiting dependency
async def rate_limiter(request: Request):
    """Rate limiting dependency for FastAPI"""
    ip = request.client.host
    current = time.time()
    
    # Initialize rate limit entry for this IP if it doesn't exist
    if ip not in rate_limits:
        rate_limits[ip] = {
            'count': 0,
            'reset_time': current + 60  # Reset after 60 seconds
        }
    
    # Reset count if the reset time has passed
    if current > rate_limits[ip]['reset_time']:
        rate_limits[ip] = {
            'count': 0,
            'reset_time': current + 60
        }
    
    # Increment count
    rate_limits[ip]['count'] += 1
    
    # Check if rate limit exceeded
    if rate_limits[ip]['count'] > RATE_LIMIT:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many requests, please try again later"
        )
    
    return ip

@app.post("/api/v1/shorten", 
         response_model=URLResponse, 
         responses={400: {"model": ErrorResponse}, 429: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
         summary="Shorten a URL",
         status_code=status.HTTP_201_CREATED)
async def shorten(url_request: URLRequest, client_ip: str = Depends(rate_limiter)):
    """
    Shortens a URL and returns the shortened version
    
    - **url**: The URL to shorten
    
    Returns the shortened URL and original URL
    """
    long_url = url_request.url
    
    # Normalize URL: add http:// if no protocol specified
    if not long_url.startswith('http://') and not long_url.startswith('https://'):
        long_url = 'http://' + long_url
    
    # Shorten the URL
    short_url, is_new = URLShortener.shorten_url(long_url)
    
    if not short_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not shorten the URL"
        )
    
    response = URLResponse(
        long_url=long_url,
        short_url=f"{BASE_URL}{short_url}",
        is_new=is_new
    )
    
    return response

@app.get("/{short_url}", 
        summary="Redirect to the original URL",
        response_class=RedirectResponse)
async def redirect_url(short_url: str, request: Request, response: Response):
    """
    Redirects to the original URL corresponding to the short URL
    
    - **short_url**: The shortened URL code
    
    Redirects to the original long URL if found
    """
    # Get the URL from the database
    url_id, long_url = URLShortener.get_long_url(short_url)
    
    if not long_url:
        return templates.TemplateResponse(
            "404.html",
            {"request": request, "short_url": short_url},
            status_code=status.HTTP_404_NOT_FOUND
        )
    
    # Record click for analytics if enabled
    if url_id:
        # Gather information for analytics
        user_agent = request.headers.get('User-Agent', '')
        referrer = request.headers.get('Referer', '')
        ip = request.client.host
        
        # Record in database asynchronously (in a real system)
        URLRepository.record_click(url_id, ip, user_agent, referrer)
    
    # Redirect to the original URL
    return RedirectResponse(url=long_url, status_code=REDIRECT_TYPE)

@app.get("/api/v1/stats/{short_url}", 
        response_model=StatsResponse,
        responses={404: {"model": ErrorResponse}, 429: {"model": ErrorResponse}},
        summary="Get statistics for a shortened URL")
async def get_stats(short_url: str, client_ip: str = Depends(rate_limiter)):
    """
    Get statistics for a shortened URL
    
    - **short_url**: The shortened URL code
    
    Returns detailed statistics including click count, creation date,
    last clicked time, referrers, browser distribution, and recent clicks
    """
    stats = URLShortener.get_url_stats(short_url)
    
    if not stats:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Short URL not found: {short_url}"
        )
    
    return stats

@app.get("/", summary="Home page")
async def index(request: Request):
    """Return the home page"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/v1/create-templates", summary="Create HTML templates")
async def create_templates_endpoint():
    """Endpoint to create basic HTML templates"""
    return create_templates()

if __name__ == '__main__':
    # This is only used when running directly from Python
    app.run(debug=True) 