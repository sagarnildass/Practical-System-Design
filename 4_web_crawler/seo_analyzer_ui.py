"""
SEO Analyzer UI using Gradio, Web Crawler, and OpenAI
"""

import gradio as gr
import logging
import json
from typing import Dict, List, Any, Tuple, Optional
from urllib.parse import urlparse
import tldextract
from openai import OpenAI
import time
import os
import threading
import queue
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import tempfile

from crawler import Crawler
from frontier import URLFrontier
from models import URL, Page
import config
from run_crawler import reset_databases
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# Check if we're in deployment mode (e.g., Hugging Face Spaces)
IS_DEPLOYMENT = os.getenv('DEPLOYMENT', 'false').lower() == 'true'

# Custom CSS for better styling
CUSTOM_CSS = """
.container {
    max-width: 1200px !important;
    margin: auto;
    padding: 20px;
}

.header {
    text-align: center;
    margin-bottom: 2rem;
}

.header h1 {
    color: #2d3748;
    font-size: 2.5rem;
    font-weight: 700;
    margin-bottom: 1rem;
}

.header p {
    color: #4a5568;
    font-size: 1.1rem;
    max-width: 800px;
    margin: 0 auto;
}

.input-section {
    background: #f7fafc;
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 24px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.analysis-section {
    background: white;
    border-radius: 12px;
    padding: 24px;
    margin-top: 24px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.log-section {
    font-family: monospace;
    background: #1a202c;
    color: #e2e8f0;
    padding: 16px;
    border-radius: 8px;
    margin-top: 24px;
}

/* Custom styling for inputs */
.input-container {
    background: white;
    padding: 16px;
    border-radius: 8px;
    margin-bottom: 16px;
}

/* Custom styling for the slider */
.slider-container {
    padding: 12px;
    background: white;
    border-radius: 8px;
}

/* Custom styling for buttons */
.primary-button {
    background: #4299e1 !important;
    color: white !important;
    padding: 12px 24px !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    transition: all 0.3s ease !important;
}

.primary-button:hover {
    background: #3182ce !important;
    transform: translateY(-1px) !important;
}

/* Markdown output styling */
.markdown-output {
    font-family: system-ui, -apple-system, sans-serif;
    line-height: 1.6;
}

.markdown-output h1 {
    color: #2d3748;
    border-bottom: 2px solid #e2e8f0;
    padding-bottom: 0.5rem;
}

.markdown-output h2 {
    color: #4a5568;
    margin-top: 2rem;
}

.markdown-output h3 {
    color: #718096;
    margin-top: 1.5rem;
}

/* Progress bar styling */
.progress-bar {
    height: 8px !important;
    border-radius: 4px !important;
    background: #ebf8ff !important;
}

.progress-bar-fill {
    background: #4299e1 !important;
    border-radius: 4px !important;
}

/* Add some spacing between sections */
.gap {
    margin: 2rem 0;
}
"""

# Create a custom handler that will store logs in a queue
class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        log_entry = self.format(record)
        try:
            self.log_queue.put_nowait(f"{datetime.now().strftime('%H:%M:%S')} - {log_entry}")
        except queue.Full:
            pass  # Ignore if queue is full

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info(f"IS_DEPLOYMENT: {IS_DEPLOYMENT}")

class InMemoryStorage:
    """Simple in-memory storage for deployment mode"""
    def __init__(self):
        self.urls = {}
        self.pages = {}
        
    def reset(self):
        self.urls.clear()
        self.pages.clear()
        
    def add_url(self, url_obj):
        self.urls[url_obj.url] = url_obj
        
    def add_page(self, page_obj):
        self.pages[page_obj.url] = page_obj
        
    def get_url(self, url):
        return self.urls.get(url)
        
    def get_page(self, url):
        return self.pages.get(url)

class SEOAnalyzer:
    """
    SEO Analyzer that combines web crawler with OpenAI analysis
    """
    
    def __init__(self, api_key: str):
        """Initialize SEO Analyzer"""
        self.client = OpenAI(api_key=api_key)
        self.crawler = None
        self.crawled_pages = []
        self.pages_crawled = 0
        self.max_pages = 0
        self.crawl_complete = threading.Event()
        self.log_queue = queue.Queue(maxsize=1000)
        self.session_id = str(uuid.uuid4())
        self.storage = InMemoryStorage() if IS_DEPLOYMENT else None
        
        # Add queue handler to logger
        queue_handler = QueueHandler(self.log_queue)
        queue_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
        logger.addHandler(queue_handler)
        
    def _setup_session_storage(self) -> Tuple[str, str, str]:
        """
        Set up session-specific storage directories
        
        Returns:
            Tuple of (storage_path, html_path, log_path)
        """
        # Create session-specific paths
        session_storage = os.path.join(config.STORAGE_PATH, self.session_id)
        session_html = os.path.join(session_storage, "html")
        session_logs = os.path.join(session_storage, "logs")
        
        # Create directories
        os.makedirs(session_storage, exist_ok=True)
        os.makedirs(session_html, exist_ok=True)
        os.makedirs(session_logs, exist_ok=True)
        
        logger.info(f"Created session storage at {session_storage}")
        return session_storage, session_html, session_logs
        
    def _cleanup_session_storage(self):
        """Clean up session-specific storage"""
        session_path = os.path.join(config.STORAGE_PATH, self.session_id)
        try:
            if os.path.exists(session_path):
                shutil.rmtree(session_path)
                logger.info(f"Cleaned up session storage at {session_path}")
        except Exception as e:
            logger.error(f"Error cleaning up session storage: {e}")
            
    def _reset_storage(self):
        """Reset storage based on deployment mode"""
        if IS_DEPLOYMENT:
            self.storage.reset()
        else:
            reset_databases()

    def analyze_website(self, url: str, max_pages: int = 10, progress: gr.Progress = gr.Progress()) -> Tuple[str, List[Dict], str]:
        """
        Crawl website and analyze SEO using OpenAI
        
        Args:
            url: Seed URL to crawl
            max_pages: Maximum number of pages to crawl
            progress: Gradio progress indicator
            
        Returns:
            Tuple of (overall analysis, list of page-specific analyses, log output)
        """
        try:
            # Reset state
            self.crawled_pages = []
            self.pages_crawled = 0
            self.max_pages = max_pages
            self.crawl_complete.clear()
            
            # Set up storage
            if IS_DEPLOYMENT:
                # Use temporary directory for file storage in deployment
                temp_dir = tempfile.mkdtemp()
                session_storage = temp_dir
                session_html = os.path.join(temp_dir, "html")
                session_logs = os.path.join(temp_dir, "logs")
                os.makedirs(session_html, exist_ok=True)
                os.makedirs(session_logs, exist_ok=True)
            else:
                session_storage, session_html, session_logs = self._setup_session_storage()
            
            # Update config paths for this session
            config.HTML_STORAGE_PATH = session_html
            config.LOG_PATH = session_logs
            
            # Clear log queue
            while not self.log_queue.empty():
                self.log_queue.get_nowait()
            
            logger.info(f"Starting analysis of {url} with max_pages={max_pages}")
            
            # Reset storage
            logger.info("Resetting storage...")
            self._reset_storage()
            logger.info("Storage reset completed")
            
            # Create new crawler instance with appropriate storage
            logger.info("Creating crawler instance...")
            if IS_DEPLOYMENT:
                # In deployment mode, use in-memory storage
                self.crawler = Crawler(storage=self.storage)
                # Set frontier to use memory mode
                self.crawler.frontier = URLFrontier(use_memory=True)
            else:
                # In local mode, use MongoDB and Redis
                self.crawler = Crawler()
            logger.info("Crawler instance created successfully")
            
            # Extract domain for filtering
            domain = self._extract_domain(url)
            logger.info(f"Analyzing domain: {domain}")
            
            # Add seed URL and configure domain filter
            self.crawler.add_seed_urls([url])
            config.ALLOWED_DOMAINS = [domain]
            logger.info("Added seed URL and configured domain filter")
            
            # Override the crawler's _process_url method to capture pages
            original_process_url = self.crawler._process_url
            def wrapped_process_url(url_obj):
                if self.pages_crawled >= self.max_pages:
                    self.crawler.running = False  # Signal crawler to stop
                    self.crawl_complete.set()
                    return
                
                original_process_url(url_obj)
                
                # Get the page based on storage mode
                if IS_DEPLOYMENT:
                    # In deployment mode, get page from in-memory storage
                    page = self.storage.get_page(url_obj.url)
                    if page:
                        _, metadata = self.crawler.parser.parse(page)
                        self.crawled_pages.append({
                            'url': url_obj.url,
                            'content': page.content,
                            'metadata': metadata
                        })
                        self.pages_crawled += 1
                        logger.info(f"Crawled page {self.pages_crawled}/{max_pages}: {url_obj.url}")
                else:
                    # In local mode, get page from MongoDB
                    page_data = self.crawler.pages_collection.find_one({'url': url_obj.url})
                    if page_data and page_data.get('content'):
                        _, metadata = self.crawler.parser.parse(Page(**page_data))
                        self.crawled_pages.append({
                            'url': url_obj.url,
                            'content': page_data['content'],
                            'metadata': metadata
                        })
                        self.pages_crawled += 1
                        logger.info(f"Crawled page {self.pages_crawled}/{max_pages}: {url_obj.url}")
                    
                if self.pages_crawled >= self.max_pages:
                    self.crawler.running = False  # Signal crawler to stop
                    self.crawl_complete.set()
            
            self.crawler._process_url = wrapped_process_url
            
            def run_crawler():
                try:
                    # Skip signal handler registration
                    self.crawler.running = True
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        try:
                            futures = [executor.submit(self.crawler._crawl_worker)]
                            for future in futures:
                                future.result()
                        except Exception as e:
                            logger.error(f"Error in crawler worker: {e}")
                        finally:
                            self.crawler.running = False
                            self.crawl_complete.set()
                except Exception as e:
                    logger.error(f"Error in run_crawler: {e}")
                    self.crawl_complete.set()
            
            # Start crawler in a thread
            crawler_thread = threading.Thread(target=run_crawler)
            crawler_thread.daemon = True
            crawler_thread.start()
            
            # Wait for completion or timeout with progress updates
            timeout = 300  # 5 minutes
            start_time = time.time()
            last_progress = 0
            while not self.crawl_complete.is_set() and time.time() - start_time < timeout:
                current_progress = min(0.8, self.pages_crawled / max_pages)
                if current_progress != last_progress:
                    progress(current_progress, f"Crawled {self.pages_crawled}/{max_pages} pages")
                    last_progress = current_progress
                time.sleep(0.1)  # More frequent updates
            
            if time.time() - start_time >= timeout:
                logger.warning("Crawler timed out")
                self.crawler.running = False
            
            # Wait for thread to finish
            crawler_thread.join(timeout=10)
            
            # Restore original method
            self.crawler._process_url = original_process_url
            
            # Collect all logs
            logs = []
            while not self.log_queue.empty():
                logs.append(self.log_queue.get_nowait())
            log_output = "\n".join(logs)
            
            if not self.crawled_pages:
                self._cleanup_session_storage()
                return "No pages were successfully crawled.", [], log_output
            
            logger.info("Starting OpenAI analysis...")
            progress(0.9, "Analyzing crawled pages with OpenAI...")
            
            # Analyze crawled pages with OpenAI
            overall_analysis = self._get_overall_analysis(self.crawled_pages)
            progress(0.95, "Generating page-specific analyses...")
            page_analyses = self._get_page_analyses(self.crawled_pages)
            
            logger.info("Analysis complete")
            progress(1.0, "Analysis complete")
            
            # Format the results
            formatted_analysis = f"""
# SEO Analysis Report for {domain}

## Overall Analysis
{overall_analysis}

## Page-Specific Analyses
"""
            for page_analysis in page_analyses:
                formatted_analysis += f"""
### {page_analysis['url']}
{page_analysis['analysis']}
"""
            
            # Clean up all resources
            logger.info("Cleaning up resources...")
            if IS_DEPLOYMENT:
                shutil.rmtree(temp_dir, ignore_errors=True)
                self.storage.reset()
            else:
                self._cleanup_session_storage()
                self._reset_storage()
            logger.info("All resources cleaned up")
            
            return formatted_analysis, page_analyses, log_output
            
        except Exception as e:
            logger.error(f"Error analyzing website: {e}")
            # Clean up all resources even on error
            if IS_DEPLOYMENT:
                shutil.rmtree(temp_dir, ignore_errors=True)
                self.storage.reset()
            else:
                self._cleanup_session_storage()
                self._reset_storage()
            # Collect all logs
            logs = []
            while not self.log_queue.empty():
                logs.append(self.log_queue.get_nowait())
            log_output = "\n".join(logs)
            return f"Error analyzing website: {str(e)}", [], log_output
            
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        extracted = tldextract.extract(url)
        return f"{extracted.domain}.{extracted.suffix}"
    
    def _get_overall_analysis(self, pages: List[Dict]) -> str:
        """Get overall SEO analysis using OpenAI"""
        try:
            # Prepare site overview for analysis
            site_overview = {
                'num_pages': len(pages),
                'pages': [{
                    'url': page['url'],
                    'metadata': page['metadata']
                } for page in pages]
            }
            
            # Create analysis prompt
            prompt = f"""
You are an expert SEO consultant. Analyze this website's SEO based on the crawled data:

{json.dumps(site_overview, indent=2)}

Provide a comprehensive SEO analysis including:
1. Overall site structure and navigation
2. Common SEO issues across pages
3. Content quality and optimization
4. Technical SEO recommendations
5. Priority improvements

Format your response in Markdown.
"""
            
            # Get analysis from OpenAI
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are an expert SEO consultant providing detailed website analysis."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=2000
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error getting overall analysis: {e}")
            return f"Error generating overall analysis: {str(e)}"
    
    def _get_page_analyses(self, pages: List[Dict]) -> List[Dict]:
        """Get page-specific SEO analyses using OpenAI"""
        page_analyses = []
        
        for page in pages:
            try:
                # Create page analysis prompt
                prompt = f"""
Analyze this page's SEO:

URL: {page['url']}
Metadata: {json.dumps(page['metadata'], indent=2)}

Provide specific recommendations for:
1. Title and meta description
2. Heading structure
3. Content optimization
4. Internal linking
5. Technical improvements

Format your response in Markdown.
"""
                
                # Get analysis from OpenAI
                response = self.client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are an expert SEO consultant providing detailed page analysis."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.7,
                    max_tokens=1000
                )
                
                page_analyses.append({
                    'url': page['url'],
                    'analysis': response.choices[0].message.content
                })
                
                # Sleep to respect rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error analyzing page {page['url']}: {e}")
                page_analyses.append({
                    'url': page['url'],
                    'analysis': f"Error analyzing page: {str(e)}"
                })
        
        return page_analyses

def create_ui() -> gr.Interface:
    """Create Gradio interface"""
    
    def analyze(url: str, api_key: str, max_pages: int, progress: gr.Progress = gr.Progress()) -> Tuple[str, str]:
        """Gradio interface function"""
        try:
            # Initialize analyzer
            analyzer = SEOAnalyzer(api_key)
            
            # Run analysis with progress updates
            analysis, _, logs = analyzer.analyze_website(url, max_pages, progress)
            
            # Collect all logs
            log_output = ""
            while not analyzer.log_queue.empty():
                try:
                    log_output += analyzer.log_queue.get_nowait() + "\n"
                except queue.Empty:
                    break
            
            # Set progress to complete
            progress(1.0, "Analysis complete")
            
            # Return results
            return analysis, log_output
            
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            logger.error(error_msg)
            return error_msg, error_msg

    # Create markdown content for the about section
    about_markdown = """
    # 🔍 SEO Analyzer Pro

    Analyze your website's SEO performance using advanced crawling and AI technology.
    
    ### Features:
    - 🕷️ Intelligent Web Crawling
    - 🧠 AI-Powered Analysis
    - 📊 Comprehensive Reports
    - 🚀 Performance Insights
    
    ### How to Use:
    1. Enter your website URL
    2. Provide your OpenAI API key
    3. Choose how many pages to analyze
    4. Click Analyze and watch the magic happen!
    
    ### What You'll Get:
    - Detailed SEO analysis
    - Content quality assessment
    - Technical recommendations
    - Performance insights
    - Actionable improvements
    """

    # Create the interface with custom styling
    with gr.Blocks(css=CUSTOM_CSS) as iface:
        gr.Markdown(about_markdown)
        
        with gr.Row():
            with gr.Column(scale=2):
                with gr.Group(elem_classes="input-section"):
                    gr.Markdown("### 📝 Enter Website Details")
                    url_input = gr.Textbox(
                        label="Website URL",
                        placeholder="https://example.com",
                        elem_classes="input-container",
                        info="Enter the full URL of the website you want to analyze (e.g., https://example.com)"
                    )
                    api_key = gr.Textbox(
                        label="OpenAI API Key",
                        placeholder="sk-...",
                        type="password",
                        elem_classes="input-container",
                        info="Your OpenAI API key is required for AI-powered analysis. Keep this secure!"
                    )
                    max_pages = gr.Slider(
                        minimum=1,
                        maximum=50,
                        value=10,
                        step=1,
                        label="Maximum Pages to Crawl",
                        elem_classes="slider-container",
                        info="Choose how many pages to analyze. More pages = more comprehensive analysis but takes longer"
                    )
                    analyze_btn = gr.Button(
                        "🔍 Analyze Website",
                        elem_classes="primary-button"
                    )

        with gr.Row():
            with gr.Column():
                with gr.Group(elem_classes="analysis-section"):
                    gr.Markdown("### 📊 Analysis Results")
                    analysis_output = gr.Markdown(
                        label="SEO Analysis",
                        elem_classes="markdown-output"
                    )

        with gr.Row():
            with gr.Column():
                with gr.Group(elem_classes="log-section"):
                    gr.Markdown("### 📋 Process Logs")
                    logs_output = gr.Textbox(
                        label="Logs",
                        lines=10,
                        elem_classes="log-output"
                    )

        # Connect the button click to the analyze function
        analyze_btn.click(
            fn=analyze,
            inputs=[url_input, api_key, max_pages],
            outputs=[analysis_output, logs_output],
        )

    return iface

if __name__ == "__main__":
    # Create base storage directory if it doesn't exist
    os.makedirs(config.STORAGE_PATH, exist_ok=True)
    
    # Create and launch UI
    ui = create_ui()
    ui.launch(
        share=False,
        server_name="0.0.0.0",
        show_api=False,
        show_error=True,
    ) 