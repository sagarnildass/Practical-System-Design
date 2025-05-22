"""
SEO Analyzer UI using Gradio, Web Crawler, and OpenAI
"""

import gradio as gr
import logging
import json
from typing import Dict, List, Any, Tuple
from urllib.parse import urlparse
import tldextract
from openai import OpenAI
import time
import os

from crawler import Crawler
from models import URL, Page
import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)

class SEOAnalyzer:
    """
    SEO Analyzer that combines web crawler with OpenAI analysis
    """
    
    def __init__(self, api_key: str):
        """Initialize SEO Analyzer"""
        self.client = OpenAI(api_key=api_key)
        self.crawler = Crawler()
        
    def analyze_website(self, url: str, max_pages: int = 10) -> Tuple[str, List[Dict]]:
        """
        Crawl website and analyze SEO using OpenAI
        
        Args:
            url: Seed URL to crawl
            max_pages: Maximum number of pages to crawl
            
        Returns:
            Tuple of (overall analysis, list of page-specific analyses)
        """
        try:
            # Extract domain for filtering
            domain = self._extract_domain(url)
            
            # Configure and run crawler
            self.crawler.reset_storage()
            self.crawler.add_seed_urls([url])
            self.crawler.set_domain_filter(domain)
            
            # Start crawling
            crawled_pages = []
            for page in self.crawler.crawl(max_pages=max_pages):
                if isinstance(page, Page) and page.content:
                    crawled_pages.append({
                        'url': page.url,
                        'content': page.content,
                        'metadata': page.metadata if hasattr(page, 'metadata') else {}
                    })
            
            if not crawled_pages:
                return "No pages were successfully crawled.", []
            
            # Analyze crawled pages with OpenAI
            overall_analysis = self._get_overall_analysis(crawled_pages)
            page_analyses = self._get_page_analyses(crawled_pages)
            
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
            
            return formatted_analysis, page_analyses
            
        except Exception as e:
            logger.error(f"Error analyzing website: {e}")
            return f"Error analyzing website: {str(e)}", []
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        parsed = tldextract.extract(url)
        return f"{parsed.domain}.{parsed.suffix}"
    
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
                model="gpt-4-turbo-preview",
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
                    model="gpt-4-turbo-preview",
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
    
    def analyze(url: str, api_key: str, max_pages: int) -> str:
        """Gradio interface function"""
        try:
            analyzer = SEOAnalyzer(api_key)
            analysis, _ = analyzer.analyze_website(url, max_pages)
            return analysis
        except Exception as e:
            return f"Error: {str(e)}"
    
    # Create interface
    iface = gr.Interface(
        fn=analyze,
        inputs=[
            gr.Textbox(label="Website URL", placeholder="https://example.com"),
            gr.Textbox(label="OpenAI API Key", type="password"),
            gr.Slider(minimum=1, maximum=50, value=10, step=1, label="Maximum Pages to Crawl")
        ],
        outputs=gr.Markdown(label="SEO Analysis"),
        title="Website SEO Analyzer",
        description="Enter a website URL to get comprehensive SEO analysis using web crawler and OpenAI.",
        theme="default",
        allow_flagging="never"
    )
    
    return iface

if __name__ == "__main__":
    # Create and launch UI
    ui = create_ui()
    ui.launch(share=False, server_name="0.0.0.0") 