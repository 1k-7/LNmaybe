# -*- coding: utf-8 -*-
import logging
import time
import requests
from urllib.parse import urlparse, parse_qs 
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

# Import Selenium
from lncrawl.webdriver.local import create_local
from selenium.webdriver import ChromeOptions

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # [TURBO] 60 threads for downloading
        self.init_executor(60) 
        
        # 1. Setup the RUNNER (Standard Requests)
        self.runner = requests.Session()
        
        self.runner.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
            "Upgrade-Insecure-Requests": "1",
        })
        
        # Force traffic through WARP
        # Ensure your WARP proxy is actually running on this port!
        self.proxy_url = "socks5h://127.0.0.1:40000"
        self.runner.proxies = {
            "http": self.proxy_url,
            "https": self.proxy_url
        }

        # Optimize connection pool
        adapter = requests.adapters.HTTPAdapter(pool_connections=60, pool_maxsize=60)
        self.runner.mount("https://", adapter)
        self.runner.mount("http://", adapter)

        # Expose runner
        self.scraper = self.runner

        self.cookies_synced = False
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info("FanMTL Strategy: Selenium Solver -> Requests Runner (Stable)")

    def refresh_cookies(self, url):
        """Launches a REAL headless Chrome browser to solve the Cloudflare Challenge."""
        logger.warning(f"🔒 Launching Browser Solver for: {url}")
        driver = None
        try:
            options = ChromeOptions()
            options.add_argument("--no-sandbox") 
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--headless=new") # Modern headless mode
            options.add_argument(f'--proxy-server={self.proxy_url}')
            
            driver = create_local(headless=True, options=options)
            
            driver.get(url)
            time.sleep(8) # Wait for initial load
            
            # Check Title
            if "Just a moment" in driver.title or "challenge" in driver.page_source.lower():
                logger.info("Browser: Detected Challenge. Waiting for solve...")
                time.sleep(15) # Give it time to solve

            cookies = driver.get_cookies()
            ua = driver.execute_script("return navigator.userAgent")
            
            found_cf = False
            for cookie in cookies:
                self.runner.cookies.set(
                    cookie['name'], 
                    cookie['value'], 
                    domain=cookie.get('domain', ''),
                    path=cookie.get('path', '/')
                )
                if 'cf_clearance' in cookie['name']:
                    found_cf = True
            
            if ua:
                self.runner.headers['User-Agent'] = ua
            
            if found_cf:
                logger.info("✅ Solver Success! Cookies synced. Resuming Turbo Mode.")
                self.cookies_synced = True
            else:
                logger.warning("⚠️ Browser finished but 'cf_clearance' missing. IP might be dirty.")
            
        except Exception as e:
            logger.critical(f"❌ Browser Solver Failed: {e}")
            pass 
        finally:
            if driver:
                try: driver.quit()
                except: pass

    def get_soup_safe(self, url, headers=None):
        """Smart wrapper: Fails fast -> Calls Solver -> Retries"""
        retries = 0
        max_retries = 2
        while True:
            try:
                req_headers = self.runner.headers.copy()
                if headers: req_headers.update(headers)

                # STEP 1: Try Fast Runner
                response = self.runner.get(url, headers=req_headers, timeout=20)
                
                # [FIXED] Detect Challenge even on 200 OK
                # FanMTL often returns 200 for the "Just a moment" page
                is_challenge = (
                    "just a moment" in response.text.lower() or 
                    "challenge-platform" in response.text.lower() or
                    "enable javascript" in response.text.lower()
                )

                if response.status_code in [403, 503, 429] or is_challenge:
                    if retries < max_retries:
                        logger.warning(f"⛔ Turbo session blocked (Status: {response.status_code}). Refreshing cookies...")
                        self.refresh_cookies(url)
                        retries += 1
                        continue
                    else:
                        logger.error("❌ Cloudflare Loop: Solver failed to clear the block.")
                        raise Exception("Cloudflare Loop")

                response.raise_for_status()
                return self.make_soup(response)

            except Exception as e:
                msg = str(e).lower()
                if "404" in msg:
                    logger.error(f"Permanent Error (404): {url}")
                    return self.make_soup("<html></html>")

                if retries < max_retries:
                    logger.warning(f"Request Error: {e}. Retrying...")
                    time.sleep(2)
                    retries += 1
                    continue
                
                logger.error(f"Failed to fetch {url} after retries.")
                return self.make_soup("<html></html>")

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        
        soup = self.get_soup_safe(self.novel_url)

        possible_title = soup.select_one("h1.novel-title")
        if possible_title:
            self.novel_title = possible_title.text.strip()
        else:
            meta_title = soup.select_one('meta[property="og:title"]')
            self.novel_title = meta_title.get("content").strip() if meta_title else "Unknown Title"

        img_tag = soup.select_one("figure.cover img") or soup.select_one(".fixed-img img")
        if img_tag:
            url = img_tag.get("src")
            if "placeholder" in str(url) and img_tag.get("data-src"):
                url = img_tag.get("data-src")
            self.novel_cover = self.absolute_url(url)

        author_tag = soup.select_one('.novel-info .author span[itemprop="author"]')
        self.novel_author = author_tag.text.strip() if author_tag else "Unknown"

        summary_div = soup.select_one(".summary .content")
        self.novel_synopsis = summary_div.get_text("\n\n").strip() if summary_div else ""

        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []

        # Parse First Page
        self.parse_chapter_list(soup)

        # Handle Pagination
        pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        if pagination_links:
            try:
                last_page = pagination_links[-1]
                href = last_page.get("href")
                common_url = self.absolute_url(href).split("?")[0]
                query = parse_qs(urlparse(href).query)
                page_params = query.get("page", ["0"])
                
                # Usually page 0 is the first page we already parsed, but FanMTL pages can be tricky.
                # We check all pages to be safe.
                page_count = int(page_params[0])
                wjm = query.get("wjm", [""])[0]
                
                ajax_headers = {"X-Requested-With": "XMLHttpRequest"}

                # Start from page 1 if page 0 is the landing page
                for page in range(0, page_count + 1):
                    # Skip page 0 if we assume it's the main page (optional optimization)
                    # But often safe to just re-parse to ensure complete list
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    page_soup = self.get_soup_safe(url, headers=ajax_headers)
                    self.parse_chapter_list(page_soup)
                    
            except Exception as e:
                logger.error(f"Pagination failed: {e}")

        # Sort and deduplicate
        self.chapters = list({c['url']: c for c in self.chapters}.values())
        self.chapters.sort(key=lambda x: x["id"])

    def parse_chapter_list(self, soup):
        if not soup: return
        # [FIXED] More generic selector
        for a in soup.select(".chapter-list a"):
            try:
                url = self.absolute_url(a["href"])
                title = a.select_one(".chapter-title")
                title = title.text.strip() if title else a.text.strip()
                
                self.chapters.append(Chapter(
                    id=len(self.chapters) + 1,
                    volume=1,
                    url=url,
                    title=title,
                ))
            except: pass

    def download_chapter_body(self, chapter):
        try:
            soup = self.get_soup_safe(chapter["url"])
            body = soup.select_one("#chapter-article .chapter-content")
            return self.cleaner.extract_contents(body).strip() if body else ""
        except Exception:
            return ""
