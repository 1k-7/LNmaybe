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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # [TURBO] 50 threads for downloading (Safe limit for this site)
        self.init_executor(50) 
        
        # 1. Setup the RUNNER (Standard Requests)
        self.runner = requests.Session()
        
        # Standard Headers
        self.runner.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
            "Upgrade-Insecure-Requests": "1",
        })
        
        # WARP Proxy Configuration
        self.proxy_url = "socks5h://127.0.0.1:40000"
        self.runner.proxies = {
            "http": self.proxy_url,
            "https": self.proxy_url
        }

        # Optimize connection pool for speed
        adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)
        self.runner.mount("https://", adapter)
        self.runner.mount("http://", adapter)

        self.scraper = self.runner
        self.cookies_synced = False
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info("FanMTL Strategy: Hybrid (Browser Index -> Requests Body)")

    def sync_cookies_from_driver(self, driver):
        """Extracts valid Cloudflare cookies from Chrome and gives them to Requests."""
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
            logger.info("✅ Cookies Synced: Cloudflare Clearance Obtained")
            self.cookies_synced = True
        else:
            logger.warning("⚠️ Browser finished but 'cf_clearance' missing. IP might be flagged.")

    def get_soup_browser(self, url):
        """Uses Real Chrome to get the page source (Guaranteed Bypass)."""
        logger.info(f"🌍 Browser fetching: {url}")
        driver = None
        try:
            options = ChromeOptions()
            options.add_argument("--no-sandbox") 
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--headless=new") 
            options.add_argument(f'--proxy-server={self.proxy_url}')
            
            driver = create_local(headless=True, options=options)
            driver.set_page_load_timeout(60)
            
            driver.get(url)
            
            # Wait for Cloudflare
            try:
                WebDriverWait(driver, 20).until_not(
                    EC.title_contains("Just a moment")
                )
            except:
                logger.warning("Browser timeout waiting for Cloudflare...")

            # Wait for Chapter List
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".chapter-list, ul.chapter-list"))
                )
            except:
                pass # Might be a different page structure

            self.sync_cookies_from_driver(driver)
            return self.make_soup(driver.page_source)
            
        except Exception as e:
            logger.error(f"Browser Error: {e}")
            raise e
        finally:
            if driver:
                try: driver.quit()
                except: pass

    def get_soup_safe(self, url, headers=None):
        """Standard request with retry logic."""
        retries = 0
        while retries < 3:
            try:
                req_headers = self.runner.headers.copy()
                if headers: req_headers.update(headers)

                response = self.runner.get(url, headers=req_headers, timeout=15)
                
                # Detect Cloudflare Page (It often returns 200 OK)
                if "just a moment" in response.text.lower() or "enable javascript" in response.text.lower():
                    logger.warning("⛔ Request Blocked (Captcha Page). Retrying...")
                    time.sleep(2)
                    retries += 1
                    continue

                response.raise_for_status()
                return self.make_soup(response)
            except Exception:
                time.sleep(1)
                retries += 1
        
        logger.error(f"Failed to fetch {url} via requests.")
        return self.make_soup("<html></html>")

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        
        # [CRITICAL FIX] Use Browser for the Index Page
        # This fixes "No chapters found" by ensuring we see the real page
        soup = self.get_soup_browser(self.novel_url)

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

        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []

        # Parse First Page
        self.parse_chapter_list(soup)

        # Handle Pagination (FanMTL usually has all chapters or simple pagination)
        # We try to use 'requests' for pagination since we now have cookies
        pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        if pagination_links:
            try:
                last_page = pagination_links[-1]
                href = last_page.get("href")
                common_url = self.absolute_url(href).split("?")[0]
                query = parse_qs(urlparse(href).query)
                page_params = query.get("page", ["0"])
                
                # Safety check for pages
                page_count = int(page_params[0])
                wjm = query.get("wjm", [""])[0]
                
                ajax_headers = {"X-Requested-With": "XMLHttpRequest"}

                # Fetch other pages using the NOW SYNCED cookies
                for page in range(0, page_count + 1):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    # We use get_soup_safe here (Requests) for speed
                    page_soup = self.get_soup_safe(url, headers=ajax_headers)
                    self.parse_chapter_list(page_soup)
                    
            except Exception as e:
                logger.error(f"Pagination failed: {e}")

        # Sort and deduplicate
        self.chapters = list({c['url']: c for c in self.chapters}.values())
        self.chapters.sort(key=lambda x: x["id"])

    def parse_chapter_list(self, soup):
        if not soup: return
        # Broad selector to catch multiple layouts
        for a in soup.select(".chapter-list a, ul.chapter-list li a, .chapters a"):
            try:
                url = self.absolute_url(a["href"])
                title_tag = a.select_one(".chapter-title")
                title = title_tag.text.strip() if title_tag else a.text.strip()
                
                self.chapters.append(Chapter(
                    id=len(self.chapters) + 1,
                    volume=1,
                    url=url,
                    title=title,
                ))
            except: pass

    def download_chapter_body(self, chapter):
        try:
            # Uses the fast session with cookies synced from the browser
            soup = self.get_soup_safe(chapter["url"])
            body = soup.select_one("#chapter-article .chapter-content, .chapter-content")
            return self.cleaner.extract_contents(body).strip() if body else ""
        except Exception:
            return ""
