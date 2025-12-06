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
        # [TURBO] 60 threads for downloading
        self.init_executor(60) 
        
        # 1. Setup the RUNNER (Standard Requests)
        self.runner = requests.Session()
        
        # [CRITICAL] Mimic Chrome 120 EXACTLY to match the Browser Solver
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        
        self.runner.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
        })
        
        # WARP Proxy Configuration
        self.proxy_url = "socks5h://127.0.0.1:40000"
        self.runner.proxies = {
            "http": self.proxy_url,
            "https": self.proxy_url
        }

        # Optimize connection pool for speed
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.runner.mount("https://", adapter)
        self.runner.mount("http://", adapter)

        self.scraper = self.runner
        self.cookies_synced = False
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info("FanMTL Strategy: Browser TOC -> Requests Body")

    def sync_cookies_from_driver(self, driver):
        """Extracts valid Cloudflare cookies from Chrome and gives them to Requests."""
        cookies = driver.get_cookies()
        
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
        
        if found_cf:
            logger.info("✅ Cookies Synced: Cloudflare Clearance Obtained")
            self.cookies_synced = True
        else:
            logger.warning("⚠️ Browser finished but 'cf_clearance' missing. IP might be dirty.")

    def get_soup_browser(self, url):
        """Uses Real Chrome to get the page source (Guaranteed Bypass)."""
        logger.info(f"🌍 Browser fetching: {url}")
        driver = None
        try:
            options = ChromeOptions()
            options.add_argument("--no-sandbox") 
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--headless=new") 
            options.add_argument(f"--user-agent={self.user_agent}")
            options.add_argument(f'--proxy-server={self.proxy_url}')
            
            # Anti-detection flags
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            driver = create_local(headless=True, options=options)
            driver.set_page_load_timeout(60)
            
            driver.get(url)
            
            # [CRITICAL] Wait for Cloudflare to pass
            logger.info("⏳ Waiting for page load...")
            time.sleep(5) 
            
            # Force wait for a known element (The Title or Chapter List)
            # This ensures we don't grab the "Just a moment" HTML
            try:
                WebDriverWait(driver, 25).until(
                    lambda d: "fanmtl" in d.title.lower() or "novel" in d.title.lower()
                )
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/chapter-']"))
                )
            except Exception:
                logger.warning("Timed out waiting for chapter list. Dumping source for debug.")

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
        """Standard request with retry logic. Uses synced cookies."""
        retries = 0
        while retries < 3:
            try:
                req_headers = self.runner.headers.copy()
                if headers: req_headers.update(headers)

                response = self.runner.get(url, headers=req_headers, timeout=15)
                
                # [FIX] Detect Cloudflare even on 200 OK
                if "just a moment" in response.text.lower() or "enable javascript" in response.text.lower():
                    if not self.cookies_synced:
                        logger.warning("⛔ Request Blocked. Launching solver...")
                        # If blocked, try to solve ONE time
                        self.get_soup_browser(url) 
                        continue
                    
                    logger.warning(f"⛔ Blocked again. Retrying {retries}/3...")
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
        
        # 1. Use BROWSER for the index to ensure we see chapters
        soup = self.get_soup_browser(self.novel_url)

        # 2. Parse Info
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

        # 3. Parse Chapters (Aggressive Selector)
        self.parse_chapter_list(soup)

        # 4. Handle Pagination (if any)
        # Using requests (get_soup_safe) for subsequent pages is fast
        pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        if pagination_links:
            try:
                last_page = pagination_links[-1]
                href = last_page.get("href")
                common_url = self.absolute_url(href).split("?")[0]
                query = parse_qs(urlparse(href).query)
                page_params = query.get("page", ["0"])
                
                page_count = int(page_params[0])
                wjm = query.get("wjm", [""])[0]
                
                ajax_headers = {"X-Requested-With": "XMLHttpRequest"}

                # Fetch pages 0 to N
                for page in range(0, page_count + 1):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    page_soup = self.get_soup_safe(url, headers=ajax_headers)
                    self.parse_chapter_list(page_soup)
                    
            except Exception as e:
                logger.error(f"Pagination failed: {e}")

        # 5. Deduplicate and Sort
        self.chapters = list({c['url']: c for c in self.chapters}.values())
        self.chapters.sort(key=lambda x: x["id"])

        if not self.chapters:
            logger.error("❌ NO CHAPTERS FOUND. DUMPING PAGE TITLE: " + soup.title.string if soup.title else "No Title")
            logger.error("HTML Snippet: " + str(soup)[:500])

    def parse_chapter_list(self, soup):
        if not soup: return
        
        # [FIX] Aggressive Selector Strategy
        # 1. Try standard list
        links = soup.select(".chapter-list a, ul.chapter-list li a")
        
        # 2. If empty, try finding ANY link with 'chapter-' in href
        if not links:
            links = soup.select("a[href*='/chapter-']")
            
        for a in links:
            try:
                url = self.absolute_url(a["href"])
                # Avoid duplicates inside the loop
                if any(x['url'] == url for x in self.chapters): continue

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
            # Use the high-speed runner
            soup = self.get_soup_safe(chapter["url"])
            body = soup.select_one("#chapter-article .chapter-content, .chapter-content")
            return self.cleaner.extract_contents(body).strip() if body else ""
        except Exception:
            return ""
