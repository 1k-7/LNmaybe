# -*- coding: utf-8 -*-
import logging
import time
import requests
import shutil
import random
from urllib.parse import urlparse, parse_qs 
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

# [CRITICAL] Use Undetected Chromedriver
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # [TURBO] 50 threads for downloading
        self.init_executor(50) 
        
        # 1. Setup the RUNNER
        self.runner = requests.Session()
        
        # Sync this UA with the browser later
        self.user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        
        self.runner.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
            "Upgrade-Insecure-Requests": "1",
        })
        
        # WARP Proxy
        self.proxy_url = "socks5h://127.0.0.1:40000"
        self.runner.proxies = {
            "http": self.proxy_url,
            "https": self.proxy_url
        }

        # Connection Pool
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.runner.mount("https://", adapter)
        self.runner.mount("http://", adapter)

        self.scraper = self.runner
        self.cookies_synced = False
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info("FanMTL Strategy: Undetected Browser -> Requests Body")

    def sync_cookies_from_driver(self, driver):
        """Extracts valid Cloudflare cookies from Chrome."""
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
        
        # [CRITICAL] Sync UA exactly to the one used by UC
        ua = driver.execute_script("return navigator.userAgent")
        self.runner.headers['User-Agent'] = ua
        
        if found_cf:
            logger.info("✅ Cookies Synced: Cloudflare Clearance Obtained")
            self.cookies_synced = True
            return True
        return False

    def get_soup_browser(self, url):
        """Uses Undetected-Chromedriver with Retry Logic."""
        logger.info(f"🌍 Browser fetching: {url}")
        driver = None
        try:
            # Docker Path Handling - Explicitly point to system chromium
            # This prevents UC from downloading a mismatched binary
            browser_path = shutil.which("chromium") or "/usr/bin/chromium"
            driver_path = shutil.which("chromedriver") or "/usr/bin/chromedriver"

            options = uc.ChromeOptions()
            options.add_argument("--no-sandbox") 
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
            options.add_argument(f'--proxy-server={self.proxy_url}')
            
            # Start UC (Undetected Chromedriver)
            # headless=True in UC is special (it doesn't set the flag the same way)
            driver = uc.Chrome(
                options=options,
                driver_executable_path=driver_path,
                browser_executable_path=browser_path,
                use_subprocess=True,
                headless=True,
                version_main=120 # Try to match if possible, or remove if causing errors
            )
            
            driver.set_page_load_timeout(90)
            driver.get(url)
            
            # [CRITICAL] WAIT FOR COOKIE LOOP
            # We do NOT proceed until we see 'cf_clearance'
            logger.info("⏳ Waiting for Cloudflare clearance...")
            start_time = time.time()
            while time.time() - start_time < 60: # Wait up to 60 seconds
                if "Just a moment" not in driver.title and "challenge" not in driver.page_source.lower():
                    # Check if we have the cookie
                    if self.sync_cookies_from_driver(driver):
                        logger.info("🔓 Bypass Successful!")
                        break
                
                # If stuck, try scrolling or clicking body to trigger JS
                try:
                    driver.find_element(By.TAG_NAME, "body").click()
                except: pass
                
                time.sleep(2)
            
            # Final Sync
            self.sync_cookies_from_driver(driver)
            
            # Return the source
            return self.make_soup(driver.page_source)
            
        except Exception as e:
            logger.error(f"Browser Error: {e}")
            return self.make_soup("<html></html>")
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
                
                # Check for Cloudflare Block
                if "just a moment" in response.text.lower():
                    if not self.cookies_synced:
                        logger.warning("⛔ Request Blocked. Launching solver...")
                        self.get_soup_browser(url) 
                        continue
                    
                    time.sleep(2)
                    retries += 1
                    continue

                response.raise_for_status()
                return self.make_soup(response)
            except Exception:
                time.sleep(1)
                retries += 1
        
        return self.make_soup("<html></html>")

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        
        # 1. Use BROWSER for index
        soup = self.get_soup_browser(self.novel_url)

        possible_title = soup.select_one("h1.novel-title")
        if possible_title:
            self.novel_title = possible_title.text.strip()
        else:
            self.novel_title = "Unknown Title"

        img_tag = soup.select_one("figure.cover img") or soup.select_one(".fixed-img img")
        if img_tag:
            url = img_tag.get("src")
            if "placeholder" in str(url) and img_tag.get("data-src"):
                url = img_tag.get("data-src")
            self.novel_cover = self.absolute_url(url)

        self.novel_author = "Unknown"
        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []

        # 3. Parse Chapters
        self.parse_chapter_list(soup)

        # 4. Handle Pagination
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

                for page in range(0, page_count + 1):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    page_soup = self.get_soup_safe(url, headers=ajax_headers)
                    self.parse_chapter_list(page_soup)
                    
            except Exception as e:
                logger.error(f"Pagination failed: {e}")

        self.chapters = list({c['url']: c for c in self.chapters}.values())
        self.chapters.sort(key=lambda x: x["id"])

        if not self.chapters:
            logger.error("❌ NO CHAPTERS FOUND. Dumping Page Source Snippet:")
            logger.error(str(soup)[:500])

    def parse_chapter_list(self, soup):
        if not soup: return
        
        links = soup.select(".chapter-list a, ul.chapter-list li a")
        if not links:
            links = soup.select("a[href*='/chapter-']")
            
        for a in links:
            try:
                url = self.absolute_url(a["href"])
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
            soup = self.get_soup_safe(chapter["url"])
            body = soup.select_one("#chapter-article .chapter-content, .chapter-content")
            return self.cleaner.extract_contents(body).strip() if body else ""
        except Exception:
            return ""
