# -*- coding: utf-8 -*-
import logging
import time
import requests
import shutil
import os
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
        # [TURBO] 50 threads as requested
        self.init_executor(50) 
        
        # 1. Setup the RUNNER
        self.runner = requests.Session()
        
        # Use a standard, modern Chrome UA (Matched to UC driver below)
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        
        self.runner.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
            "Upgrade-Insecure-Requests": "1",
        })
        
        # WARP Proxy (Essential for your setup)
        self.proxy_url = "socks5h://127.0.0.1:40000"
        self.runner.proxies = {
            "http": self.proxy_url,
            "https": self.proxy_url
        }

        # Optimized Connection Pool
        adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)
        self.runner.mount("https://", adapter)
        self.runner.mount("http://", adapter)

        self.scraper = self.runner
        self.cookies_synced = False
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info("FanMTL Strategy: Undetected Browser -> Requests Body")

    def sync_cookies_from_driver(self, driver):
        """Transplants the 'clearance' cookie from Chrome to Requests."""
        cookies = driver.get_cookies()
        found_cf = False
        for cookie in cookies:
            self.runner.cookies.set(
                cookie['name'], 
                cookie['value'], 
                domain=cookie.get('domain', ''),
                path=cookie.get('path', '/')
            )
            if 'cf_clearance' in cookie['name'] or 'cf_chl' in cookie['name']:
                found_cf = True
        
        # [CRITICAL] Sync UA exactly. 
        # Using a different UA than the one that solved the captcha causes infinite loops.
        ua = driver.execute_script("return navigator.userAgent")
        self.runner.headers['User-Agent'] = ua
        
        if found_cf:
            logger.info("✅ Cookies Synced: Cloudflare Clearance Obtained")
            self.cookies_synced = True
        else:
            logger.warning("⚠️ Browser finished but 'cf_clearance' missing. IP might be flagged.")

    def get_soup_browser(self, url):
        """Launches Undetected-Chromedriver to smash through the Cloudflare Loop."""
        logger.info(f"🌍 Browser fetching: {url}")
        driver = None
        try:
            options = uc.ChromeOptions()
            options.add_argument("--no-sandbox") 
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--headless=new") # Modern headless
            options.add_argument(f'--proxy-server={self.proxy_url}')
            
            # Docker Path Handling
            driver_path = shutil.which("chromedriver") or "/usr/bin/chromedriver"
            browser_path = shutil.which("chromium") or "/usr/bin/chromium"

            driver = uc.Chrome(
                options=options,
                driver_executable_path=driver_path,
                browser_executable_path=browser_path,
                use_subprocess=True,
                version_main=120 # Adjust if your docker chromium version differs
            )
            
            driver.set_page_load_timeout(60)
            driver.get(url)
            
            # [LOOP FIX] Wait logic for "Just a moment"
            logger.info("⏳ Waiting for Cloudflare...")
            time.sleep(5) # Base wait
            
            try:
                # 1. Check if we are stuck on the challenge page
                if "Just a moment" in driver.title or "challenge" in driver.page_source.lower():
                    logger.info("🔒 Challenge Detected. Attempting to click...")
                    
                    # Try clicking the shadow-root checkbox if visible
                    # Note: UC mode often solves this automatically just by being present
                    time.sleep(5) 
                    
                    # Wait for redirect to actual content
                    WebDriverWait(driver, 30).until_not(
                        EC.title_contains("Just a moment")
                    )
            except Exception as e:
                logger.warning(f"Challenge wait timed out (might be passed already): {e}")

            # 2. Verify we are on the novel page
            if "novel" not in driver.current_url and "fanmtl" not in driver.current_url:
                 logger.error(f"❌ Browser stuck on: {driver.current_url}")

            self.sync_cookies_from_driver(driver)
            return self.make_soup(driver.page_source)
            
        except Exception as e:
            logger.error(f"Browser Error: {e}")
            return self.make_soup("<html></html>")
        finally:
            if driver:
                try: driver.quit()
                except: pass

    def get_soup_safe(self, url, headers=None):
        """Fast Request with Fallback."""
        retries = 0
        while retries < 3:
            try:
                req_headers = self.runner.headers.copy()
                if headers: req_headers.update(headers)

                response = self.runner.get(url, headers=req_headers, timeout=15)
                
                # [LOOP FIX] If we see the challenge page text, we are blocked
                if "just a moment" in response.text.lower() or "enable javascript" in response.text.lower():
                    if not self.cookies_synced:
                        logger.warning("⛔ Request Blocked. Launching solver...")
                        self.get_soup_browser(url) 
                        continue
                    
                    # If we are synced but still blocked, our IP/Cookie is burned.
                    # Wait and retry.
                    logger.warning(f"⛔ Blocked with cookies. Cooling down (2s)...")
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
        
        # 1. Initial Browser Pass (Get Cookies & Info)
        soup = self.get_soup_browser(self.novel_url)

        possible_title = soup.select_one("h1.novel-title")
        if possible_title:
            self.novel_title = possible_title.text.strip()
        else:
            self.novel_title = "Unknown Title (Possible Block)"

        img_tag = soup.select_one("figure.cover img") or soup.select_one(".fixed-img img")
        if img_tag:
            url = img_tag.get("src")
            if "placeholder" in str(url) and img_tag.get("data-src"):
                url = img_tag.get("data-src")
            self.novel_cover = self.absolute_url(url)

        self.novel_author = "Unknown"
        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []

        # 2. Parse Chapters (Browser Source)
        self.parse_chapter_list(soup)

        # 3. Handle Pagination (Using Fast Requests)
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

        # Sort and deduplicate
        self.chapters = list({c['url']: c for c in self.chapters}.values())
        self.chapters.sort(key=lambda x: x["id"])

        if not self.chapters:
            logger.error("❌ NO CHAPTERS FOUND. Cloudflare loop active.")

    def parse_chapter_list(self, soup):
        if not soup: return
        
        # Aggressive selector to catch any chapter link
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
