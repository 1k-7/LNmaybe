# -*- coding: utf-8 -*-
import logging
import time
import shutil
import random
from urllib.parse import urlparse, parse_qs 
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

# [CRITICAL] Bypass Tools
from DrissionPage import ChromiumPage, ChromiumOptions
from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # [TURBO] 50 Threads
        self.init_executor(50) 
        
        # 1. Setup the RUNNER (TLS Impersonation)
        # Use chrome120 to match the browser
        self.runner = cffi_requests.Session(impersonate="chrome120")
        
        # [CRITICAL] Use LINUX User-Agent to match Docker Container
        # Sending "Windows" UA from a Linux Docker container = Instant Block
        self.user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        
        self.runner.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
        })
        
        # WARP Proxy (Optional - If this fails, comment these lines out to use VPS IP)
        self.proxy_ip = "127.0.0.1"
        self.proxy_port = "40000"
        self.runner.proxies = {
            "http": f"socks5h://{self.proxy_ip}:{self.proxy_port}",
            "https": f"socks5h://{self.proxy_ip}:{self.proxy_port}"
        }

        self.scraper = self.runner
        self.cookies_synced = False
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info("FanMTL Strategy: DrissionPage (Linux UA) -> CFFI")

    def get_cookies_via_drission(self, url):
        """Uses DrissionPage (CDP) to solve Cloudflare."""
        logger.info(f"🛡️ DrissionPage Solving: {url}")
        page = None
        
        try:
            # Configure DrissionPage for Docker
            co = ChromiumOptions()
            co.set_argument("--no-sandbox")
            co.set_argument("--disable-dev-shm-usage")
            co.set_argument("--disable-gpu")
            co.set_argument("--disable-popup-blocking")
            # [CRITICAL] Match the Runner UA
            co.set_argument(f"--user-agent={self.user_agent}")
            # [CRITICAL] Use WARP Proxy in Browser too
            co.set_argument(f"--proxy-server=socks5://{self.proxy_ip}:{self.proxy_port}")
            
            # Auto-find chromium
            browser_path = shutil.which("chromium") or "/usr/bin/chromium"
            co.set_browser_path(browser_path)

            page = ChromiumPage(addr_or_opts=co)
            
            # Load Page
            page.get(url)
            
            logger.info("⏳ Waiting for Turnstile/Challenge...")
            start_time = time.time()
            
            # Wait Loop
            while time.time() - start_time < 60:
                # Check for Success (Title is usually the Novel Title or 'FanMTL')
                title = page.title.lower()
                if "just a moment" not in title and "challenge" not in page.html.lower():
                    if "fanmtl" in title or "novel" in title:
                        break
                
                # Check for 520 Error
                if "520" in title:
                    logger.warning("⚠️ 520 Error. Refreshing...")
                    page.refresh()
                    time.sleep(5)
                    continue

                # DrissionPage auto-handles many turnstiles, but we wait
                time.sleep(1)

            # Extract Cookies
            cookies = page.cookies(as_dict=True)
            
            # Sync to Runner
            self.runner.cookies.clear()
            found_cf = False
            for name, value in cookies.items():
                if name == 'cf_clearance':
                    found_cf = True
                self.runner.cookies.set(name, value, domain=".fanmtl.com")
            
            if found_cf:
                logger.info("✅ COOKIE OBTAINED: cf_clearance found!")
                self.cookies_synced = True
                return page.html
            else:
                logger.error("❌ Failed to get cf_clearance cookie.")
                logger.error(f"Page Title: {page.title}")
                return None

        except Exception as e:
            logger.error(f"Drission Error: {e}")
            return None
        finally:
            if page: 
                try: page.quit()
                except: pass

    def get_soup_safe(self, url, headers=None):
        retries = 0
        while retries < 3:
            try:
                # Solve if needed
                if not self.cookies_synced:
                    self.get_cookies_via_drission(url)
                    if not self.cookies_synced:
                        raise Exception("Cookie sync failed")

                response = self.runner.get(url, timeout=20)
                
                # Check for Block
                if "just a moment" in response.text.lower() or response.status_code in [403, 520, 503]:
                    logger.warning(f"⛔ Blocked ({response.status_code}). Re-solving...")
                    self.cookies_synced = False # Force re-solve next loop
                    time.sleep(2)
                    retries += 1
                    continue

                response.raise_for_status()
                return self.make_soup(response.content)

            except Exception:
                time.sleep(1)
                retries += 1
        
        return self.make_soup("<html></html>")

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        
        # Initial Solve
        html = self.get_cookies_via_drission(self.novel_url)
        if not html:
            # Fallback if browser failed to return source but got cookies
            soup = self.get_soup_safe(self.novel_url)
        else:
            soup = self.make_soup(html)

        possible_title = soup.select_one("h1.novel-title")
        if possible_title:
            self.novel_title = possible_title.text.strip()
        else:
            self.novel_title = "Unknown"

        img_tag = soup.select_one("figure.cover img") or soup.select_one(".fixed-img img")
        if img_tag:
            self.novel_cover = self.absolute_url(img_tag.get("src"))

        self.novel_author = "Unknown"
        self.volumes = [{"id": 1, "title": "Volume 1"}]
        self.chapters = []

        self.parse_chapter_list(soup)

        # Pagination
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
                
                for page in range(0, page_count + 1):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    page_soup = self.get_soup_safe(url)
                    self.parse_chapter_list(page_soup)
                    
            except Exception as e:
                logger.error(f"Pagination failed: {e}")

        self.chapters = list({c['url']: c for c in self.chapters}.values())
        self.chapters.sort(key=lambda x: x["id"])

        if not self.chapters:
            logger.error("❌ NO CHAPTERS FOUND.")

    def parse_chapter_list(self, soup):
        if not soup: return
        links = soup.select(".chapter-list a, ul.chapter-list li a")
        if not links:
            links = soup.select("a[href*='/chapter-']")
            
        for a in links:
            try:
                url = self.absolute_url(a["href"])
                if any(x['url'] == url for x in self.chapters): continue
                title = a.text.strip()
                self.chapters.append(Chapter(id=len(self.chapters)+1, volume=1, url=url, title=title))
            except: pass

    def download_chapter_body(self, chapter):
        try:
            soup = self.get_soup_safe(chapter["url"])
            body = soup.select_one("#chapter-article .chapter-content, .chapter-content")
            return self.cleaner.extract_contents(body).strip() if body else ""
        except Exception:
            return ""
