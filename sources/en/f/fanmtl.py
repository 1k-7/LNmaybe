# -*- coding: utf-8 -*-
import logging
import requests
import time
import os
from threading import Lock
from urllib.parse import urlparse, parse_qs 
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # Read Config
        self.proxy_url = os.getenv("RENDER_PROXY_URL")
        self.deploy_hook = os.getenv("RENDER_DEPLOY_HOOK")

        if not self.proxy_url:
            raise Exception("❌ RENDER_PROXY_URL missing! Check docker run -e params.")

        self.proxy_url = self.proxy_url.rstrip("/")
        
        # [TURBO] 60 Threads
        self.init_executor(60) 
        
        # [CRITICAL FIX] Connection Pool Size
        # Must exceed thread count (60) to prevent "Connection pool is full" warnings
        self.bridge = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.bridge.mount("https://", adapter)
        self.bridge.mount("http://", adapter)
        
        # [CRITICAL FIX] Thread Lock for Redeploy
        # Prevents 60 threads from triggering the hook at the same time
        self.redeploy_lock = Lock()

        self.cleaner.bad_css.update({'div[align="center"]'})
        
        # Initial Health Check
        self.check_proxy_health()
        logger.info(f"FanMTL Strategy: Render Proxy ({self.proxy_url}) + 60 Threads")

    def check_proxy_health(self):
        """Verifies the Render Proxy is running."""
        try:
            resp = self.bridge.get(f"{self.proxy_url}/", timeout=10)
            if resp.status_code != 200 or resp.json().get("status") != "alive":
                raise Exception("Proxy not alive")
        except Exception as e:
            logger.warning(f"⚠️ Proxy Health Check Failed: {e}")
            # Don't crash, maybe it's just waking up

    def trigger_redeploy(self):
        """Thread-safe IP Rotation."""
        if not self.deploy_hook:
            logger.error("⚠️ No Deploy Hook! Cannot rotate IP.")
            return False

        # Acquire Lock: Only 1 thread can run this block at a time.
        # Others will wait here until it finishes.
        with self.redeploy_lock:
            # 1. Double Check: Maybe another thread already fixed it?
            try:
                if self.bridge.get(f"{self.proxy_url}/", timeout=5).status_code == 200:
                    # logger.info("Using fresh IP from previous redeploy.")
                    return True
            except: pass

            logger.critical("♻️ TRIGGERING RENDER REDEPLOY (Rotating IP)...")
            try:
                # 2. Trigger Hook
                self.bridge.get(self.deploy_hook)
                logger.info("⏳ Waiting for Service Restart (approx 3 mins)...")
                
                # 3. Wait for downtime (service stops)
                time.sleep(30)
                
                # 4. Poll until back online
                for i in range(60): # 10 minutes max
                    try:
                        r = self.bridge.get(f"{self.proxy_url}/", timeout=5)
                        if r.status_code == 200:
                            logger.info("✅ Render Service Back Online! Resuming...")
                            return True
                    except: pass
                    
                    if i % 6 == 0: logger.info("💤 Waiting for Render...")
                    time.sleep(10)
                    
                logger.error("❌ Redeploy timed out.")
                return False
            except Exception as e:
                logger.error(f"Redeploy Error: {e}")
                return False

    def fetch_via_render(self, url):
        """Fetches URL via Proxy."""
        for _ in range(3):
            try:
                resp = self.bridge.post(f"{self.proxy_url}/fetch", json={"url": url}, timeout=60)
                
                # Handle Gateway Errors (Render Restarting)
                if resp.status_code in [502, 503, 504, 404]:
                    time.sleep(5)
                    continue

                data = resp.json()
                
                # Handle Blocks
                if data.get("status") == "blocked":
                    # This call will block this thread until new IP is ready
                    if self.trigger_redeploy():
                        continue 
                    else:
                        return None

                if data.get("status") == "success":
                    return data.get("html")

            except Exception as e:
                time.sleep(2)
        return None

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        html = self.fetch_via_render(self.novel_url)
        if not html: raise Exception("Failed to load novel info via Proxy")

        soup = self.make_soup(html)
        
        self.novel_title = soup.select_one("h1.novel-title").text.strip() if soup.select_one("h1.novel-title") else "Unknown"
        img = soup.select_one("figure.cover img")
        self.novel_cover = self.absolute_url(img['src']) if img else None
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
                page_count = int(query.get("page", ["0"])[0])
                wjm = query.get("wjm", [""])[0]
                
                for page in range(0, page_count + 1):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    html = self.fetch_via_render(url)
                    if html: self.parse_chapter_list(self.make_soup(html))
            except Exception as e:
                logger.error(f"Pagination failed: {e}")

        self.chapters.sort(key=lambda x: x["id"])

    def parse_chapter_list(self, soup):
        if not soup: return
        links = soup.select(".chapter-list a")
        for a in links:
            try:
                url = self.absolute_url(a["href"])
                title = a.select_one(".chapter-title").text.strip()
                self.chapters.append(Chapter(id=len(self.chapters)+1, volume=1, url=url, title=title))
            except: pass

    def download_chapter_body(self, chapter):
        html = self.fetch_via_render(chapter["url"])
        if not html: return ""
        soup = self.make_soup(html)
        body = soup.select_one("#chapter-article .chapter-content")
        return self.cleaner.extract_contents(body).strip() if body else ""
