# -*- coding: utf-8 -*-
import logging
import requests
import time
import os
import json
from urllib.parse import urlparse, parse_qs 
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # Read Config from Docker Env
        self.proxy_url = os.getenv("RENDER_PROXY_URL")
        self.deploy_hook = os.getenv("RENDER_DEPLOY_HOOK")

        if not self.proxy_url:
            raise Exception("❌ RENDER_PROXY_URL is missing! Pass it in docker run -e.")

        # Strip trailing slash if present
        self.proxy_url = self.proxy_url.rstrip("/")
        
        # [TURBO] 60 Threads (Safe via Proxy)
        self.init_executor(60) 
        self.bridge = requests.Session()
        self.cleaner.bad_css.update({'div[align="center"]'})
        
        # Health Check
        self.check_proxy_health()
        logger.info(f"FanMTL Strategy: Remote Proxy ({self.proxy_url})")

    def check_proxy_health(self):
        """Verifies the Render Proxy is running the correct code."""
        try:
            logger.info(f"Testing Proxy: {self.proxy_url}/")
            resp = self.bridge.get(f"{self.proxy_url}/", timeout=10)
            if resp.status_code != 200 or resp.json().get("status") != "alive":
                raise Exception(f"Proxy returned {resp.status_code} (Expected 200 'alive')")
            logger.info("✅ Proxy Connection Established!")
        except Exception as e:
            logger.critical(f"❌ PROXY FAILED: {e}")
            logger.critical("Did you deploy the 'main.py' FastAPI code to Render? If you deployed the bot code there, it won't work.")
            raise Exception("Proxy Unreachable")

    def trigger_redeploy(self):
        """Triggers a New IP deployment on Render."""
        if not self.deploy_hook:
            logger.error("⚠️ No RENDER_DEPLOY_HOOK provided. Cannot rotate IP!")
            return False

        logger.critical("♻️ IP BLOCKED! Triggering Render Redeploy...")
        try:
            self.bridge.get(self.deploy_hook)
            logger.info("⏳ Redeploy triggered. Waiting for service restart (approx 2-3 mins)...")
            
            time.sleep(30)
            # Poll for health
            for i in range(60): # Wait up to 10 mins
                try:
                    r = self.bridge.get(f"{self.proxy_url}/", timeout=5)
                    if r.status_code == 200:
                        logger.info("✅ New IP Online! Resuming...")
                        return True
                except: pass
                logger.info("💤 Waiting for Render...")
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
                resp = self.bridge.post(f"{self.proxy_url}/fetch", json={"url": url}, timeout=45)
                
                if resp.status_code != 200:
                    logger.warning(f"Proxy Gateway Error ({resp.status_code})")
                    time.sleep(5)
                    continue

                data = resp.json()
                
                if data.get("status") == "blocked":
                    if self.trigger_redeploy():
                        continue 
                    else:
                        return None

                if data.get("status") == "success":
                    return data.get("html")

            except Exception as e:
                logger.warning(f"Bridge Error: {e}")
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
