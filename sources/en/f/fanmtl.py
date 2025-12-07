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
        self.proxy_url = os.getenv("RENDER_PROXY_URL")
        self.deploy_hook = os.getenv("RENDER_DEPLOY_HOOK")

        if not self.proxy_url:
            raise Exception("❌ RENDER_PROXY_URL is missing!")

        self.proxy_url = self.proxy_url.rstrip("/")
        
        # [TURBO] 60 Threads (Safe via Proxy)
        self.init_executor(60) 
        self.bridge = requests.Session()
        # [FIX] Larger pool for bot->proxy communication
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.bridge.mount("https://", adapter)
        self.bridge.mount("http://", adapter)

        self.cleaner.bad_css.update({'div[align="center"]'})
        self.check_proxy_health()
        logger.info(f"FanMTL Strategy: Robust Proxy ({self.proxy_url})")

    def check_proxy_health(self):
        try:
            resp = self.bridge.get(f"{self.proxy_url}/", timeout=10)
            if resp.status_code != 200 or resp.json().get("status") != "alive":
                raise Exception("Proxy Unreachable")
        except Exception as e:
            logger.critical(f"❌ PROXY FAILED: {e}")
            raise Exception("Proxy Unreachable")

    def trigger_redeploy(self):
        if not self.deploy_hook: return False
        logger.critical("♻️ TRIGGERING REDEPLOY...")
        try:
            self.bridge.get(self.deploy_hook)
            time.sleep(30)
            for i in range(60):
                try:
                    if self.bridge.get(f"{self.proxy_url}/", timeout=5).status_code == 200:
                        logger.info("✅ Proxy Online!")
                        return True
                except: pass
                time.sleep(10)
            return False
        except: return False

    def fetch_via_render(self, url):
        # [FIX] Increased retries from 3 to 10 for flaky chapters
        for i in range(10):
            try:
                resp = self.bridge.post(f"{self.proxy_url}/fetch", json={"url": url}, timeout=45)
                
                if resp.status_code != 200:
                    time.sleep(5)
                    continue

                data = resp.json()
                
                if data.get("status") == "blocked":
                    if self.trigger_redeploy(): continue 
                    else: return None

                if data.get("status") == "success":
                    html = data.get("html")
                    # [FIX] Validation: Ensure content is not empty
                    if html and len(html) > 500:
                        return html
                    else:
                        logger.warning(f"Empty HTML received. Retrying {i}...")

            except Exception as e:
                time.sleep(2 * (i + 1)) # Backoff
        return None

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        html = self.fetch_via_render(self.novel_url)
        if not html: raise Exception("Failed to load novel info")

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
        
        # [FIX] Retry Indexing if 0 chapters found
        if not self.chapters:
            logger.warning("⚠️ 0 Chapters found. Retrying index fetch...")
            # Trigger a reload or redeploy if needed
            self.trigger_redeploy()
            # Try one more time recursively (careful of infinite loop, but useful here)
            # Just re-calling fetch once to see if fresh IP helps
            html = self.fetch_via_render(self.novel_url)
            if html:
                self.parse_chapter_list(self.make_soup(html))

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
