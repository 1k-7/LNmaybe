# -*- coding: utf-8 -*-
import logging
import requests
import time
from urllib.parse import urlparse, parse_qs 
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

logger = logging.getLogger(__name__)

# [CONFIGURATION]
# 1. The URL of your Render Proxy App
RENDER_PROXY_URL = "https://YOUR-APP-NAME.onrender.com" 

# 2. The Deploy Hook URL from Render (Settings -> Build & Deploy -> Deploy Hook)
# KEEP THIS SECRET!
RENDER_DEPLOY_HOOK = "https://api.render.com/deploy/srv-xxxxxx?key=xxxxxx"

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # [TURBO] 60 Threads is SAFE because we rotate IPs via Render
        self.init_executor(40) 
        
        self.bridge = requests.Session()
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info(f"FanMTL Strategy: Auto-Rotating Render Proxy")

    def trigger_redeploy_and_wait(self):
        """Hits the Render Deploy Hook and waits for the service to return with a new IP."""
        logger.critical("♻️ TRIGGERING RENDER REDEPLOYMENT (New IP)...")
        
        try:
            # 1. Trigger Hook
            self.bridge.get(RENDER_DEPLOY_HOOK)
            logger.info("⏳ Deployment started. Waiting for service to go down...")
            
            # 2. Wait for service to restart (approx 2-4 minutes)
            # We poll the health endpoint until it returns 200 OK
            health_url = f"{RENDER_PROXY_URL}/"
            
            # Initial wait to let Render start the process
            time.sleep(30)
            
            start_wait = time.time()
            while time.time() - start_wait < 600: # Wait up to 10 mins
                try:
                    resp = self.bridge.get(health_url, timeout=5)
                    if resp.status_code == 200 and resp.json().get("status") == "alive":
                        logger.info("✅ Render Service is BACK ONLINE! Resuming crawl.")
                        return True
                except:
                    # Connection error means it's still deploying/restarting
                    pass
                
                logger.info("💤 Waiting for Render to come online...")
                time.sleep(10)
            
            logger.error("❌ Render Redeploy Timed Out.")
            return False

        except Exception as e:
            logger.error(f"Redeploy Failed: {e}")
            return False

    def fetch_via_render(self, target_url):
        """Sends URL to Render App -> Returns HTML. Handles Block -> Redeploy Loop."""
        retries = 0
        while retries < 3:
            try:
                # Send the URL to your Render Proxy (/fetch endpoint)
                response = self.bridge.post(
                    f"{RENDER_PROXY_URL}/fetch", 
                    json={"url": target_url}, 
                    timeout=60 # Give Render time to fetch
                )
                
                if response.status_code != 200:
                    # 502/503 means Render is restarting/down
                    logger.warning(f"Render Gateway Error ({response.status_code}). Waiting...")
                    time.sleep(10)
                    continue

                data = response.json()
                
                # [BLOCK DETECTION]
                if data.get("status") == "blocked":
                    logger.warning(f"⛔ Render IP Blocked! Initiating Rotation...")
                    
                    # Call the hook to get a new IP
                    if self.trigger_redeploy_and_wait():
                        retries = 0 # Reset retries on success
                        continue
                    else:
                        return None # Redeploy failed
                    
                if data.get("status") == "success":
                    return data.get("html")
                
                # Other errors (404, etc)
                logger.error(f"Proxy Error: {data.get('message')}")
                return None

            except Exception as e:
                logger.warning(f"Bridge Connection Issue: {e}")
                time.sleep(5)
                retries += 1
        
        return None

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        
        html = self.fetch_via_render(self.novel_url)
        if not html:
            raise Exception("Failed to load novel info (Proxy Failure)")

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
                    html_page = self.fetch_via_render(url)
                    if html_page:
                        self.parse_chapter_list(self.make_soup(html_page))
                    
            except Exception as e:
                logger.error(f"Pagination failed: {e}")

        self.chapters = list({c['url']: c for c in self.chapters}.values())
        self.chapters.sort(key=lambda x: x["id"])

        if not self.chapters:
            logger.warning(f"⚠️ 0 Chapters found for {self.novel_title}")

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
        html = self.fetch_via_render(chapter["url"])
        if not html: return ""
        
        soup = self.make_soup(html)
        body = soup.select_one("#chapter-article .chapter-content, .chapter-content")
        return self.cleaner.extract_contents(body).strip() if body else ""
