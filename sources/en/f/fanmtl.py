# -*- coding: utf-8 -*-
import logging
import time
import shutil
import os
from urllib.parse import urlparse, parse_qs 
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

# [CRITICAL] Bypass Tools
from DrissionPage import ChromiumPage, ChromiumOptions
from pyvirtualdisplay import Display
from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # [TURBO] 50 Threads
        self.init_executor(50) 
        
        # 1. Setup the RUNNER
        self.runner = cffi_requests.Session(impersonate="chrome120")
        
        # WARP Proxy (Optional - Comment out if VPS IP is better)
        self.proxy_ip = "127.0.0.1"
        self.proxy_port = "40000"
        self.proxies = {
            "http": f"socks5h://{self.proxy_ip}:{self.proxy_port}",
            "https": f"socks5h://{self.proxy_ip}:{self.proxy_port}"
        }
        # self.runner.proxies = self.proxies # Enable if using WARP

        self.scraper = self.runner
        self.cookies_synced = False
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info("FanMTL Strategy: DrissionPage (Strict Wait) -> CFFI")

    def solve_captcha(self, url):
        """Launches DrissionPage to solve Cloudflare Turnstile."""
        logger.info(f"🛡️ Launching Solver: {url}")
        display = None
        page = None
        
        try:
            # 1. Start Virtual Display
            display = Display(visible=0, size=(1920, 1080))
            display.start()

            # 2. Configure Chromium
            co = ChromiumOptions()
            co.set_argument("--no-sandbox")
            co.set_argument("--disable-dev-shm-usage")
            co.set_argument("--disable-gpu")
            co.set_argument("--disable-popup-blocking")
            
            browser_path = shutil.which("chromium") or "/usr/bin/chromium"
            co.set_browser_path(browser_path)

            page = ChromiumPage(addr_or_opts=co)
            
            # 3. Load Page
            page.get(url)
            
            logger.info("⏳ Analyzing Page...")
            start_time = time.time()
            
            # 4. Solve Loop
            while time.time() - start_time < 90:
                title = page.title.lower()
                
                # [CRITICAL FIX] Strict Content Wait
                # Do not proceed unless we see the CHAPTER LIST
                if "just a moment" not in title and "challenge" not in page.html.lower():
                    if page.ele(".chapter-list") or page.ele("ul.chapters") or page.ele(".chapters"):
                        logger.info("🔓 Page Loaded & Chapters Visible!")
                        break
                
                # 520 Error Check
                if "520" in title:
                    logger.warning("⚠️ 520 Error. Refreshing...")
                    page.refresh()
                    time.sleep(5)
                    continue

                # Turnstile Clicker
                try:
                    ele = page.ele('@src^https://challenges.cloudflare.com')
                    if ele:
                        ele.click()
                        time.sleep(2)
                except: pass
                
                time.sleep(1)

            # 5. Extract Session Data
            # Note: DrissionPage cookies() returns a LIST of dicts
            cookies_list = page.cookies() 
            ua = page.run_js("return navigator.userAgent")
            
            # 6. Verify Clearance
            found_cf = False
            self.runner.cookies.clear()
            
            for cookie in cookies_list:
                name = cookie.get('name')
                value = cookie.get('value')
                
                if name == 'cf_clearance':
                    found_cf = True
                
                # Set cookie in runner
                self.runner.cookies.set(name, value, domain=".fanmtl.com")
            
            if found_cf:
                logger.info("✅ CF-Clearance Obtained!")
                self.runner.headers['User-Agent'] = ua
                self.cookies_synced = True
                return page.html
            else:
                logger.error("❌ Solver Failed: No cf_clearance cookie.")
                # Debug Dump - Use this to see what the bot saw
                logger.error(f"Last Title: {page.title}")
                return None

        except Exception as e:
            logger.error(f"Solver Crash: {e}")
            return None
        finally:
            if page: page.quit()
            if display: display.stop()

    def get_soup_safe(self, url, headers=None):
        retries = 0
        while retries < 3:
            try:
                if not self.cookies_synced:
                    self.solve_captcha(url)
                    if not self.cookies_synced:
                        raise Exception("Solver failed")

                response = self.runner.get(url, timeout=20)
                
                if "just a moment" in response.text.lower() or response.status_code in [403, 520, 503]:
                    logger.warning(f"⛔ Token Expired ({response.status_code}). Re-solving...")
                    self.cookies_synced = False
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
        
        # [FIX] Get HTML from browser (which waited for chapters)
        html = self.solve_captcha(self.novel_url)
        if html:
            soup = self.make_soup(html)
        else:
            soup = self.get_soup_safe(self.novel_url)

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

        # Parse from the browser source (Guaranteed to have chapters now)
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
                    page_soup = self.get_soup_safe(url)
                    self.parse_chapter_list(page_soup)
                    
            except Exception as e:
                logger.error(f"Pagination failed: {e}")

        self.chapters = list({c['url']: c for c in self.chapters}.values())
        self.chapters.sort(key=lambda x: x["id"])

        if not self.chapters:
            logger.error("❌ NO CHAPTERS FOUND. Dumping Page Title to verify:")
            if soup.title: logger.error(soup.title.string)

    def parse_chapter_list(self, soup):
        if not soup: return
        # Aggressive selector
        links = soup.select(".chapter-list a, ul.chapter-list li a")
        if not links:
            links = soup.select("a[href*='/chapter-']")
            
        for a in links:
            try:
                url = self.absolute_url(a["href"])
                if any(x['url'] == url for x in self.chapters): continue
                title_tag = a.select_one(".chapter-title")
                title = title_tag.text.strip() if title_tag else a.text.strip()
                self.chapters.append(Chapter(id=len(self.chapters)+1, volume=1, url=url, title=title))
            except: pass

    def download_chapter_body(self, chapter):
        try:
            soup = self.get_soup_safe(chapter["url"])
            body = soup.select_one("#chapter-article .chapter-content, .chapter-content")
            return self.cleaner.extract_contents(body).strip() if body else ""
        except Exception:
            return ""
