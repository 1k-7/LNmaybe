# -*- coding: utf-8 -*-
import logging
import time
import shutil
import random
import json
from urllib.parse import urlparse, parse_qs 
from bs4 import BeautifulSoup
from lncrawl.models import Chapter
from lncrawl.core.crawler import Crawler

# [CRITICAL] Bypass Tools
import undetected_chromedriver as uc
from pyvirtualdisplay import Display
from curl_cffi import requests as cffi_requests
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # [TURBO] 50 threads
        self.init_executor(50) 
        
        # 1. Setup the RUNNER (TLS Impersonation)
        # Upgraded to chrome124 to match modern browser fingerprints
        self.runner = cffi_requests.Session(impersonate="chrome124")
        
        # Use a consistent User-Agent for Linux (Docker)
        self.user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        
        self.runner.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.fanmtl.com/",
            "Upgrade-Insecure-Requests": "1",
            # [FIX] Add Client Hints to satisfy strict Origin servers
            "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Linux"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
        })
        
        # WARP Proxy Configuration
        # If 520 persists, try commenting these out to use your VPS IP directly
        self.proxy_ip = "127.0.0.1"
        self.proxy_port = "40000"
        self.chrome_proxy = f"socks5://{self.proxy_ip}:{self.proxy_port}"
        self.requests_proxy = f"socks5h://{self.proxy_ip}:{self.proxy_port}"

        self.runner.proxies = {
            "http": self.requests_proxy,
            "https": self.requests_proxy
        }

        self.scraper = self.runner
        self.cookies_synced = False
        self.cleaner.bad_css.update({'div[align="center"]'})
        logger.info("FanMTL Strategy: Virtual Display -> Header Alignment -> Cookie Diet")

    def sync_cookies_from_driver(self, driver):
        """Extracts ONLY critical Cloudflare cookies to prevent Header Bloat (520 Error)."""
        cookies = driver.get_cookies()
        found_cf = False
        
        # Clear existing to prevent conflicts
        self.runner.cookies.clear()
        
        for cookie in cookies:
            # [CRITICAL FIX] Only keep Cloudflare cookies. 
            # Junk cookies from ads/tracking often cause 520 errors on the Origin.
            if cookie['name'] in ['cf_clearance', '__cf_bm']:
                self.runner.cookies.set(
                    cookie['name'], 
                    cookie['value'], 
                    domain=cookie.get('domain', ''),
                    path=cookie.get('path', '/')
                )
                if cookie['name'] == 'cf_clearance':
                    found_cf = True
        
        if found_cf:
            logger.info("✅ Cookies Synced: Clean Cloudflare Clearance Obtained")
            self.cookies_synced = True
            return True
        return False

    def simulate_human(self, driver):
        """Moves mouse to trigger passive checks."""
        try:
            action = ActionChains(driver)
            for _ in range(2):
                x = random.randint(0, 300)
                y = random.randint(0, 300)
                action.move_by_offset(x, y).perform()
                action.reset_actions()
                time.sleep(0.2)
        except: pass

    def get_soup_browser(self, url):
        """Uses Undetected-Chromedriver with 520 Error Recovery."""
        logger.info(f"🌍 Browser fetching: {url}")
        driver = None
        display = None
        
        try:
            display = Display(visible=0, size=(1920, 1080))
            display.start()

            browser_path = shutil.which("chromium") or "/usr/bin/chromium"
            driver_path = shutil.which("chromedriver") or "/usr/bin/chromedriver"

            options = uc.ChromeOptions()
            options.add_argument("--no-sandbox") 
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument(f'--proxy-server={self.chrome_proxy}')
            options.add_argument("--disable-popup-blocking")
            # [FIX] Force specific UA in browser to match curl_cffi
            options.add_argument(f"--user-agent={self.user_agent}")
            
            driver = uc.Chrome(
                options=options,
                driver_executable_path=driver_path,
                browser_executable_path=browser_path,
                use_subprocess=True,
                headless=False,
                version_main=124 # Match chrome version if possible
            )
            
            driver.set_page_load_timeout(120)
            driver.get(url)
            
            logger.info("⏳ Waiting for page load (Checking for 520/Challenge)...")
            start_time = time.time()
            
            while time.time() - start_time < 90:
                page_source = driver.page_source.lower()
                title = driver.title.lower()

                # [FIX] Handle 520 Error (Server Reset)
                # If we see 520, we MUST clear cookies and retry.
                if "520" in title or "web server is returning an unknown error" in page_source:
                    logger.warning("⚠️ 520 Origin Error. Cleaning session and reloading...")
                    driver.delete_all_cookies()
                    time.sleep(2)
                    driver.refresh()
                    time.sleep(5)
                    continue

                # Handle "Just a moment"
                if "just a moment" in title or "challenge" in page_source:
                    self.simulate_human(driver)
                    # Try to click iframes
                    try:
                        iframes = driver.find_elements(By.TAG_NAME, "iframe")
                        for frame in iframes:
                            try:
                                if "challenge" in frame.get_attribute("src"):
                                    driver.switch_to.frame(frame)
                                    driver.find_element(By.CSS_SELECTOR, "body").click()
                                    driver.switch_to.default_content()
                            except: 
                                driver.switch_to.default_content()
                    except: pass
                    time.sleep(2)
                    continue

                # Success Condition
                if "fanmtl" in title or "novel" in title or "chapter" in page_source:
                    if self.sync_cookies_from_driver(driver):
                        logger.info("🔓 Bypass Successful!")
                        break
                
                time.sleep(1)
            
            return self.make_soup(driver.page_source)
            
        except Exception as e:
            logger.error(f"Browser Error: {e}")
            return self.make_soup("<html></html>")
        finally:
            if driver:
                try: driver.quit()
                except: pass
            if display:
                try: display.stop()
                except: pass

    def get_soup_safe(self, url, headers=None):
        retries = 0
        while retries < 3:
            try:
                response = self.runner.get(url, timeout=15)
                
                # Check for blocks
                if "just a moment" in response.text.lower() or response.status_code == 520:
                    if not self.cookies_synced:
                        logger.warning("⛔ Request Blocked. Launching solver...")
                        self.get_soup_browser(url) 
                        continue
                    
                    logger.warning(f"⛔ Blocked ({response.status_code}). Retrying...")
                    time.sleep(2)
                    retries += 1
                    continue

                if response.status_code != 200:
                    response.raise_for_status()
                    
                return self.make_soup(response.content)
            except Exception:
                time.sleep(1)
                retries += 1
        
        return self.make_soup("<html></html>")

    def read_novel_info(self):
        logger.debug("Visiting %s", self.novel_url)
        soup = self.get_soup_browser(self.novel_url)

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

        # Pagination using fast cffi_requests
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
