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
import undetected_chromedriver as uc
from pyvirtualdisplay import Display
from curl_cffi import requests as cffi_requests
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

logger = logging.getLogger(__name__)

class FanMTLCrawler(Crawler):
    has_mtl = True
    base_url = "https://www.fanmtl.com/"

    def initialize(self):
        # [VIDEO COMPLIANCE] "Scrape slowly"
        # 50 threads triggers the "Behavioral Analysis" block (Video 2)
        # We lower this to 5 to survive.
        self.init_executor(5) 
        
        # 1. Setup the RUNNER
        self.runner = cffi_requests.Session(impersonate="chrome120")
        
        # Use a generic Linux UA that matches the Docker Chromium
        self.user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        
        self.runner.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Referer": "https://www.fanmtl.com/",
        })
        
        # WARP Proxy
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
        logger.info("FanMTL Strategy: Canvas Fonts + Throttling + TLS")

    def sync_cookies_from_driver(self, driver):
        """Extracts valid Cloudflare cookies."""
        cookies = driver.get_cookies()
        found_cf = False
        
        self.runner.cookies.clear()
        
        for cookie in cookies:
            # Only keep essential Cloudflare cookies (Video 1 Tip: Real Request Headers)
            if cookie['name'] in ['cf_clearance', '__cf_bm']:
                self.runner.cookies.set(
                    cookie['name'], 
                    cookie['value'], 
                    domain=cookie.get('domain', ''),
                    path=cookie.get('path', '/')
                )
                if cookie['name'] == 'cf_clearance':
                    found_cf = True
        
        # [IMPORTANT] Update UA to match the browser exactly
        ua = driver.execute_script("return navigator.userAgent")
        self.runner.headers['User-Agent'] = ua
        
        if found_cf:
            logger.info("✅ Cookies Synced")
            self.cookies_synced = True
            return True
        return False

    def simulate_human(self, driver):
        """Moves mouse randomly (Video 2: Simulate human interactions)."""
        try:
            action = ActionChains(driver)
            x = random.randint(0, 500)
            y = random.randint(0, 500)
            action.move_by_offset(x, y).perform()
            action.reset_actions()
        except: pass

    def get_soup_browser(self, url):
        """Uses Undetected-Chromedriver with Virtual Display & Fonts."""
        logger.info(f"🌍 Browser fetching: {url}")
        driver = None
        display = None
        
        try:
            # Start Virtual Display (Passes 'Headless' check)
            display = Display(visible=0, size=(1920, 1080))
            display.start()

            browser_path = shutil.which("chromium") or "/usr/bin/chromium"
            driver_path = shutil.which("chromedriver") or "/usr/bin/chromedriver"

            options = uc.ChromeOptions()
            options.add_argument("--no-sandbox") 
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument(f'--proxy-server={self.chrome_proxy}')
            options.add_argument("--disable-popup-blocking")
            
            driver = uc.Chrome(
                options=options,
                driver_executable_path=driver_path,
                browser_executable_path=browser_path,
                use_subprocess=True,
                headless=False,
                version_main=120
            )
            
            driver.set_page_load_timeout(120)
            driver.get(url)
            
            start_time = time.time()
            while time.time() - start_time < 90:
                # Handle 520 / 403
                if "520" in driver.title or "403" in driver.title:
                    logger.warning("⚠️ Access Error. Refreshing...")
                    driver.delete_all_cookies()
                    time.sleep(2)
                    driver.refresh()
                    time.sleep(5)
                    continue

                # Handle Cloudflare
                if "Just a moment" in driver.title or "challenge" in driver.page_source.lower():
                    self.simulate_human(driver)
                    # Try clicking iframes
                    try:
                        iframes = driver.find_elements(By.TAG_NAME, "iframe")
                        for frame in iframes:
                            try:
                                if "challenge" in frame.get_attribute("src"):
                                    driver.switch_to.frame(frame)
                                    driver.find_element(By.CSS_SELECTOR, "body").click()
                                    driver.switch_to.default_content()
                            except: driver.switch_to.default_content()
                    except: pass
                    time.sleep(2)
                    continue

                if "fanmtl" in driver.title.lower() or "novel" in driver.title.lower():
                    if self.sync_cookies_from_driver(driver):
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
        """Requests with Random Delays (Video 2 Tip)."""
        # [VIDEO COMPLIANCE] "Incorporate random delays"
        time.sleep(random.uniform(1.5, 3.5)) 
        
        retries = 0
        while retries < 3:
            try:
                response = self.runner.get(url, timeout=15)
                
                if "just a moment" in response.text.lower() or response.status_code == 520:
                    if not self.cookies_synced:
                        self.get_soup_browser(url) 
                        continue
                    
                    time.sleep(5)
                    retries += 1
                    continue

                if response.status_code != 200:
                    response.raise_for_status()
                    
                return self.make_soup(response.content)
            except Exception:
                time.sleep(2)
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

        # Pagination
        pagination_links = soup.select('.pagination a[data-ajax-update="#chpagedlist"]')
        if pagination_links:
            try:
                last_page = pagination_links[-1]
                common_url = self.absolute_url(last_page.get("href")).split("?")[0]
                query = parse_qs(urlparse(last_page.get("href")).query)
                page_count = int(query.get("page", ["0"])[0])
                wjm = query.get("wjm", [""])[0]
                
                for page in range(0, page_count + 1):
                    url = f"{common_url}?page={page}&wjm={wjm}"
                    # Use safe request with delays
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
