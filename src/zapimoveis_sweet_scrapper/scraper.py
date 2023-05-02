import pandas as pd
import time
from seleniumwire import webdriver
from seleniumwire.utils import decode as sw_decode
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException, NoSuchElementException, TimeoutException, StaleElementReferenceException
import numpy as np
from selenium.webdriver.common.keys import Keys
import random
import re
import sys
from read_response import read_response, get_max_page_from_response, EmptyResponseError, KeyResponseError
from seleniumwire.thirdparty.mitmproxy.exceptions import TcpDisconnect
from ssl import SSLSyscallError
from typing import Union, Literal
import chromedriver_autoinstaller
class Scraper:
    def __init__(self, link:str, proxy_list: pd.DataFrame, 
                 chrome_path: str, chromedriver_path:Union[str, None], output_path: str, 
                 user_agent: Union[str, None]=None, implicitly_wait_time: int=30,
                 page_load_timeout:int=180, max_page_limit:int=100, first_it:bool=True) -> None:
        
        self.__link = link
        self.__user_agent = user_agent
        self.__proxy_list = proxy_list
        self.__proxy_list_counter = 0
        self.__file_name = output_path
        self.__max_page_limter = max_page_limit
        self.__first_it = first_it
        self.__chrome_path = chrome_path
        self.__implicitly_wait_time = implicitly_wait_time
        self.__page_load_timeout = page_load_timeout
        if chromedriver_path is None:
            self.__chromedriver_path = chromedriver_autoinstaller.install()
        else:
           self.__chromedriver_path = chromedriver_path
        pass

    def create_driver(self) -> webdriver.Chrome:
        proxy_list = self.__proxy_list
        proxy_list_counter = self.__proxy_list_counter
        self.__proxy_list_counter += 1

        proxy_username = proxy_list['user'].iloc[proxy_list_counter % len(proxy_list)]
        proxy_password = proxy_list['password'].iloc[proxy_list_counter % len(proxy_list)]
        proxy_ip = proxy_list['ip'].iloc[proxy_list_counter % len(proxy_list)]
        proxy_port = proxy_list['port'].iloc[proxy_list_counter % len(proxy_list)]

        seleniumwire_options = {
            'proxy': {
                'http': f'http://{proxy_username}:{proxy_password}@{proxy_ip}:{proxy_port}',
                'verify_ssl': False,
            },
        }

        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--start-maximized")
        
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("ignore-certificate-errors")

        if self.__user_agent is None:
            options.add_argument(f'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36')
        else:
            options.add_argument(f'user-agent={self.__user_agent}')

        options.binary_location = self.__chrome_path
        
        options.add_experimental_option('prefs', {"download.default_directory": './',
                                                "download.prompt_for_download": False,
                                                "download.directory_upgrade": True,
                                                "plugins.always_open_pdf_externally": True,
                                                "profile.managed_default_content_settings.images": 2})
        
        options.add_experimental_option("excludeSwitches", ["enable-logging"])

        desired_capabilities = DesiredCapabilities.CHROME
        desired_capabilities["goog:loggingPrefs"] = {"performance": "ALL"}

        driver = webdriver.Chrome(service = Service(executable_path= self.__chromedriver_path),
                                options=options, seleniumwire_options=seleniumwire_options,
                                desired_capabilities=desired_capabilities)
        driver.implicitly_wait(self.__implicitly_wait_time)
        driver.set_page_load_timeout(self.__page_load_timeout)
        return driver
    
    def scroll_down(self, driver: webdriver.Chrome, slow: bool=True) -> None:
        if slow:
            total_page_height = driver.execute_script("return document.body.scrollHeight")
            browser_window_height = driver.get_window_size(windowHandle='current')['height']
            current_position = driver.execute_script('return window.pageYOffset')
            while total_page_height - current_position > browser_window_height:
                driver.execute_script(f"window.scrollTo({current_position}, {browser_window_height + current_position});")
                current_position = driver.execute_script('return window.pageYOffset')
                time.sleep(0.2)  # It is necessary here to give it some time to load the content
            return None
        else:
            driver.execute_script("window.scrollBy(0,document.body.scrollHeight)")

    def get_current_page(self, driver: webdriver.Chrome) -> int:
        self.scroll_down(driver, False)
        time.sleep(1)
        self.scroll_down(driver, False)
        pages_container = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//*[@id='app']/section/div[1]/div[3]/div[2]/ul")))
        value = int(pages_container.get_attribute('current-page'))
        return value
        
    def get_max_pages(self, driver: webdriver.Chrome) -> int:
        self.scroll_down(driver, False)
        time.sleep(1)
        self.scroll_down(driver, False)
        pages_container = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//*[@id='app']/section/div[1]/div[3]/div[2]/ul")))
        value = int(pages_container.get_attribute('page-count'))
        if value > self.__max_page_limter:
            return self.__max_page_limter
        else:
            return value

    def next_page(self, driver: webdriver.Chrome) -> None:
        self.scroll_down(driver, False)
        time.sleep(1)
        self.scroll_down(driver, False)
        pages_container = WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, "//*[@id='app']/section/div[1]/div[3]/div[2]/ul")))
        next_page_xpath = "//li/button[@class = 'pagination__button pagination__button--next js-next-page button button-primary button-primary--outline button--regular button--icon']"
        next_page = WebDriverWait(pages_container, 40).until(EC.element_to_be_clickable((By.XPATH, next_page_xpath)))
        driver.execute_script("arguments[0].scrollIntoView();", next_page)
        driver.execute_script("arguments[0].click();", next_page)

    def previous_page(self, driver: webdriver.Chrome) -> None:
        self.scroll_down(driver, False)
        time.sleep(1)
        self.scroll_down(driver, False)
        pages_container = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//*[@id='app']/section/div[1]/div[3]/div[2]/ul")))
        previous_page_xpath = "//li/button[@class = 'pagination__button pagination__button--prev js-prev-page button button-primary button-primary--outline button--regular button--icon']"
        previous_page = WebDriverWait(pages_container, 20).until(EC.element_to_be_clickable((By.XPATH, previous_page_xpath)))
        driver.execute_script("arguments[0].scrollIntoView();", previous_page)
        driver.execute_script("arguments[0].click();", previous_page)

    def check_exists_by_xpath(self, driver, xpath):
        try:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, xpath)))
        except TimeoutException:
            return False
        
        return True

    def next_previous_page(self, driver: webdriver.Chrome) -> None:
        self.next_page(driver)
        time.sleep(5)
        self.previous_page(driver)
        time.sleep(5)
        return None
    
    def get_correct_request_response(self, driver: webdriver.Chrome, retry: int=0) -> bytes:
        requests = driver.requests
        for req in requests:
            if ("listings?category" in req.url) & (req.response is not None):
                json_response = sw_decode(req.response.body, req.response.headers.get('Content-Encoding', 'identity'))
                json_response = json_response.decode("utf8")
                del driver.requests
                return json_response
        if retry < 3:
            del driver.requests
            self.next_previous_page(driver)
            self.get_correct_request_response(driver, retry + 1)
        else:
            raise EmptyResponseError
        
    def scrape(self, driver: webdriver.Chrome, retry_count: int=0, custom_link:str=None) -> None:
        if custom_link is None:
            link = self.__link
        else: link = custom_link
        try:
            driver.get(link)
            driver.refresh()
            self.next_previous_page(driver)
            
            current_page = None
            max_pages = self.get_max_pages(driver)
            while self.get_current_page(driver) <= max_pages:
                current_page = self.get_current_page(driver)
                if self.__first_it:
                    response = self.get_correct_request_response(driver)
                    max_pages = get_max_page_from_response(response)
                    df_vendas = read_response(response)
                    print(df_vendas)
                    df_vendas.to_csv(self.__file_name, sep=";", index=False)
                    self.__first_it = False
                else:
                    response = self.get_correct_request_response(driver)
                    max_pages = get_max_page_from_response(response)
                    df_vendas = read_response(response)
                    print(df_vendas)
                    df_vendas.to_csv(self.__file_name, sep=";", index=False, mode='a', header=False)

                print(f"Scrapped page {current_page}")
                if self.get_current_page(driver) < max_pages:
                    self.next_page(driver)
                else:
                    break
                retry_count = 0
                time.sleep(random.randint(1,2))
        except (WebDriverException, TimeoutException, NoSuchElementException, 
                StaleElementReferenceException, SSLSyscallError, EmptyResponseError,
                KeyResponseError, TcpDisconnect) as err:
            print(err)
            if retry_count < 3:
                print("Falhou no main, tentando novamente")
                f = open("./error.html", "w", encoding="utf-8")
                f.write(driver.page_source)
                f.close()

                driver.quit()
                driver = self.create_driver()
                if current_page is None:
                    self.scrape(driver, retry_count + 1, custom_link=link)
                else:
                    new_link = re.sub(r'pagina=\d+', f'pagina={current_page}', link)
                    self.scrape(driver, retry_count + 1, custom_link=new_link)
            else:
                driver.quit()
                error_type, error_instance, traceback = sys.exc_info()
                raise traceback
        except:
            driver.quit()
            error_type, error_instance, traceback = sys.exc_info()
            raise traceback
        driver.quit()
        print("Finished")
        return None
    
    def run(self):
        driver = self.create_driver()
        self.scrape(driver)