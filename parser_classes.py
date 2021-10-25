import requests
from bs4 import BeautifulSoup
import re
import urllib.parse
import asyncio
import aiohttp
import time
import pandas as pd
import random

proxy_list = ['http://20.81.106.180:8888', 'http://202.73.51.234:80', 'http://51.195.201.93:80',
              'http://103.152.35.245:80', 'http://104.233.204.77:82', 'http://104.233.204.73:82',
              'http://209.141.55.228:80', 'http://206.253.164.122:80', 'http://139.99.237.62:80',
              'http://51.195.201.93:80', 'http://157.119.234.50:80', 'http://202.73.51.234:80',
              'http://199.19.226.12:80', 'http://103.216.103.25:80', 'http://45.199.148.4:80']


class SpaceChinaParser:
    def __init__(self):
        self.URL = 'http://www.spacechina.com/n25/index.html'

        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
        }

        self.SECTIONS_PAGES_DICT = dict()
        self.SECTIONS_DICT = dict()
        self.SECTIONS_DICT_W_KEY_WORDS = dict()
        self.ARTICLES_URLS = []

    def __create_full_link(self, start_url, list_of_links):
        return [urllib.parse.urljoin(start_url, link) for link in list_of_links]

    async def __get_soup_by_url(self, url, session, headers):
        try:
            async with session.get(url, headers=headers) as response:
                response_text = await response.text()
                soup = BeautifulSoup(response_text, 'lxml')
                return soup
        except:
            return BeautifulSoup('', 'lxml')

    async def __get_pages_for_section(self, section_url, session, headers):
        section_soup = await self.__get_soup_by_url(section_url, session, headers)
        pages_links_for_section_soup = section_soup.find('div', style='display:none')
        try:
            pages_links_for_section = [tag['href'] for tag in pages_links_for_section_soup.find_all('a')]
        except:
            pages_links_for_section = []
        full_pages_links_for_section = self.__create_full_link(section_url, pages_links_for_section)
        print(section_url)
        self.SECTIONS_PAGES_DICT[section_url] = full_pages_links_for_section
        return full_pages_links_for_section

    async def __load_pages_for_sections(self, sections):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for section_url in sections:
                task = asyncio.create_task(self.__get_pages_for_section(section_url, session, self.HEADERS))
                tasks.append(task)
            await asyncio.gather(*tasks)

    async def __get_articles_from_first_page_in_section(self, section_url, session, headers):
        section_soup = await self.__get_soup_by_url(section_url, session, headers)
        first_page_article_links_for_section_soup = section_soup.find('div', class_='olist_box_r').find('span')
        first_page_article_links_for_section = [tag['href'] for tag in
                                                first_page_article_links_for_section_soup.find_all('a')]
        full_first_page_article_links_for_section = self.__create_full_link(section_url,
                                                                            first_page_article_links_for_section)
        self.SECTIONS_DICT[section_url] = self.SECTIONS_DICT[section_url].union(
            full_first_page_article_links_for_section)
        return full_first_page_article_links_for_section

    async def __load_articles_from_first_page_in_sections(self, sections):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for section_url in sections:
                task = asyncio.create_task(
                    self.__get_articles_from_first_page_in_section(section_url, session, self.HEADERS))
                tasks.append(task)
            await asyncio.gather(*tasks)

    async def __get_articles_for_page_in_section(self, page_link, section_url, session, headers):
        page_soup = await self.__get_soup_by_url(page_link, session, headers)
        page_article_links = [tag['href'] for tag in page_soup.find_all('a')]
        full_page_article_links = self.__create_full_link(page_link, page_article_links)
        self.SECTIONS_DICT[section_url] = self.SECTIONS_DICT[section_url].union(full_page_article_links)
        return full_page_article_links

    async def __load_articles_for_sections(self, sections):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for section_url in sections:
                for page_link in self.SECTIONS_PAGES_DICT[section_url]:
                    task = asyncio.create_task(
                        self.__get_articles_for_page_in_section(page_link, section_url, session, self.HEADERS))
                    tasks.append(task)
            await asyncio.gather(*tasks)

    async def __find_article_with_key_words(self, session, section, article, key_words_string):
        print(article)
        try:
            async with session.get(article, headers=self.HEADERS) as resp:
                resp_text = await resp.text()
                if re.search(key_words_string, resp_text):
                    print(f'found key words in {article}')
                    self.ARTICLES_URLS.append(article)
        except:
            pass

    async def __load_articles_with_key_words(self, sections, key_words_string):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for section in sections:
                for article in self.SECTIONS_DICT[section]:
                    task = asyncio.create_task(
                        self.__find_article_with_key_words(session, section, article, key_words_string))
                    tasks.append(task)
            await asyncio.gather(*tasks)

    def run(self, key_words_string):

        with requests.Session() as s:
            # Получаю разметку главной страницы
            main_page = s.get(self.URL, headers=self.HEADERS)
            main_page.encoding = main_page.apparent_encoding
            main_page_soup = BeautifulSoup(main_page.text, 'lxml')

            # Получаю html разметку второго меню главной страницы (Центр новостей) и получаю полные ссылки на секции
            menu2 = main_page_soup.find('div', attrs={"class": "common_wrap", "id": "menua2"})
            all_sections_links = [tag['href'] for tag in menu2.find_all('a')]
            all_sections_links = self.__create_full_link(self.URL, all_sections_links)

            for section_url in all_sections_links:
                self.SECTIONS_DICT[section_url] = set()
                self.SECTIONS_PAGES_DICT[section_url] = []
                self.SECTIONS_DICT_W_KEY_WORDS[section_url] = []

            start_time = time.time()
            asyncio.run(self.__load_pages_for_sections(all_sections_links))
            asyncio.run(self.__load_articles_from_first_page_in_sections(all_sections_links))
            asyncio.run(self.__load_articles_for_sections(all_sections_links))
            asyncio.run(self.__load_articles_with_key_words(all_sections_links, key_words_string))

            pd.DataFrame(self.ARTICLES_URLS, columns=['url']).to_csv(
                'China Aerospace Science and Technology Corporation.csv',
                index=False)
            end_time = time.time() - start_time
            print(f'Execution time: {end_time} seconds')


class jqkaParser:
    def __init__(self):
        self.list_of_section_urls = ['http://yuanchuang.10jqka.com.cn/ycall_list/',
                                'http://invest.10jqka.com.cn/lczx_list/',
                                'http://news.10jqka.com.cn/today_list/',
                                'http://stock.10jqka.com.cn/companynews_list/',
                                'http://stock.10jqka.com.cn/hks/ggdt_list/',
                                'http://news.10jqka.com.cn/guojicj_list/',
                                'http://yuanchuang.10jqka.com.cn/djsjdp_list/',
                                'http://yuanchuang.10jqka.com.cn/mrnxgg_list/',
                                'http://stock.10jqka.com.cn/chuangye/cybdt_list/',
                                'http://news.10jqka.com.cn/fc_list/',
                                'http://stock.10jqka.com.cn/jiepan_list/',
                                'http://stock.10jqka.com.cn/bkfy_list/',
                                'http://stock.10jqka.com.cn/bktt_list/',
                                'http://stock.10jqka.com.cn/hks/hknews_list/',
                                'http://stock.10jqka.com.cn/hks/ggyj_list/',
                                'http://stock.10jqka.com.cn/usstock/gjgs_list/',
                                'http://stock.10jqka.com.cn/usstock/zggxw_list/',
                                'http://bond.10jqka.com.cn/zqzx_list/',
                                'http://news.10jqka.com.cn/jrsc_list/',
                                'http://goodsfu.10jqka.com.cn/futuresnews_list/',
                                'http://goodsfu.10jqka.com.cn/spqh_list/',
                                'http://invest.10jqka.com.cn/lczx_list/',
                                'http://fe.10jqka.com.cn/whzx_list/',
                                'http://news.10jqka.com.cn/cjkx_list/',
                                'http://news.10jqka.com.cn/fortune_list/',
                                'http://stock.10jqka.com.cn/hsdp_list/',
                                'http://stock.10jqka.com.cn/tzjh_list/',
                                'http://stock.10jqka.com.cn/ggqiquan_list/',
                                'http://invest.10jqka.com.cn/sc_list/']

        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
        }

        self.SECTIONS_PAGES_DICT = dict()
        self.SECTIONS_DICT = dict()
        self.SECTIONS_DICT_W_KEY_WORDS = dict()
        self.ARTICLES_URLS = []

    def __create_full_link(self, start_url, list_of_links):
        return [urllib.parse.urljoin(start_url, link) for link in list_of_links]

    async def __get_soup_by_url(self, url, session, headers):
        proxy = random.choice(proxy_list)
        try:
            async with session.get(url, headers=headers, proxy=proxy) as response:
                response_text = await response.text()
                print(url, f'with proxy {proxy} ', 'Access granted')
                soup = BeautifulSoup(response_text, 'lxml')
                return soup
        except:
            print(url, f'with proxy {proxy} ', 'Access denied')
            return BeautifulSoup('', 'lxml')

    async def __get_pages_for_section(self, section_url, session, headers):
        section_soup = await self.__get_soup_by_url(section_url, session, headers)
        try:
            last_page_index = int(section_soup.find('a', {"class": "end"}).string)
            pages_links_for_section = [f'{section_url}index_{index}.shtml' for index in range(1, last_page_index+1)]
        except:
            pages_links_for_section = []
        full_pages_links_for_section = self.__create_full_link(section_url, pages_links_for_section)
        self.SECTIONS_PAGES_DICT[section_url] = full_pages_links_for_section
        return full_pages_links_for_section

    async def __load_pages_for_sections(self, sections):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for section_url in sections:
                task = asyncio.create_task(self.__get_pages_for_section(section_url, session, self.HEADERS))
                tasks.append(task)
            await asyncio.gather(*tasks)

    async def __get_articles_for_page_in_section(self, page_link, section_url, session, headers):
        page_soup = await self.__get_soup_by_url(page_link, session, headers)
        page_article_links = [tag['href'] for tag in page_soup.find_all('a', {"class": "arc-cont"})]
        full_page_article_links = self.__create_full_link(page_link, page_article_links)
        self.SECTIONS_DICT[section_url] = self.SECTIONS_DICT[section_url].union(full_page_article_links)
        return full_page_article_links

    async def __load_articles_for_sections(self, sections):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for section_url in sections:
                for page_link in self.SECTIONS_PAGES_DICT[section_url]:
                    task = asyncio.create_task(
                        self.__get_articles_for_page_in_section(page_link, section_url, session, self.HEADERS))
                    tasks.append(task)
            await asyncio.gather(*tasks)

    async def __find_article_with_key_words(self, session, section, article, key_words_string):
        proxy = random.choice(proxy_list)
        try:
            async with session.get(article, headers=self.HEADERS, proxy=proxy) as resp:
                resp_text = await resp.text()
                print(article, f'with proxy {proxy} ', 'Access granted')
                if re.search(key_words_string, resp_text):
                    print(f'found key words in {article}')
                    self.ARTICLES_URLS.append(article)
        except:
            print(article, f'with proxy {proxy} ', 'Access denied')

    async def __load_articles_with_key_words(self, sections, key_words_string):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for section in sections:
                for article in self.SECTIONS_DICT[section]:
                    task = asyncio.create_task(
                        self.__find_article_with_key_words(session, section, article, key_words_string))
                    tasks.append(task)
            await asyncio.gather(*tasks)

    def run(self, key_words_string):

        with requests.Session() as s:

            for section_url in self.list_of_section_urls:
                self.SECTIONS_DICT[section_url] = set()
                self.SECTIONS_PAGES_DICT[section_url] = []
                self.SECTIONS_DICT_W_KEY_WORDS[section_url] = []

            start_time = time.time()
            asyncio.run(self.__load_pages_for_sections(self.list_of_section_urls))
            asyncio.run(self.__load_articles_for_sections(self.list_of_section_urls))
            asyncio.run(self.__load_articles_with_key_words(self.list_of_section_urls, key_words_string))

            pd.DataFrame(self.ARTICLES_URLS, columns=['url']).to_csv(
                '10jqka.csv',
                index=False)
            end_time = time.time() - start_time
            print(f'Execution time: {end_time} seconds')
