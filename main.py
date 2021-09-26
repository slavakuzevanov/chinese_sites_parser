import requests
from bs4 import BeautifulSoup
import re
import urllib.parse
import asyncio
import aiohttp
import time


def create_full_link(start_url, list_of_links):
    return [urllib.parse.urljoin(start_url, link) for link in list_of_links]


SPACECHINA_URL = 'http://www.spacechina.com/n25/index.html'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
}

with requests.Session() as s:
    # Получаю разметку главной страницы
    main_page = s.get(SPACECHINA_URL, headers=HEADERS)
    main_page.encoding = main_page.apparent_encoding
    main_page_soup = BeautifulSoup(main_page.text, 'lxml')

    # Получаю html разметку второго меню главной страницы (Центр новостей) и получаю полные ссылки на секции
    menu2 = main_page_soup.find('div', attrs={"class": "common_wrap", "id": "menua2"})
    all_sections_links = [tag['href'] for tag in menu2.find_all('a')]
    all_sections_links = create_full_link(SPACECHINA_URL, all_sections_links)

print(all_sections_links)

SECTIONS_PAGES_DICT = dict()
SECTIONS_DICT = dict()
SECTIONS_DICT_W_KEY_WORDS = dict()
for section_url in all_sections_links:
    SECTIONS_DICT[section_url] = set()
    SECTIONS_PAGES_DICT[section_url] = []
    SECTIONS_DICT_W_KEY_WORDS[section_url] = []


async def get_soup_by_url(url, session, headers=HEADERS):
    try:
        async with session.get(url, headers=headers) as response:
            response_text = await response.text()
            soup = BeautifulSoup(response_text, 'lxml')
            return soup
    except:
        return BeautifulSoup('', 'lxml')


async def get_pages_for_section(section_url, session, headers=HEADERS):
    section_soup = await get_soup_by_url(section_url, session, headers)
    pages_links_for_section_soup = section_soup.find('div', style='display:none')
    try:
        pages_links_for_section = [tag['href'] for tag in pages_links_for_section_soup.find_all('a')]
    except:
        pages_links_for_section = []
    full_pages_links_for_section = create_full_link(section_url, pages_links_for_section)
    print(section_url)
    SECTIONS_PAGES_DICT[section_url] = full_pages_links_for_section
    return full_pages_links_for_section


async def load_pages_for_sections(sections):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for section_url in sections:
            task = asyncio.create_task(get_pages_for_section(section_url, session))
            tasks.append(task)
        await asyncio.gather(*tasks)


async def get_articles_from_first_page_in_section(section_url, session, headers=HEADERS):
    section_soup = await get_soup_by_url(section_url, session, headers)
    first_page_article_links_for_section_soup = section_soup.find('div', class_='olist_box_r').find('span')
    first_page_article_links_for_section = [tag['href'] for tag in
                                            first_page_article_links_for_section_soup.find_all('a')]
    full_first_page_article_links_for_section = create_full_link(section_url, first_page_article_links_for_section)
    SECTIONS_DICT[section_url] = SECTIONS_DICT[section_url].union(full_first_page_article_links_for_section)
    return full_first_page_article_links_for_section


async def load_articles_from_first_page_in_sections(sections):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for section_url in sections:
            task = asyncio.create_task(get_articles_from_first_page_in_section(section_url, session))
            tasks.append(task)
        await asyncio.gather(*tasks)


async def get_articles_for_page_in_section(page_link, section_url, session, headers=HEADERS):
    page_soup = await get_soup_by_url(page_link, session, headers)
    page_article_links = [tag['href'] for tag in page_soup.find_all('a')]
    full_page_article_links = create_full_link(page_link, page_article_links)
    SECTIONS_DICT[section_url] = SECTIONS_DICT[section_url].union(full_page_article_links)
    return full_page_article_links


async def load_articles_for_sections(sections):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for section_url in sections:
            for page_link in SECTIONS_PAGES_DICT[section_url]:
                task = asyncio.create_task(get_articles_for_page_in_section(page_link, section_url, session))
                tasks.append(task)
        await asyncio.gather(*tasks)


async def find_article_with_key_words(session, section, article):
    print(article)
    try:
        async with session.get(article, headers=HEADERS) as resp:
            resp_text = await resp.text()
            if re.search(r'(航天211厂)|(航天科技一院211厂北京装备公司)|(航天科技一院211厂)|(该厂民品子公司长征火箭装备公司的)|(民品子公司天津技术公司)|(天津技术公司)|(211厂军民融合)|('
                     r'长二F火箭)|(天宫一号)|(神舟八)|(神舟九)|(神舟十号)|(长征二F)|(長征五号)|(長征七号)|(長征八号)', resp_text):
                print(f'found key words in {article}')
                SECTIONS_DICT_W_KEY_WORDS[section].append(article)
    except:
        pass
        # return resp_text


async def load_articles_with_key_words(sections):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for section in sections:
            for article in SECTIONS_DICT[section]:
                task = asyncio.create_task(find_article_with_key_words(session, section, article))
                tasks.append(task)
        await asyncio.gather(*tasks)


start_time = time.time()
asyncio.run(load_pages_for_sections(all_sections_links))
print('SECTIONS_PAGES_DICT: ', SECTIONS_PAGES_DICT)
asyncio.run(load_articles_from_first_page_in_sections(all_sections_links))
print('------------------------------------------------------------------------------')
print(SECTIONS_DICT)
asyncio.run(load_articles_for_sections(all_sections_links))
print('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
print(SECTIONS_DICT)
asyncio.run(load_articles_with_key_words(all_sections_links))
print('///////////////////////////////////////////////////////////////////////////////')
print(SECTIONS_DICT_W_KEY_WORDS)


import json
with open('result.json', 'w') as fp:
    json.dump(SECTIONS_DICT_W_KEY_WORDS, fp)
end_time = time.time() - start_time
print(f'Execution time: {end_time} seconds')


