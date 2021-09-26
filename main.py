import requests
from bs4 import BeautifulSoup
import re
import urllib.parse
import asyncio
import aiohttp
import time


def create_full_link(start_url, list_of_links):
    return [urllib.parse.urljoin(start_url, link) for link in list_of_links]


# Получаю html разметку секции
def get_section_soup(section_url, headers):
    response = s.get(section_url, headers=HEADERS)
    response.encoding = response.apparent_encoding
    section_soup = BeautifulSoup(response.text, 'lxml')
    return section_soup


'''Каждая секция состоит из страниц. На каждой из страниц ссылки на статьи. 
При этом в списке нет первой текущей страницы. Её нужно обрабатывать отдельно'''


# Получаю полные ссылки на страницы секции
def get_full_pages_links_for_section(section_soup, section_url):
    pages_links_for_section_soup = section_soup.find('div', style='display:none')
    pages_links_for_section = [tag['href'] for tag in pages_links_for_section_soup.find_all('a')]
    full_pages_links_for_section = create_full_link(section_url, pages_links_for_section)
    return full_pages_links_for_section


# Получаю все ссылки на статьи для секции
def get_full_all_article_links_for_section(section_soup, section_url):
    # Обрабатываю текущую страницу секции. Получаю ссылки на статьи
    first_page_article_links_for_section_soup = section_soup.find('div', class_='olist_box_r').find('span')
    first_page_article_links_for_section = [tag['href'] for tag in
                                            first_page_article_links_for_section_soup.find_all('a')]
    full_first_page_article_links_for_section = create_full_link(section_url, first_page_article_links_for_section)
    # Получаю полные ссылки на страницы секции
    full_pages_links_for_section = get_full_pages_links_for_section(section_soup, section_url)
    # Итерируюсь по всем ссылкам страниц. Получаю ссылки на все статьи.
    # Добавляю их в сэт всех ссылок на статьи (all_article_links_for_section)
    all_article_links_for_section = set()
    for page_link in full_pages_links_for_section:
        response = s.get(page_link, headers=HEADERS)
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, 'lxml')
        page_article_links_for_section = [tag['href'] for tag in soup.find_all('a')]
        all_article_links_for_section = all_article_links_for_section.union(set(page_article_links_for_section))
    print(len(all_article_links_for_section))
    # Добавляю к списку всех ссылок статей секции список статей секции с первой страницы
    all_article_links_for_section = all_article_links_for_section.union(set(full_first_page_article_links_for_section))
    print(len(all_article_links_for_section))
    full_all_article_links_for_section = create_full_link(section_url, all_article_links_for_section)
    return full_all_article_links_for_section


SPACECHINA_URL = 'http://www.spacechina.com/n25/index.html'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
}

articles = ['http://www.spacechina.com/n25/n2014789/n2014804/index.html',
            'http://www.spacechina.com/n25/n2014789/n2014799/index.html']

start_time = time.time()

s = requests.Session()
# Получаю html разметку главной страницы сайта
main_page = s.get(SPACECHINA_URL, headers=HEADERS)
main_page.encoding = main_page.apparent_encoding
main_page_soup = BeautifulSoup(main_page.text, 'lxml')

# Получаю html разметку второго меню главной страницы (Центр новостей) и получаю полные ссылки на секции
menu2 = main_page_soup.find('div', attrs={"class": "common_wrap", "id": "menua2"})
all_sections_links = [tag['href'] for tag in menu2.find_all('a')]
full_all_section_links = create_full_link(SPACECHINA_URL, all_sections_links)

SECTIONS_DICT = dict()
SECTIONS_DICT_W_KEY_WORDS = dict()

print('SECTIONS:')
print(full_all_section_links)

for section in full_all_section_links:
    section_soup = get_section_soup(section_url=section, headers=HEADERS)
    full_all_article_links_for_section = get_full_all_article_links_for_section(section_soup, section)
    SECTIONS_DICT[section] = full_all_article_links_for_section
    SECTIONS_DICT_W_KEY_WORDS[section] = []


async def get_page_data(session, section, article):
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


async def load_site_data(sections):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for section in sections:
            for article in SECTIONS_DICT[section]:
                task = asyncio.create_task(get_page_data(session, section, article))
                tasks.append(task)
        await asyncio.gather(*tasks)



asyncio.run(load_site_data(full_all_section_links))
end_time = time.time() - start_time
print(f'Execution time: {end_time} seconds')

print(SECTIONS_DICT_W_KEY_WORDS)
