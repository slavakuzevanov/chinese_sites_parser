import requests
from bs4 import BeautifulSoup
import re
import urllib.parse
import asyncio
import aiohttp

SPACECHINA_URL = 'http://www.spacechina.com/n25/index.html'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
}

s = requests.Session()


def create_full_link(start_url, list_of_links):
    return [urllib.parse.urljoin(start_url, link) for link in list_of_links]


# Получаю html разметку главной страницы сайта
main_page = s.get(SPACECHINA_URL, headers=HEADERS)
main_page.encoding = main_page.apparent_encoding
main_page_soup = BeautifulSoup(main_page.text, 'lxml')

# Получаю html разметку второго меню главной страницы (Центр новостей) и получаю полные ссылки на секции
menu2 = main_page_soup.find('div', attrs={"class": "common_wrap", "id": "menua2"})
all_sections_links = [tag['href'] for tag in menu2.find_all('a')]
full_all_section_links = create_full_link(SPACECHINA_URL, all_sections_links)


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


# Асинхронная функция для поиска ключевых слов в статье
async def article_with_key_words_for_section(session, section, article):
    async with session.get(article, headers=HEADERS) as resp:
        resp_text = await resp.text()
        if re.search(r'(航天211厂)|(航天科技一院211厂北京装备公司)|(航天科技一院211厂)|(该厂民品子公司长征火箭装备公司的)|(民品子公司天津技术公司)|(天津技术公司)|(211厂军民融合)|('
                     r'长二F火箭)|(天宫一号)|(神舟八)|(神舟九)|(神舟十号)|(长征二F)|(長征五号)|(長征七号)|(長征八号)', resp_text):
            print(f'found key words in {article}')
            SECTIONS_DICT[section].append(article)


async def load_site_data(sections):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for section_url in sections:
            SECTIONS_DICT[section_url] = []
            section_soup = get_section_soup(section_url=section_url, headers=HEADERS)
            full_all_article_links_for_section = get_full_all_article_links_for_section(section_soup, section_url)
            for article in full_all_article_links_for_section:
                task = asyncio.create_task(article_with_key_words_for_section(session, section_url, article))
                tasks.append(task)
        await asyncio.gather(*tasks)




# Теперь смотрю есть ли в статьях ключевые слова
def get_article_links_where_found(list_of_links):
    article_links_where_found = []
    count = 0
    for link in list_of_links:
        count += 1
        try:
            response = s.get(link, headers=HEADERS)
            response.encoding = response.apparent_encoding
            if re.search(
                    r'(航天211厂)|(航天科技一院211厂北京装备公司)|(航天科技一院211厂)|(该厂民品子公司长征火箭装备公司的)|(民品子公司天津技术公司)|(天津技术公司)|(211厂军民融合)|('
                    r'长二F火箭)|(天宫一号)|(神舟八)|(神舟九)|(神舟十号)|(长征二F)|(長征五号)|(長征七号)|(長征八号)',
                    response.text):
                article_links_where_found.append(link)
            print(f'{count}/{len(list_of_links)}')
        except:
            print(f'{count}/{len(list_of_links)}')
    return article_links_where_found


SECTIONS_DICT = dict()
# Проделываю для каждой секции
asyncio.run(load_site_data(full_all_section_links[2:3]))

print(SECTIONS_DICT)
