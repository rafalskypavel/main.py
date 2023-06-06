import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from requests.exceptions import ConnectTimeout
import concurrent.futures

URLS = [
   "https://bb-engineering.ru/pnevmaticheskie-privody/"
]

cards = []

def get_html(url):
    """Функция для получения HTML-кода страницы с повторными попытками"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0;Win64)'}
    while True:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()  # Проверяем статус запроса
            return response
        except ConnectTimeout:
            print(f'Превышено время ожидания соединения с: {url}')
        except requests.exceptions.RequestException as e:
            print(f'Ошибка при отправке запроса: {e}')
        print(f'Повторная попытка парсинга страницы: {url}')


def get_content(html):
    """Получение данных со страницы"""
    soup = BeautifulSoup(html.text, 'html.parser')
    items = soup.findAll('div', {'class': 'ty-column4'})
    for item in items:
        try:
            link = item.find('div', class_='ut2-gl__name').find('a').get('href')
        except AttributeError:
            link = None
        try:
            name = item.find('div', class_='ut2-gl__name').get_text(strip=True)
            print(name)
        except AttributeError:
            name = None
        try:
            price = item.find('span', {'class': 'ty-price-num'}).get_text(strip=True)
        except AttributeError:
            price = None

        cards.append({
            'Ссылка': link,
            'Наименование': name,
            'Цена без НДС': price,
        })


def parse_subgroup(url):
    html = get_html(url)

    if html.status_code == 200:
        print(f'Парсинг страницы: {url}')

        get_content(html)

        print(f'Всего: {len(cards)} позиций')
        soup = BeautifulSoup(html.text, 'html.parser')

        itemsLevel = soup.findAll('div', {'class': 'ab-lc-landing'})

        for item in itemsLevel:
            print("Опустились в подгруппу ", item.text.strip())
            subgroup_url = item.find('div', {'class': 'head'}).find("a").get('href')
            print(subgroup_url, " Ссылка на группу: ", item.text.strip())
            subgroup_pages = get_page_count(subgroup_url)
            if subgroup_pages >= 1:
                for i in range(1, subgroup_pages + 1):
                    parse_subgroup(subgroup_url + f"page-{i}/")
            else:
                parse_subgroup(subgroup_url)


    else:
        print(f'Ответ сервера: {html.status_code}. Парсинг невозможен!')


def get_page_count(url):
    """Функция для получения количества страниц в группе"""
    html = get_html(url)
    if html.status_code == 200:
        soup = BeautifulSoup(html.text, 'html.parser')
        pagination_element = soup.find('div', class_='ty-pagination__items')
        last_page = int(pagination_element.find_all('a')[-1]['data-ca-page']) if pagination_element else 1
        return last_page
    return 1

    current_page = 1
    last_page = None

    while last_page is None:
        url = f"{url}page-{current_page}/"
        last_page = get_page_count(url)
        current_page += 1

    print(f"Последняя страница: {last_page}")


def parser(url):
    parse_subgroup(url)


if __name__ == "__main__":
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Запуск парсинга в нескольких потоках
        executor.map(parser, URLS)

    end_time = time.time()
    execution_time = end_time - start_time
    execution_time_minutes = execution_time / 60

    print(f"Время выполнения: {execution_time_minutes} минут")

    dataframe = pd.DataFrame(cards)
    dataframe.to_excel("website_data.xlsx", sheet_name='website_data', index=False)
