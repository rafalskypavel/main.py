import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from requests.exceptions import ConnectTimeout

URLS = [

    "https://pnevmex.ru/catalog/bloki-podgotovki-vozduha/bloki-podgotovki-vozduha-camozzi/",
    "https://pnevmex.ru/catalog/pnevmaticheskie-raspredeliteli/firma-camozzi/",
    "https://pnevmex.ru/catalog/elektromagnitnye-katushki-i-razemy/solenoidy-dlya-raspredelitelej-camozzi-cerij-a-ap-cfb-3-4-9-na/",
    "https://pnevmex.ru/catalog/drosseli-pnevmaticheskie/drosseli-pnevmaticheskie-camozzi/",
    "https://pnevmex.ru/catalog/glushiteli-pnevmaticheskie/glushiteli-pnevmaticheskie-camozzi/",
    "https://pnevmex.ru/catalog/pnevmocilindry-i-elektrocilindry/camozzi/",
    "https://pnevmex.ru/catalog/pnevmaticheskie-truboprovody/pnevmaticheskie-truboprovody-camozzi/",
    "https://pnevmex.ru/catalog/fitingi/fitingi-camozzi/",
    "https://pnevmex.ru/catalog/datchiki-i-rele/datchiki-i-rele-camozzi/",
    "https://pnevmex.ru/catalog/funkcionalnye-klapany-i-logicheskie-elementy/avtomaticheskie-klapany-i-logicheskie-elementy-camozzi/",
    "https://pnevmex.ru/catalog/manometry-pnevmaticheskie/manometry-pnevmaticheskie-camozzi/",
    "https://pnevmex.ru/catalog/remkomplekty-dlya-pnevmocilindrov-i-raspredelitelej/remkomplekty-dlya-pnevmocilindrov-i-raspredelitelej-camozzi/",
    "https://pnevmex.ru/catalog/proporcionalnaya-tehnika/proporcionalnaya-tehnika-camozzi/",
    "https://pnevmex.ru/catalog/vakuumnaya-tehnika/vakuumnaya-tehnika-camozzi/",
    "https://pnevmex.ru/catalog/pnevmaticheskie-shvaty/pnevmaticheskie-shvaty-camozzi/",
    "https://pnevmex.ru/catalog/solenoidnye-klapany/solenoidnye-klapany-camozzi--serii-cfb--5946/",
    "https://pnevmex.ru/catalog/pnevmoupravlyaemye--otsechnye-i-perezhimnye-klapany/pnevmoupravlyaemye-otsechnye-klapany-camozzi--seriya-cka/",
    "https://pnevmex.ru/catalog/sharovye-krany/sharovye-krany-camozzi/",
    "https://pnevmex.ru/catalog/privody-pnevmaticheskie-povorotnye/pnevmoprivody-camozzi-povorotnye-seriya-ca/",

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
    items = soup.findAll('div', {'class': 'flexRow product'})
    for item in items:
        try:
            link = 'https://pnevmex.ru' + item.find('div', class_='product-block product-info').find('a').get('href')
        except AttributeError:
            link = None
        try:
            name = item.find('div', class_='product-block product-info').a.get_text(strip=True)
        except AttributeError:
            name = None
        try:
            unit = item.find('div', class_='product-quantity__type').get_text(strip=True)
        except AttributeError:
            unit = None
        try:
            price = item.find('div', {'class': 'product-block product-props'}).div.span.get_text(strip=True).replace(
                ' ', '').replace('руб.', '')
        except AttributeError:
            price = None

        cards.append({
            'Ссылка': link,
            'Наименование': name,
            'Единица измерения': unit,
            'Цена без НДС': price,
        })


def get_page_count(url):
    """Функция для получения количества страниц в группе"""
    html = get_html(url)
    if html.status_code == 200:
        soup = BeautifulSoup(html.text, 'html.parser')
        pagination = soup.find('div', {'class': 'pagination'})
        if pagination is not None:
            pages = pagination.findAll('a')
            last_page = pages[-1].text.strip() if pages else 1
            return int(last_page)
    return 1


def parse_subgroup(url, visited_urls, parsed_urls):
    # Если URL уже посещен, выходим из функции
    if url in visited_urls:
        return

    visited_urls.add(url)

    html = get_html(url)

    if html.status_code == 200:
        print(f'Парсинг страницы: {url}')

        get_content(html)

        print(f'Всего: {len(cards)} позиций')

        soup = BeautifulSoup(html.text, 'html.parser')
        pagination = soup.find('div', {'class': 'pagination'})
        if pagination is not None:
            pages = pagination.findAll('a')
            for page in pages:
                page_url = 'https://pnevmex.ru' + page['href']
                if page_url != url and page_url not in parsed_urls:
                    parse_subgroup(page_url, visited_urls.copy(), parsed_urls)

        itemsLevel = soup.findAll('div', {'class': 'type'})
        for item in itemsLevel:
            print("Опустились в подгруппу ", item.text.strip())
            subgroup_url = 'https://pnevmex.ru' + item.find('a', {'class': 'type-link'}).get('href')
            print(subgroup_url, " Ссылка на группу: ", item.text.strip())
            subgroup_pages = get_page_count(subgroup_url)
            if subgroup_pages > 1:
                parse_subgroup(subgroup_url, visited_urls.copy(), parsed_urls)
            else:
                parse_subgroup(subgroup_url, visited_urls.copy(), parsed_urls)

        parsed_urls.add(url)

    else:
        print(f'Ответ сервера: {html.status_code}. Парсинг невозможен!')


def parser(url):
    visited_urls = set()
    parsed_urls = set()
    parse_subgroup(url, visited_urls, parsed_urls)


if __name__ == "__main__":
    start_time = time.time()

    for url in URLS:
        parser(url)

    end_time = time.time()
    execution_time = end_time - start_time
    execution_time_minutes = execution_time / 60

    print(f"Время выполнения: {execution_time_minutes} минут")

    dataframe = pd.DataFrame(cards)
    dataframe.to_excel("pnevmex.xlsx", sheet_name='pnevmex', index=False)
