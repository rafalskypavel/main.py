import requests
from bs4 import BeautifulSoup
import pandas
import time
from requests.exceptions import ConnectTimeout

URLS = "https://pnevmex.ru/brands/camozzi/"

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
    return cards


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


def parse_subgroup(url, visited_urls):
    # Если URL уже посещен, выходим из функции
    if url in visited_urls:
        return

    # Добавляем URL в список посещенных
    visited_urls.add(url)

    # Получаем HTML-код страницы
    html = get_html(url)

    # Если статус ответа 200 (успешный запрос)
    if html.status_code == 200:
        # Выводим информацию о парсинге текущей страницы
        print(f'Парсинг страницы: {url}')

        # Извлекаем данные со страницы
        get_content(html)

        # Выводим общее количество товаров
        print(f'Всего: {len(cards)} позиций')

        # Проверяем наличие пагинации на подстраницах
        soup = BeautifulSoup(html.text, 'html.parser')
        pagination = soup.find('div', {'class': 'pagination'})
        if pagination is not None:
            pages = pagination.findAll('a')
            # Перебираем каждую страницу пагинации
            for page in pages:
                # Формируем URL страницы
                page_url = 'https://pnevmex.ru' + page['href']
                # Исключаем повторный парсинг текущей страницы
                if page_url != url:
                    # Рекурсивно вызываем функцию parse_subgroup() для парсинга следующей страницы пагинации
                    parse_subgroup(page_url, visited_urls.copy())

        # Рекурсивно вызываем функцию parse_subgroup() для парсинга подгрупп
        itemsLevel = soup.findAll('div', {'class': 'type'})
        for item in itemsLevel:
            print("Опустились в подгруппу ", item.text.strip())
            subgroup_url = 'https://pnevmex.ru' + item.find('a', {'class': 'type-link'}).get('href')
            print(subgroup_url, " Ссылка на группу: ", item.text.strip())
            parse_subgroup(subgroup_url, visited_urls.copy())

    else:
        print(f'Ответ сервера: {html.status_code}. Парсинг невозможен!')


def parser(url):
    """Функция парсинга"""
    visited_urls = set()
    parse_subgroup(url, visited_urls)


if __name__ == "__main__":
    start_time = time.time()
    parser(URLS)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Время выполнения: {execution_time} секунд")
    dataframe = pandas.DataFrame(cards)
    with pandas.ExcelWriter("pnevmex.xlsx", mode="a", engine="openpyxl", if_sheet_exists="overlay") as writer:
        dataframe.to_excel(writer, sheet_name='pnevmex')