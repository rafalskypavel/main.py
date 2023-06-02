import requests
from bs4 import BeautifulSoup
import pandas
import time

URLS = "https://pnevmex.ru/brands/camozzi/"

cards = []


def get_html(url):
    """Получение HTML-кода страницы"""
    headers = {
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0",
    }
    html = requests.get(url, headers=headers)
    return html


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


def parser(url):
    """Рекурсивная функция парсинга"""
    print(f'Парсим данные с: "{url}"', url.title())
    html = get_html(url)
    if html.status_code == 200:
        print(f'Парсинг страницы: {1}')
        get_content(html)
        print(f'Всего: {len(cards)} позиций')

        soup = BeautifulSoup(html.text, 'html.parser')
        itemsLevel = soup.findAll('div', {'class': 'type'})
        for item in itemsLevel:
            print("Опустились в подгруппу ", item.text.strip())
            s = 'https://pnevmex.ru' + item.find('a', {'class': 'type-link'}).get('href')
            print(s, " Ссылка на группу: ", item.text.strip())
            parser(s)

    else:
        print(f'Ответ сервера:{html.status_code}. Парсинг невозможен!')


if __name__ == "__main__":

    # Начало замера времени
    start_time = time.time()

    parser(URLS)

    # Конец замера времени
    end_time = time.time()

    # Вычисление времени выполнения
    execution_time = end_time - start_time
    # Вывод времени выполнения
    print(f"Время выполнения: {execution_time} секунд")


    dataframe = pandas.DataFrame(cards)
    with pandas.ExcelWriter("pnevmex.xlsx",
                            mode="a",
                            engine="openpyxl",
                            if_sheet_exists="overlay", ) as writer:
        dataframe.to_excel(writer, sheet_name='pnevmex')