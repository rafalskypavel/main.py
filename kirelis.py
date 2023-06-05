import requests  # Импорт модуля для отправки HTTP-запросов
from bs4 import BeautifulSoup  # Импорт модуля для парсинга HTML
import pandas as pd  # Импорт модуля для работы с данными в формате таблицы
import time  # Импорт модуля для работы со временем
from requests.exceptions import ConnectTimeout  # Импорт исключения для обработки таймаута при соединении
import concurrent.futures  # Импорт модуля для выполнения задач в нескольких потоках
from queue import Queue  # Импорт класса очереди для безопасного обмена данными между потоками
from threading import Lock

cards_queue = Queue()  # Создание очереди для хранения данных
lock = Lock()  # Создание объекта блокировки

URLS = [
    "https://www.kirelis.ru/catalog/"

]


def get_html(url):
    """Функция для получения HTML-кода страницы с повторными попытками"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0;Win64)'}
    while True:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()  # Проверка статуса запроса
            return response
        except ConnectTimeout:
            print(f'Превышено время ожидания соединения с: {url}')
            print(f'Повторная попытка парсинга страницы: {url}')

def get_content(html):
    """Получение данных о товаре на странице"""
    soup = BeautifulSoup(html.text, 'html.parser')
    items = soup.find_all('div', {'data-cz': 'card'})
    if items:
        for item in items:
            try:
                availability = item.find('div', class_='availability').get_text(strip=True)
            except AttributeError:
                availability = None
            try:
                link = "https://www.kirelis.ru" + item.find('div', class_='name-item').find('a').get('href')
            except AttributeError:
                link = None
            try:
                name = item.find('div', class_='name-item').get_text(strip=True)
            except AttributeError:
                name = None
            try:
                unit = item.find('div', class_='length-prod hidden-sm hidden-xs').get_text(strip=True)
            except AttributeError:
                unit = None
            try:
                priceR = item.find('div', class_='price-item').get_text(strip=True).replace(' ', '') \
                    .replace('i/м', '').replace('.', ',').replace('i/кг', '').replace('i/шт', '') \
                    .replace("i/упак", '').replace("i/пог,м", "").replace("i/пар", "").replace("i/рул", "")
            except AttributeError:
                priceR = None
            try:
                priceO = item.find('div', class_='price-item-rozn').get_text(strip=True).replace(' ', '') \
                    .replace('i', '').replace('.', ',').replace('опт,', '')
            except AttributeError:
                priceO = None

            lock.acquire()
            try:
                cards_queue.put({
                    'availability': availability,
                    'Ссылка': link,
                    'Наименование': name,
                    'Единица измерения': unit,
                    'Цена c НДС РРЦ': priceR,
                    'Цена c НДС ОПТ': priceO,
                })
            finally:
                lock.release()  # Разблокировка после внесения изменений

def parse_subgroup(url):
    html = get_html(url)
    if html.status_code == 200:
        print(f'Парсинг страницы: {url}')
        get_content(html)
        print(f'Всего: {cards_queue.qsize()} позиций')
        soup = BeautifulSoup(html.text, 'html.parser')
        items = soup.find_all('div', {'class': 'item item-section'})

        for item in items:
            subgroup_url = "https://www.kirelis.ru" + item.find('div', {'class': 'img-item'}).a.get('href')
            print(subgroup_url)
            try:
                parse_subgroup(subgroup_url + "page/all/")
            except requests.exceptions.RequestException:
                parse_subgroup(subgroup_url)

    else:
        print(f'Ответ сервера: {html.status_code}. Парсинг невозможен!')


if __name__ == "__main__":
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Отправляем задачи на парсинг в пул потоков
        future_to_url = {executor.submit(parse_subgroup, url): url for url in URLS}

        # Ожидаем завершения всех задач
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()  # Получаем результат задачи
            except Exception as e:
                print(f'При парсинге страницы {url} возникла ошибка: {e}')

    end_time = time.time()
    execution_time = end_time - start_time
    execution_time_minutes = execution_time / 60

    print(f"Время выполнения: {execution_time_minutes} минут")

    cards = []
    while not cards_queue.empty():
        cards.append(cards_queue.get())

    dataframe = pd.DataFrame(cards)
    dataframe.to_excel("kirelis.xlsx", sheet_name='kirelis', index=False)