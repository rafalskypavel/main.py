import requests  # Импорт модуля requests для отправки HTTP-запросов
from bs4 import BeautifulSoup  # Импорт модуля BeautifulSoup для парсинга HTML-кода
import pandas as pd  # Импорт модуля pandas для работы с данными
import time  # Импорт модуля time для измерения времени выполнения
from requests.exceptions import ConnectTimeout  # Импорт исключения ConnectTimeout из модуля requests.exceptions
import concurrent.futures  # Импорт модуля concurrent.futures для параллельного выполнения задач

URLS = [
    ("https://bb-engineering.ru/pnevmaticheskie-privody/servoprivody-dlya-process-tehniki-ru/camozzi/", 73),
    ("https://bb-engineering.ru/pnevmaticheskie-privody/privody-s-napravlyayuschimi/camozzi/", 113),
    ("https://bb-engineering.ru/pnevmaticheskie-privody/shtokovye-cilindry/camozzi/", 360),
    ("https://bb-engineering.ru/pnevmaticheskie-privody/nepolnopovorotnye-privody/camozzi/", 4),
    ("https://bb-engineering.ru/pnevmaticheskie-privody/besshtokovye-cilindry/camozzi/", 6),
    ("https://bb-engineering.ru/pnevmaticheskie-privody/stopornye-cilindry/camozzi/", 11),
    ("https://bb-engineering.ru/pnevmaticheskie-privody/tandem-cilindry-i-mnogopozicionnye-cilindry/camozzi/", 17),
    ("https://bb-engineering.ru/pnevmaticheskie-privody/krepleniya-i-prinadlezhnosti-dlya-cilindrov/camozzi/", 38),
    ("https://bb-engineering.ru/raspredeliteli-i-klapani/raspredeliteli-s-elektro-i-pnevmoupravleniem/camozzi/", 90),
    ("https://bb-engineering.ru/raspredeliteli-i-klapani/raspredeliteli-s-mehanicheskim-i-ruchnym-upravleniem/camozzi/",
     11),
    ("https://bb-engineering.ru/raspredeliteli-i-klapani/otsechnye-klapany/camozzi/", 3),
    ("https://bb-engineering.ru/raspredeliteli-i-klapani/drosseli/camozzi/", 13),
    ("https://bb-engineering.ru/raspredeliteli-i-klapani/pnevmoostrova/camozzi/", 3),
    ("https://bb-engineering.ru/raspredeliteli-i-klapani/proporcionalnye-klapany/camozzi/", 21),
    ("https://bb-engineering.ru/raspredeliteli-i-klapani/prinadlezhnosti-dlya-raspredeliteley/camozzi/", 34),
    ("https://bb-engineering.ru/podgotovka-szhatogo-vozduha/kombinaciya-blokov-podgotovki-vozduha/camozzi/", 5),
    ("https://bb-engineering.ru/podgotovka-szhatogo-vozduha/filtry/camozzi/", 18),
    ("https://bb-engineering.ru/podgotovka-szhatogo-vozduha/regulyatory/camozzi/", 20),
    ("https://bb-engineering.ru/podgotovka-szhatogo-vozduha/filtry-regulyatory/camozzi/", 38),
    ("https://bb-engineering.ru/podgotovka-szhatogo-vozduha/masloraspyliteli/camozzi/", 2),
    ("https://bb-engineering.ru/podgotovka-szhatogo-vozduha/otsechnye-klapany-i-klapany-plavnogo-puska/camozzi/", 4),
    ("https://bb-engineering.ru/podgotovka-szhatogo-vozduha/kollektory/camozzi/", 2),
    ("https://bb-engineering.ru/podgotovka-szhatogo-vozduha/usiliteli-davleniya/camozzi/", 1),
    ("https://bb-engineering.ru/podgotovka-szhatogo-vozduha/indikatory-davleniya/camozzi/", 3),
    ("https://bb-engineering.ru/podgotovka-szhatogo-vozduha/prinadlezhnosti-dlya-podgotovki-vozduha/camozzi/", 5),
    ("https://bb-engineering.ru/fitingi-i-pnevmoshlangi/cangovye-fitingi/camozzi/", 73),
    ("https://bb-engineering.ru/fitingi-i-pnevmoshlangi/nippelnye-fitingi/camozzi/", 28),
    ("https://bb-engineering.ru/fitingi-i-pnevmoshlangi/fitingi-nippelnye/camozzi/", 1),
    ("https://bb-engineering.ru/fitingi-i-pnevmoshlangi/fitingi-obzhimnye/camozzi/", 19),
    ("https://bb-engineering.ru/fitingi-i-pnevmoshlangi/rezbovye-fitingi/camozzi/", 23),
    ("https://bb-engineering.ru/fitingi-i-pnevmoshlangi/bystrosemnye-soedineniya/camozzi/", 5),
    ("https://bb-engineering.ru/fitingi-i-pnevmoshlangi/glushiteli/camozzi/", 3),
    ("https://bb-engineering.ru/fitingi-i-pnevmoshlangi/kollektory/camozzi/", 2),
    ("https://bb-engineering.ru/fitingi-i-pnevmoshlangi/uplotnitelnye-kolca/camozzi/", 2),
    ("https://bb-engineering.ru/pnevmoshlangi-pnevmotrubki-polimernye/camozzi/", 183),
    ("https://bb-engineering.ru/zapornaya-armatura/zatvory-diskovye/camozzi/", 53),
    ("https://bb-engineering.ru/zapornaya-armatura/sharovye-krany/camozzi/", 22),
    ("https://bb-engineering.ru/zapornaya-armatura/elektromagnitnye-klapany/camozzi/", 5),
    ("https://bb-engineering.ru/zapornaya-armatura/sedelnye-klapany/camozzi/", 25),
    ("https://bb-engineering.ru/zapornaya-armatura/klapany-s-pnevmoupravleniem/camozzi/", 2),
    ("https://bb-engineering.ru/zapornaya-armatura/klapany-predohranitelnye/camozzi/", 2),
    ("https://bb-engineering.ru/zapornaya-armatura/privody-dlya-zapornoy-armatury/camozzi/", 73),
    ("https://bb-engineering.ru/zapornaya-armatura/elektropnevmaticheskie-pozicionery/camozzi/", 28),
    ("https://bb-engineering.ru/vakuumnaya-tehnika/generatory-vakuuma/camozzi/", 3),
    ("https://bb-engineering.ru/vakuumnaya-tehnika/vakuumnye-zahvaty/camozzi/", 16),
    ("https://bb-engineering.ru/vakuumnaya-tehnika/vakuumnye-filtry/camozzi/", 2),
    ("https://bb-engineering.ru/vakuumnaya-tehnika/kompensatory/camozzi/", 2),
    ("https://bb-engineering.ru/vakuumnaya-tehnika/prinadlezhnosti-dlya-vakuumnoy-tehniki/camozzi/", 1),
    ("https://bb-engineering.ru/zahvaty/camozzi/", 7),
    ("https://bb-engineering.ru/datchiki/datchiki-polozheniya/camozzi/", 6),
    ("https://bb-engineering.ru/datchiki/datchiki-davleniya-i-vakuuma/camozzi/", 2),
    ("https://bb-engineering.ru/datchiki/datchiki-rashoda/camozzi/", 3),
    ("https://bb-engineering.ru/datchiki/elektromehanicheskie-pereklyuchateli/camozzi/", 1),
    ("https://bb-engineering.ru/datchiki/uzkospecializirovannye-datchiki/camozzi/", 1),
    ("https://bb-engineering.ru/datchiki/prinadlezhnosti-dlya-datchikov/camozzi/", 3),
    ("https://bb-engineering.ru/elektromehanicheskie-sistemy/elektromehanicheskie-privody/camozzi/", 15),
    ("https://bb-engineering.ru/elektromehanicheskie-sistemy/dvigateli/camozzi/", 3),
    ("https://bb-engineering.ru/elektromehanicheskie-sistemy/reduktory/camozzi/", 3),
    ("https://bb-engineering.ru/kontrollery-i-sistemy-upravleniya/kontrollery-dlya-elektrodvigateley/camozzi/", 1),
    (
    "https://bb-engineering.ru/kontrollery-i-sistemy-upravleniya/prinadlezhnosti-dlya-kontrollerov-i-sistem-upravleniya/camozzi/",
    1),

]  # Список URL-адресов и соответствующего количества страниц для парсинга

cards = []  # Пустой список для хранения данных о товарах


def get_html(url):
    """Функция для получения HTML-кода страницы с повторными попытками"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0;Win64)'}  # Заголовки HTTP-запроса
    while True:
        try:
            response = requests.get(url, headers=headers, timeout=10)  # Отправка GET-запроса
            response.raise_for_status()  # Проверка статуса ответа
            return response
        except ConnectTimeout:
            print(f'Превышено время ожидания соединения с: {url}')  # Обработка исключения ConnectTimeout
        except requests.exceptions.RequestException as e:
            print(f'Ошибка при отправке запроса: {e}')  # Обработка исключения RequestException
        print(f'Повторная попытка парсинга страницы: {url}')  # Повторная попытка парсинга страницы при ошибке


def get_content(html):
    """Получение данных со страницы"""
    soup = BeautifulSoup(html.text, 'html.parser')  # Создание объекта BeautifulSoup для парсинга HTML
    items = soup.findAll('div', {'class': 'ty-column4'})  # Поиск всех элементов с указанным классом
    for item in items:
        try:
            link = item.find('div', class_='ut2-gl__name').find('a').get('href')  # Получение ссылки на товар
        except AttributeError:
            link = None
        try:
            name = item.find('div', class_='ut2-gl__name').get_text(strip=True)  # Получение наименования товара
            print(name)
        except AttributeError:
            name = None
        try:
            price = item.find('span', {'class': 'ty-price-num'}).get_text(strip=True)  # Получение цены товара
        except AttributeError:
            price = None

        cards.append({
            'Ссылка': link,
            'Наименование': name,
            'Цена без НДС': price,
        })  # Добавление данных о товаре в список cards


def parse_subgroup(url, subgroup_pages):
    """Парсинг подгруппы товаров"""
    for i in range(1, subgroup_pages + 1):
        page_url = url + f"page-{i}/"  # Формирование URL страницы
        html = get_html(page_url)  # Получение HTML-кода страницы
        if html.status_code == 200:
            print(f'Парсинг страницы: {page_url}')  # Вывод сообщения о парсинге страницы
            get_content(html)  # Получение данных со страницы
        else:
            print(
                f'Ответ сервера: {html.status_code}. Парсинг невозможен для страницы: {page_url}')  # Вывод сообщения о невозможности парсинга


def parser(url_pages):
    """Функция-обертка для парсинга подгруппы товаров"""
    url, pages = url_pages
    parse_subgroup(url, pages)  # Вызов функции парсинга подгруппы товаров


if __name__ == "__main__":
    start_time = time.time()  # Запись текущего времени начала выполнения программы

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Запуск парсинга в нескольких потоках
        executor.map(parser, URLS)

    end_time = time.time()  # Запись текущего времени окончания выполнения программы
    execution_time = end_time - start_time  # Вычисление общего времени выполнения в секундах

    execution_time_minutes = execution_time / 60  # Преобразование времени выполнения в минуты

    print("Итоговое количество товаров: ", len(cards))

    print(f"Время выполнения: {execution_time_minutes} минут")  # Вывод времени выполнения программы в минутах

    dataframe = pd.DataFrame(cards)  # Создание объекта DataFrame из списка cards
    dataframe.to_excel("website_data.xlsx", sheet_name='website_data', index=False)  # Экспорт данных в Excel-файл