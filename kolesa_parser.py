import asyncio
import re
import os
import pandas as pd
from aiohttp import ClientSession
from aiohttp.client_exceptions import ServerDisconnectedError, ClientOSError, InvalidURL
from time import sleep
from bs4 import BeautifulSoup


PARSING_COLUMN = ['brand', 'model', 'year', 'city', 'latitude', 'longitude', 'price', 'mileage', 'capacity', 'body',
                  'color', 'transmission', 'drive', 'url']


class CSVStorage:
    def __init__(self, path, attributes: list):
        self.path = path
        self.attributes = attributes

    def create_empty(self):
        pd.DataFrame(columns=self.attributes).to_csv(self.path, index=False)

    def save(self, data: pd.DataFrame):
        data.to_csv(self.path, mode='a', header=False, index=False)

    def load(self) -> pd.DataFrame:
        return pd.read_csv(self.path)


async def fetch(url, session):
    async with session.get(url) as response:
        page_content = await response.text(encoding='ANSI')
        print('fetched: ', url)
        return page_content, url


async def bound_fetch(url, session, sm):
    try:
        async with sm:
            return await fetch(url, session)
    except (ServerDisconnectedError, ClientOSError) as e:
        print(e)
        sleep(30)
    except InvalidURL:
        return {}, url


async def get_urls_page(brand, page_num, session, urls, sm):
    url = f'https://kolesa.kz/cars/{brand}/?page={page_num}'
    task, _ = await bound_fetch(url, session, sm)
    urls_page = read_html_urls(task)
    urls.save(pd.DataFrame(urls_page))


async def parse_urls(brands, urls):
    headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
    sm = asyncio.Semaphore(50)
    tasks = []

    async with ClientSession(headers=headers) as session:
        for row in brands.load().itertuples():
            for page_num in range(1, int(row.count) // 20 + 2):
                task = asyncio.ensure_future(get_urls_page(row.brand, page_num, session, urls, sm))
                tasks.append(task)
        result_cor = await asyncio.gather(*tasks)
    return result_cor


async def get_car_data(storage, session, url, sm):
    html, url = await asyncio.ensure_future(bound_fetch(url, session, sm))
    try:
        car = read_html_car(html, url)
    except AttributeError:
        print('error')
        return
    storage.save(pd.DataFrame().from_dict(car))


async def parse_data(storage, urls):
    headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
    sm = asyncio.Semaphore(50)
    tasks = []
    async with ClientSession(headers=headers) as session:
        for url in urls:
            task = asyncio.ensure_future(get_car_data(storage, session, url, sm))
            tasks.append(task)
        result_cor = await asyncio.gather(*tasks)
    return result_cor


def process_data(brand, model, year, city, price, mileage, capacity, body, color, transmission, drive, url):
    price = re.sub('\xa0', '', price)
    price = re.sub('₸', '', price)

    if isinstance(mileage, str):
        mileage = re.sub('[^0-9]', '', mileage)

    if isinstance(capacity, str):
        capacity = re.sub('[^0-9.]', '', capacity)

    _ = []
    for var in [brand, model, year, city, price, mileage, capacity, body, color, transmission, drive, price]:
        if isinstance(var, str):
            _.append(var.strip())
        else:
            _.append(None)
    brand, model, year, city, price, mileage, capacity, body, color, transmission, drive, price = _

    car = {'brand': [brand], 'model': [model], 'year': [year], 'city': [city], 'latitude': [None], 'longitude': [None],
           'price': [price], 'mileage': [mileage], 'capacity': [capacity], 'body': [body], 'color': [color],
           'transmission': [transmission], 'drive': [drive], 'url': [url]}
    return car


def read_html_car(html_text, url):
    html_text = html_text.encode('ANSI')
    soup = BeautifulSoup(html_text, 'html.parser')

    brand = soup.find('span', itemprop='brand').text
    model = soup.find('span', itemprop='name').text
    year = soup.find('span', class_='year').text
    price = soup.find('div', class_='offer__price').text

    attribs_name = {
        'Город': None, 'Пробег': None, 'Объем двигателя, л': None, 'Кузов': None, 'Цвет': None, 'Коробка передач': None,
        'Привод': None
    }
    attr_block = soup.find('div', class_='offer__parameters')
    attributes = attr_block.find_all('dl')
    for attr in attributes:
        title = attr.find('dt', class_='value-title').text.strip()
        value = attr.find('dd', class_='value').text

        if title in attribs_name.keys():
            attribs_name[title] = value

    car = process_data(brand, model, year, attribs_name['Город'], price, attribs_name['Пробег'],
                       attribs_name['Объем двигателя, л'], attribs_name['Кузов'], attribs_name['Цвет'],
                       attribs_name['Коробка передач'], attribs_name['Привод'], url)
    return car


def read_html_urls(html_text):
    urls = []
    soup = BeautifulSoup(html_text, 'html.parser')
    blocks = soup('div', class_='row vw-item list-item a-elem')
    blocks_blue = soup('div', class_='row vw-item list-item blue a-elem')
    blocks_yellow = soup('div', class_='row vw-item list-item yellow a-elem')
    all_blocks = blocks + blocks_blue + blocks_yellow
    for block in all_blocks:
        url = block.find('a', class_='list-link ddl_product_link').get('href')
        url = {'url': ('https://kolesa.kz' + url.strip())}
        urls.append(url)
    return urls


def get_prepared_urls(data, urls, update):
    prepared_urls = urls.load()
    prepared_urls.drop_duplicates(keep=False)
    prepared_urls = prepared_urls['url']
    print('Unique urls: ', len(prepared_urls))

    if update:
        parsed_urls = data.load()
        parsed_urls = parsed_urls['url']
        prepared_urls = pd.concat([prepared_urls, parsed_urls, parsed_urls]).drop_duplicates(keep=False)
    return prepared_urls


def run_parser(data_path, update=False):
    urls_path = os.path.dirname(data_path) + '/' + os.path.basename(data_path)[:-4] + '-urls.csv'
    brands_path = os.path.dirname(data_path) + '/' + os.path.basename(data_path)[:-4] + '-brands.csv'

    # creating urls storage
    urls_storage = CSVStorage(urls_path, ['url'])
    brands_storage = CSVStorage(brands_path, ['brand', 'url_count'])
    data_storage = CSVStorage(data_path, PARSING_COLUMN)

    urls_storage.create_empty()
    if not update:
        data_storage.create_empty()

    # TODO: parse all car brands from kolesa, for more dynamic update
    # parse_brands = parse_brands(kolesa.kz)

    # parsing urls
    loop_urls = asyncio.get_event_loop()
    future_urls = asyncio.ensure_future(parse_urls(brands_storage, urls_storage))
    loop_urls.run_until_complete(future_urls)

    # preparing list of needed urls
    prepared_urls = get_prepared_urls(data_storage, urls_storage, update)

    # parsing data
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(parse_data(data_storage, prepared_urls))
    loop.run_until_complete(future)

