import asyncio
import re
import csv
import os
from aiohttp import ClientSession
from time import sleep
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim


class CSVStorage:
    def __init__(self, path, attributes: list):
        self.path = path
        self.attributes = attributes

    def create_table(self):
        with open(self.path, 'w', newline='\n') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(self.attributes)

    def save_row(self, row: dict):
        with open(self.path, 'a', newline='\n') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow([row[atr] for atr in self.attributes])

    def save_page(self, page):
        for row in page:
            try:
                self.save_row(row)
            except UnicodeEncodeError as e:
                print('Unicode error', e)
                pass

    def save_data(self, data):
        for page in data:
            self.save_page(page)

    def load_columns(self, columns):
        with open(self.path, 'r') as file:
            reader = csv.DictReader(file)
            data = []
            for row in reader:
                data.append(tuple([row[column] for column in columns]))
            return data


async def fetch(url, session, storage):
    async with session.get(url) as response:
        page_content = await response.text(encoding='ANSI')
        print(f'fetched {url}')
        return page_content


async def bound_fetch(url, session, storage, sm):
    try:
        async with sm:
            return await fetch(url, session, storage)
    except TimeoutError as e:
        print(e)
        sleep(30)


async def get_urls_page(brand, page_num, session, urls, sm):
    url = f'https://kolesa.kz/cars/{brand}/?page={page_num}'
    task = await bound_fetch(url, session, urls, sm)
    urls_page = read_html_urls(task)
    urls.save_page(urls_page)


async def parse_urls(brands, urls):
    headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
    sm = asyncio.Semaphore(50)
    tasks = []
    async with ClientSession(headers=headers) as session:
        for brand, count in brands.load_columns(['brand', 'url_count']):
            for page_num in range(1, int(count)//20 + 2):
                task = asyncio.ensure_future(get_urls_page(brand, page_num, session, urls, sm))
                tasks.append(task)
        result_cor = await asyncio.gather(*tasks)
    return result_cor


async def parse_data(storage, urls):
    headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
    sm = asyncio.Semaphore(50)
    async with ClientSession(headers=headers) as session:
        for url in urls:
            task = await asyncio.ensure_future(bound_fetch(url, session, storage, sm))
            car = read_html_car(task)
            storage.save_row(car)


def get_location(city_name):
    geolocator = Nominatim(user_agent='kolesa_parser')
    location = geolocator.geocode(city_name)
    return location.latitude, location.longitude


def format_data(name, year, price, city, url):
    name = name.strip()
    year = year.strip()[:4]
    price = price.strip()
    price = re.sub('\xa0', '', price)
    price = re.sub('₸', '', price)
    city = city.strip()
    latitude, longitude = get_location(city)
    url = ('https://kolesa.kz' + url.strip())
    car = {'name': name, 'year': year, 'latitude': latitude, 'longitude': longitude, 'price': price, 'url': url}
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


def read_html_car(html_text):
    car = {}
    return car


def get_prepared_urls(storage):
    urls = storage.load_columns(['url'])
    unique = set(urls)
    print('Unique urls: ', len(unique))
    return unique


def run_parser(data_path):
    urls_path = os.path.dirname(data_path) + '/' + os.path.basename(data_path)[:-4] + '-urls.csv'
    brands_path = os.path.dirname(data_path) + '/' + os.path.basename(data_path)[:-4] + '-brands.csv'

    # creating urls storage
    urls_storage = CSVStorage(urls_path, ['url'])
    # urls_storage.create_table()

    # creating car brands storage
    brands_storage = CSVStorage(brands_path, ['brand', 'url_count'])

    # TODO: parse all car brands from kolesa, for more dynamic update
    # parse_brands = parse_brands(kolesa.kz)

    # parsing urls
    # loop_urls = asyncio.get_event_loop()
    # future_urls = asyncio.ensure_future(parse_urls(brands_storage, urls_storage))
    # loop_urls.run_until_complete(future_urls)

    # preparing list of needed urls
    prepared_urls = get_prepared_urls(urls_storage)

    # # create data storage
    # data_storage = CSVStorage(data_path)
    # data_storage.create_table()

    # # parsing data
    # loop = asyncio.get_event_loop()
    # future = asyncio.ensure_future(parse_data(data_storage, prepared_urls))
    # loop.run_until_complete(future)
