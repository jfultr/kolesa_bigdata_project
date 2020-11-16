import asyncio
import re
import csv
from aiohttp import ClientSession
from time import sleep
from bs4 import BeautifulSoup


class CSVStorage:
    def __init__(self, path):
        self.path = path

    def create_table(self):
        with open(self.path, 'w', newline='\n') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(['name', 'year', 'city', 'price', 'url'])

    def collect_cache(self):
        with open(self.path, 'r') as file:
            pass
        return []

    def save_row(self, row):
        with open(self.path, 'a', newline='\n') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow([row['name'], row['year'], row['city'], row['price'], row['url']])

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


async def fetch(url, session, storage):
    async with session.get(url) as response:
        page_content = await response.text(encoding='ANSI')
        print(f'fetched {url}')
        data = read_html(page_content)
        storage.save_page(data)


async def bound_fetch(url, session, storage, sm):
    try:
        async with sm:
            return await fetch(url, session, storage)
    except TimeoutError as e:
        print(e)
        sleep(30)


async def parse_data(storage, pages_count):
    headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
    sm = asyncio.Semaphore(50)
    tasks = []
    async with ClientSession(headers=headers) as session:
        for page_num in range(1, pages_count + 1):
            url = f'https://kolesa.kz/cars/?page={page_num}'
            task = asyncio.ensure_future(bound_fetch(url, session, storage, sm))
            tasks.append(task)

        result_cor = await asyncio.gather(*tasks)
    return result_cor


def format_data(name, year, price, city, url):
    name = name.strip()
    year = year.strip()[:4]
    price = price.strip()
    price = re.sub('\xa0', '', price)
    price = re.sub('₸', '', price)
    city = city.strip()
    url = ('https://kolesa.kz' + url.strip())
    car = {'name': name, 'year': year, 'city': city, 'price': price, 'url': url}
    return car


def read_html(html_text):
    cars = []
    soup = BeautifulSoup(html_text, 'html.parser')
    blocks = soup('div', class_='row vw-item list-item a-elem')
    blocks_blue = soup('div', class_='row vw-item list-item blue a-elem')
    blocks_yellow = soup('div', class_='row vw-item list-item yellow a-elem')
    all_blocks = blocks + blocks_blue + blocks_yellow
    for block in all_blocks:
        name = block.find('span', class_='a-el-info-title').text
        year = block.find('div', class_='a-search-description').text
        price = block.find('span', class_='price').text
        city = block.find('div', class_='list-region').text
        url = block.find('a', class_='list-link ddl_product_link').get('href')

        cars.append(format_data(name, year, price, city, url))
    return cars


def run_parser(path, pages_count):
    # creating CSV storage
    storage = CSVStorage(path)

    # parsing data
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(parse_data(storage, pages_count))
    loop.run_until_complete(future)
