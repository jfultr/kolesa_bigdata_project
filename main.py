import asyncio
import sys
import re
import csv
import os.path
import uuid
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
            print(f'saved: {row["name"]}')

    def save_data(self, data):
        for page in data:
            for row in page:
                try:
                    self.save_row(row)
                except UnicodeEncodeError:
                    print('Unicode error')
                    pass


async def fetch(url, session):
    async with session.get(url) as response:
        page_content = await response.text()
        print(f'fetched {url}')
        return page_content


async def bound_fetch(url, session, sm):
    try:
        async with sm:
            return await fetch(url, session)
    except TimeoutError as e:
        print(e)
        sleep(30)


async def parse_data():
    headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
    sm = asyncio.Semaphore(50)
    tasks = []
    async with ClientSession(headers=headers) as session:
        for page_num in range(1, 1001):
            url = f'https://kolesa.kz/cars/?page={page_num}'
            task = asyncio.ensure_future(bound_fetch(url, session, sm))
            tasks.append(task)

        result_cor = await asyncio.gather(*tasks)
    return result_cor


def read_html(html_text):
    cars = []
    soup = BeautifulSoup(html_text, 'html.parser')
    blocks = soup('div', class_='row vw-item list-item a-elem')
    blocks_blue = soup('div', class_='row vw-item list-item blue a-elem')
    blocks_yellow = soup('div', class_='row vw-item list-item yellow a-elem')
    all_blocks = blocks + blocks_blue + blocks_yellow
    for block in all_blocks:
        name = block.find('span', class_='a-el-info-title').text.strip()
        year = block.find('div', class_='a-search-description').text.strip()[:4]
        price = block.find('span', class_='price').text.strip()
        price = re.sub('\xa0', '', price)
        price = re.sub('₸', '', price)
        city = block.find('div', class_='list-region').text.strip()
        url = 'https://kolesa.kz' + block.find('a', class_='list-link ddl_product_link').get('href').strip()
        car = {'name': name, 'year': year, 'city': city, 'price': price, 'url': url}
        cars.append(car)
    return cars


def run_parser(path=''):
    # creating CSV storage
    storage = CSVStorage(path)
    storage.create_table()

    # parsing data
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(parse_data())
    loop.run_until_complete(future)
    pages = future.result()

    # writing data tot storage
    dataset = []
    for page in pages:
        data = read_html(page)
        dataset.append(data)
    storage.save_data(dataset)


if __name__ == "__main__":
    try:
        path_to_file = sys.argv[1]
    except IndexError:
        raise AttributeError('Please enter file name')

    if '\\' not in path_to_file:
        path_to_file = os.path.dirname(__file__) + '/' + path_to_file

    # if script runs without path to CSV file, data will be collected from website
    if os.path.exists(path_to_file):
        name = str(uuid.uuid4())
        path_to_file = os.path.dirname(__file__) + '/' + name + '.csv'
        if input(f'File with this name exists. Program can write new file with name {name}\n'
                 f'Do you want continue "y"?') == 'y':
            pass
        else:
            exit()
    run_parser(path_to_file)