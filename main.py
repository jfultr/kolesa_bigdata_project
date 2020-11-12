from time import sleep
import asyncio
import sys
from aiohttp import ClientSession
from bs4 import BeautifulSoup


class CSVStorage:
    def __init__(self, path):
        self.path = path

    def collect_cache(self):
        with open(self.path, 'r') as file:
            pass
        return []

    def save_data(self, data):
        pass


async def fetch(url, session):
    async with session.get(url) as response:
        page_content = await response.text()
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
        for page_num in range(1, 2):
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
    print(len(all_blocks))
    for block in all_blocks:
        name = block.find('span', class_='a-el-info-title').text.strip()
        year = block.find('div', class_='a-search-description').text.strip()[:5]
        price = block.find('span', class_='price').text.strip()
        city = block.find('div', class_='list-region').text.strip()
        url = 'https://kolesa.kz' + block.find('a', class_='list-link ddl_product_link').get('href').strip()
        car = {'name': name, 'year': year, 'price': city, 'city': price, 'url': url}
        cars.append(car)
        print('{:30}{:8}{:30}{:15}{}'.format(name, year, city, price, url))
    return cars


def run(path=''):
    loop = asyncio.get_event_loop()
    storage = CSVStorage(path)
    # if script runs without path to CSV file, data will be collected from website
    if not path:
        pages = asyncio.ensure_future(parse_data())
    else:
        pages = storage.collect_cache()
    loop.run_until_complete(pages)

    dataset = []
    for page in pages.result():
        data = read_html(page)
        dataset.append(data)
    storage.save_data(dataset)


if __name__ == "__main__":
    path_to_file = ''
    if len(sys.argv) > 1:
        path_to_file = sys.argv[1]
    run(path_to_file)
