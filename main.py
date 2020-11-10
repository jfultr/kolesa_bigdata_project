from time import sleep
import asyncio
import sys
from aiohttp import ClientSession
from bs4 import BeautifulSoup


class CSVStorage:
    def __init__(self, path):
        self.path = path

    def collect_cache(self):
        return []


async def get_content(url, session):
    pass


async def parse_data():
    car = {'name': None,
           'year': None,
           'price': None,
           'city': None,
           'url': None}

    headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
    tasks = []
    async with ClientSession(headers=headers) as session:
        for page_num in range(1000):
            url = f'https://kolesa.kz/cars/?page={page_num}'  # полную ссылку нужно бы вставить а другое место
            task = asyncio.ensure_future(get_content(url, session))
            tasks.append(task)

    result_cor = await asyncio.gather(*tasks)
    return result_cor


def run(path=''):
    storage = CSVStorage(path)
    if not path:
        pages = asyncio.ensure_future(parse_data())
    else:
        pages = storage.collect_cache()
    for page in pages:
        pass


if __name__ == "__main__":
    path_to_file = sys.argv[0]
    run(path_to_file)

