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
    base_url = 'https://kolesa.kz'
    sm = asyncio.Semaphore(50)
    tasks = []
    async with ClientSession(headers=headers) as session:
        for page_num in range(1, 3):
            url = base_url + f'/cars/?page={page_num}'  # полную ссылку нужно бы вставить а другое место
            task = asyncio.ensure_future(bound_fetch(url, session, sm))
            tasks.append(task)

        result_cor = await asyncio.gather(*tasks)
    return result_cor


def run(path=''):
    loop = asyncio.get_event_loop()
    storage = CSVStorage(path)
    # if script runs without path to CSV file, data will be collected from website
    if not path:
        pages = asyncio.ensure_future(parse_data())
    else:
        pages = storage.collect_cache()

    loop.run_until_complete(pages)
    for page in pages.result():
        pass


if __name__ == "__main__":
    path_to_file = ''
    if len(sys.argv) > 1:
        path_to_file = sys.argv[1]
    run(path_to_file)
