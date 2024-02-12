import requests
import time

from sqlitedict import SqliteDict
from collections import namedtuple
from bs4 import BeautifulSoup
from functools import partial
from tqdm import tqdm
tqdm = partial(tqdm, position=0, leave=True)

RawPage = namedtuple('RawPage', 'address content address_from')
WAITING_TIME = 1

if __name__ == '__main__':

    page_root = 'https://orka.sejm.gov.pl'
    table_name = 'archive_raw_pages_mp'
    db_address = 'db/example.sqlite'

    with SqliteDict(db_address, tablename=table_name) as db:
        for key, val in tqdm(db.items()):

            bs = BeautifulSoup(val.content, 'html.parser')
            mp_list = bs.find('table').find_all('a', {'target': 'Prawa'})
            mp_link = [(ii.get_text(), ii['href']) for ii in mp_list]

            with SqliteDict(db_address, tablename='biography_raw_pages_mp') as db_bio:
                for name, link in tqdm(mp_link):
                    time.sleep(WAITING_TIME)
                    address = page_root + link
                    rq = requests.get(address)
                    db_bio[key+'_'+name] = RawPage(address, rq.content, val.address)

                db_bio.commit()
