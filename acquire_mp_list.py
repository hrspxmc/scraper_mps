import requests
import time

from sqlitedict import SqliteDict
from collections import namedtuple
from functools import partial
from tqdm import tqdm
tqdm = partial(tqdm, position=0, leave=True)

RawPage = namedtuple('RawPage', 'address content address_from')
WAITING_TIME = 1

if __name__ == '__main__':

    page_root = 'https://orka.sejm.gov.pl/ArchAll2.nsf/'

    suffixes = ['pierwsza', 'druga', 'trzecia',
                'czwarta', 'piata', 'szosta',
                'siodma', 'osma', 'dziewiata',
                'dziesiata']

    suffixes_rp = [ii+'RP' for ii in suffixes[0:9]]
    table_name = 'archive_raw_pages_mp'
    db_address = 'db/example.sqlite'

    with SqliteDict(db_address, tablename=table_name) as db:

        for ix, sfx in tqdm(enumerate(suffixes, 1)):
            time.sleep(WAITING_TIME)
            page_address = page_root + sfx
            req = requests.get(page_address)
            db_key = str(ix)+'_PRL'
            db[db_key] = RawPage(page_address, req.content, None)

        for ix, sfx in tqdm(enumerate(suffixes_rp, 1)):
            time.sleep(WAITING_TIME)
            page_address = page_root + sfx
            req = requests.get(page_address)
            db_key = str(ix)+'_RP'
            db[db_key] = RawPage(page_address, req.content, None)

        db.commit()
