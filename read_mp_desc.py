import requests
import time
import pandas as pd
import numpy as np
import re

from sqlitedict import SqliteDict
from collections import namedtuple
from bs4 import BeautifulSoup
from functools import partial
from tqdm import tqdm
from unidecode import unidecode

tqdm = partial(tqdm, position=0, leave=True)

RawPage = namedtuple('RawPage', 'address content address_from')
WAITING_TIME = 1

table_name = 'archive_raw_pages_mp'
db_address = 'db/example.sqlite'

db_dict = {}
with SqliteDict(db_address, tablename='biography_raw_pages_mp') as db_bio:
    for ii, jj in db_bio.items():
        db_dict[ii] = jj


def get_mp_name(soup):
    if name:=soup.find("p", {"class": "posel"}):
        return name.get_text().strip()
    else:
        return None


def get_club(soup):
    if name:=soup.find("td", {"class": "Klub"}):
        return name.get_text().strip()
    else:
        return None

def flatten(xss):
    return [x for xs in xss for x in xs]

def get_data_in_list(soup, class_name):
    ul_tag = soup.find_all("ul", {"class": class_name})

    res_dict = {}
    for tag in ul_tag:
        for li in tag.find_all("li"):
            if itm := li.find('p', {"class": "left"}):
                key = itm.get_text().strip()
                val = li.find('p', {"class": "right"}).get_text().strip()
                res_dict[key] = val
    return res_dict

def get_link_under_name(soup, name):
    link = [ii['href'] for ii in soup.find_all('a') if ii.get_text() == name]
    if len(link) > 0:
        return link[0]
    else:
        return None

def get_committees(soup):
    if ul:= soup.find_all('ul', {'class': 'old'}):
        li = [ii.find_all('li') for ii in ul]
        li = flatten(li)
        committees = [ii.get_text() for ii in li]
        return committees
    else:
        return None


total_dict = {}
for key, val in tqdm(db_dict.items()):
    soup = BeautifulSoup(val.content, 'html.parser')

    mp_dict = {}

    mp_dict['name'] = get_mp_name(soup)
    mp_dict['adress'] = val.address
    mp_dict['club'] = get_club(soup)
    mp_dict['comittees'] = get_committees(soup)
    mp_dict['data'] = get_data_in_list(soup, 'dane1')
    mp_dict['data'] = get_data_in_list(soup, 'dane2') | mp_dict['data']
    mp_dict['comittees_link'] = get_link_under_name(soup, 'Przynależność do komisji i podkomisji')
    mp_dict['speeches_link'] = get_link_under_name(soup, 'Wystąpienia na posiedzeniach Sejmu')
    mp_dict['teams_link'] = get_link_under_name(soup, 'Przynależność do zespołów parlamentarnych')
    mp_dict['external_affairs_link'] = get_link_under_name(soup, 'Przynależność do stałych delegacji parlamentarnych i grup bilateralnych')
    mp_dict['questions_link'] = get_link_under_name(soup, 'Interpelacje, zapytania, pytania w sprawach bieżących, oświadczenia')
    total_dict[key] = mp_dict


#download raw pages

req_keys = []
for key, val in total_dict.items():
    if val['comittees_link']:
        req_keys.append(key)

with SqliteDict(db_address, tablename='comittees_raw_pages') as db_comittees:

    remaining_keys = set(req_keys) - set(db_comittees.keys())
    for key in tqdm(list(remaining_keys)):
        val = total_dict[key]
        if link := val['comittees_link']:
            time.sleep(WAITING_TIME)
            rq = requests.get(link)
            db_comittees[key] = RawPage(link, rq.content, None)
            db_comittees.commit()


req_keys = []
for key, val in total_dict.items():
    if val['external_affairs_link']:
        req_keys.append(key)

with SqliteDict(db_address, tablename='external_affairs_raw_pages') as db_ext:

    remaining_keys = set(req_keys) - set(db_ext.keys())
    for key in tqdm(list(remaining_keys)):
        val = total_dict[key]
        if link := val['external_affairs_link']:
            time.sleep(WAITING_TIME)
            rq = requests.get(link)
            db_ext[key] = RawPage(link, rq.content, None)
            db_ext.commit()


req_keys = []
for key, val in total_dict.items():
    if val['teams_link']:
        req_keys.append(key)

with SqliteDict(db_address, tablename='teams_raw_pages') as db_teams:

    remaining_keys = set(req_keys) - set(db_teams.keys())
    for key in tqdm(list(remaining_keys)):
        val = total_dict[key]
        if link := val['teams_link']:
            time.sleep(WAITING_TIME)
            rq = requests.get(link)
            db_teams[key] = RawPage(link, rq.content, None)
            db_teams.commit()


#speeches
req_keys = []
for key, val in total_dict.items():
    if (('RP' in key) and (int(key[0]) <= 6)):
        if val['speeches_link']:
            req_keys.append(key)

with SqliteDict(db_address, tablename='speeches_old_raw_pages') as db_speeches_old:

    remaining_keys = set(req_keys) - set(db_speeches_old.keys())
    for key in tqdm(list(remaining_keys)):
        val = total_dict[key]
        if link := val['speeches_link']:
            time.sleep(WAITING_TIME)
            rq = requests.get(link)
            db_speeches_old[key] = RawPage(link, rq.content, None)
            db_speeches_old.commit()

#speeches new
req_keys = []
for key, val in total_dict.items():
    if (('RP' in key) and (int(key[0]) > 6)):
        if val['speeches_link']:
            req_keys.append(key)

with SqliteDict(db_address, tablename='speeches_new_raw_pages') as db_speeches_new:

    remaining_keys = set(req_keys) - set(db_speeches_new.keys())
    for key in tqdm(list(remaining_keys)):
        val = total_dict[key]
        if link := val['speeches_link']:

            time.sleep(WAITING_TIME)
            orig_rq = requests.get(link)

            #for ii in range(2, 31):
            #    time.sleep(WAITING_TIME)
            #    rq = requests.get(link + suffix.format(page_nb=ii))
            #    if rq.content == orig_rq.content:
            #        print("break!")
            #        break
            #    pages_list.append(RawPage(link + suffix.format(page_nb = ii), rq.content, None))

            db_speeches_new[key] = RawPage(link, orig_rq.content, None)
            db_speeches_new.commit()


req_keys = []
for key, val in total_dict.items():
    if (('RP' in key) and (int(key[0]) > 6)):
        if val['questions_link']:
            req_keys.append(key)

with SqliteDict(db_address, tablename='questions_new_raw_pages') as db_questions_new:

    remaining_keys = set(req_keys) - set(db_questions_new.keys())
    for key in tqdm(list(remaining_keys)):
        val = total_dict[key]
        if link := val['questions_link']:
            time.sleep(WAITING_TIME)
            rq = requests.get(link)
            db_questions_new[key] = RawPage(link, rq.content, None)
            db_questions_new.commit()



# extract vals

teams_dict = {}
with SqliteDict(db_address, tablename='teams_raw_pages') as db_teams:
    for key,value in db_teams.items():
        teams_dict[key] = value

comittees_dict = {}
with SqliteDict(db_address, tablename='comittees_raw_pages') as db_comittees:
    for key,value in db_comittees.items():
        comittees_dict[key] = value

external_dict = {}
with SqliteDict(db_address, tablename='external_affairs_raw_pages') as db_ext:
    for key,value in db_ext.items():
        external_dict[key] = value

old_speeches_dict = {}
with SqliteDict(db_address, tablename='speeches_old_raw_pages') as db_speeches_old:
    for key,value in db_speeches_old.items():
        old_speeches_dict[key] = value

new_speeches_dict = {}
with SqliteDict(db_address, tablename='speeches_new_raw_pages') as db_speeches_new:
    for key,value in db_speeches_new.items():
        new_speeches_dict[key] = value

new_questions_dict = {}
with SqliteDict(db_address, tablename='questions_new_raw_pages') as db_questions_new:
    for key,value in db_questions_new.items():
        new_questions_dict[key] = value

# res

comittees_res = {}
for key, val in tqdm(comittees_dict.items()):
    soup = BeautifulSoup(val.content, 'html.parser')
    comittees_res[key] = [ii.get_text().strip().replace('\n', ' ') for ii in soup.find_all('td') if ('Komisja' in ii.get_text() or 'Podkomisja' in ii.get_text())]

ext_res = {}
for key, val in tqdm(external_dict.items()):
    soup = BeautifulSoup(val.content, 'html.parser')
    ext_res[key] = [ii.get_text().strip() for ii in soup.find_all('a') if ('Grupa' in ii.get_text() or 'Delegacja' in ii.get_text())]

teams_res = {}
for key, val in tqdm(teams_dict.items()):
    soup = BeautifulSoup(val.content, 'html.parser')
    teams_res[key] = [ii.find('a').get_text() for ii in soup.find_all('td') if ii.find_all('a')]

speeches_old_res = {}
for key, val in tqdm(old_speeches_dict.items()):
    soup = BeautifulSoup(val.content, 'html.parser')
    speeches_old_res[key] = [ii.find('a').get_text() for ii in soup.find_all('td') if ii.find_all('a')]

speeches_new_res = {}
for key, val in tqdm(list(new_speeches_dict.items())[1:]):
    soup = BeautifulSoup(val.content, 'html.parser')
    speeches_new_res[key] = [ii.find('a').get_text() for ii in soup.find_all('td') if ii.find_all('a') and ('retransmisja' not in ii.find('a').get_text())]


questions_new_res = {}
for key, val in tqdm(list(new_questions_dict.items())):
    soup = BeautifulSoup(val.content, features="lxml")
    if table := soup.find_all('table'):
        tmp_df = pd.read_html(str(soup.find_all('table')))[0]
        question_dict = dict(zip(
            [ii.split(",")[0].replace(":", "") for ii in tmp_df[0].tolist()],
            [ii for ii in tmp_df[1].tolist()])
        )
        questions_new_res[key] = question_dict


# assemble dict


assembled_dict = {}
tmp_dict = {}
for key, val in tqdm(total_dict.items()):
    tmp_dict = val

    if 'data' in tmp_dict and 'data' in val:
        tmp_dict = tmp_dict | val['data']

    if key in comittees_res:
       tmp_dict['comittees'] = comittees_res[key]

    if key in ext_res:
       tmp_dict['groups'] = ext_res[key]

    if key in teams_res:
       tmp_dict['teams'] = teams_res[key]

    if key in speeches_old_res:
       tmp_dict['speeches'] = speeches_old_res[key]

    if key in speeches_new_res:
       tmp_dict['speeches'] = speeches_new_res[key]

    if key in questions_new_res:
       tmp_dict = tmp_dict | questions_new_res[key]

    assembled_dict[key] = tmp_dict

smaller_dict = {}

for key,value in assembled_dict.items():
    if "3_RP" not in key:
        smaller_dict[key] = value

final_df = pd.DataFrame.from_dict(smaller_dict, orient='index')

final_df1 = pd.DataFrame(index=final_df.index)
final_df1['link'] = final_df['adress']
final_df1['imie i nazwisko'] = final_df['name']
final_df1['prl'] = [1 if 'PRL' in ii else 0 for ii in list(final_df.index)]
final_df1['kadencja'] = [ii.split("_")[0] for ii in list(final_df.index)]
final_df1['data_ur'] = [None if pd.isna(ii) else ii.split(",")[0] for ii in final_df['Data i miejce urodzenia:'].values.tolist()]
miejsce =  [None if pd.isna(ii) else ii.split(",") for ii in final_df['Data i miejce urodzenia:'].values.tolist()]

miejsce_urodzenia = []
for ii in miejsce:
    if ii:
        if len(ii) > 1:
            miejsce_urodzenia.append(ii[1])
        else:
            miejsce_urodzenia.append(None)
    else:
        miejsce_urodzenia.append(None)

final_df1['miejsce_urodzenia'] = miejsce_urodzenia
final_df1['education_sejm'] = final_df['Wykształcenie:']

def verify_keyword(table, column_name, keyword):
    col_list = table[column_name].tolist()
    res_list = []
    for ii in col_list:
        if not pd.isna(ii):
            if keyword in ii:
                res_list.append(1)
            else:
                res_list.append(0)
        else:
            res_list.append(None)
    return res_list



df_terms = final_df1[['imie i nazwisko', 'kadencja', 'prl']].reset_index(drop=True)

df_terms['label'] = ['kad_' + ii+ '_' +jj for ii,jj in zip(['prl' if ii==1 else "rp" for ii in df_terms['prl'].tolist()], df_terms['kadencja'].tolist())]

df_terms['value'] = 1

df_terms1 = (df_terms.pivot_table(index=['imie i nazwisko'],
                    columns=['label'],
                      values=['value'],
                      fill_value=0))



final_df1['higher_educ_sejm'] = verify_keyword(final_df1, 'education_sejm', 'wyższe')
final_df1['second_educ_sejm'] = verify_keyword(final_df1, 'education_sejm', 'średnie')
final_df1['agricult_educ_sejm'] = verify_keyword(final_df1, 'education_sejm', 'rolni')




final_df1['alma_mater'] = final_df['Ukończona szkoła:']
final_df1['occupation_sejm'] = final_df['Zawód/stanowisko:'].combine_first(final_df['Zawód:'])

final_df1['lawyer'] = verify_keyword(final_df1, 'occupation_sejm', 'prawn')
final_df1['economist'] = verify_keyword(final_df1, 'occupation_sejm', 'ekonom')
final_df1['teacher'] = verify_keyword(final_df1, 'occupation_sejm', 'naucz') or verify_keyword(final_df1, 'occupation_sejm', 'wykł')


final_df1['function'] = final_df['Funkcja w Sejmie:']
final_df1['languages'] = final_df['Znajomość języków:']
final_df1['lista'] = final_df['Lista:'].combine_first(final_df['Partia (wybory):'])
final_df1['okreg_nazwa'] = [None if pd.isna(ii) else re.sub('[0-9]', '', ii).strip() for ii in final_df['Okręg wyborczy:'].tolist()]
final_df1['okreg_numer'] = [None if pd.isna(ii) else re.sub('[^0-9]', '', ii).strip() for ii in final_df['Okręg wyborczy:'].tolist()]
final_df1['licz_glosow'] = final_df['Liczba głosów:']
final_df1['data_slubowania'] = final_df['Data ślubowania:'].combine_first(final_df['Ślubowanie:'])

final_df1 = final_df1.reset_index().merge(df_terms1['value'], how="left",
                            left_on=['imie i nazwisko'],
                            right_on=['imie i nazwisko']).set_index('index')

final_df1['interpelacje'] = final_df['Interpelacje']
final_df1['zapytania'] = final_df['Zapytania']
final_df1['pyt_biezace'] = final_df['Pytania w sprawach bieżących']
final_df1['oswiadczenia'] = final_df['Oświadczenia']

n_speeches = []
for ii in final_df['speeches'].tolist():
    if np.any(pd.isna(ii)):
        n_speeches.append(None)
    else:
        n_speeches.append(len(ii))

#niewyglosz = []
#for ii in final_df['speeches'].tolist():
#    if np.any(pd.isna(ii)):
#        n_speeches.append(None)
#    else:
#        tmp_ii = [ix for ix in ii if 'niewygłoszony' in ix]
#        niewyglosz.append(len(tmp_ii))

final_df1['wystapienia'] = n_speeches


def check_keywords(txt, keywords):
    return any(x in txt for x in keywords)

def check_speech_keyword(keywords):
    n_speeches = []
    for ii in final_df['speeches'].tolist():
        if np.any(pd.isna(ii)):
            n_speeches.append(None)
        else:
            ii_tmp = [ix for ix in ii if check_keywords(ix, keywords)]
            n_speeches.append(len(ii_tmp))
    return n_speeches

final_df1['podatk'] = check_speech_keyword(['podatk'])
final_df1['inflacj'] = check_speech_keyword(['inflacj'])
final_df1['bezrobo'] = check_speech_keyword(['bezrobo'])
final_df1['gospodar'] = check_speech_keyword(['gospodar'])
final_df1['budzet'] = check_speech_keyword(['budzet'])
final_df1['finans'] = check_speech_keyword(['finans'])
final_df1['dług'] = check_speech_keyword(['dług'])
final_df1['srodowisk'] = check_speech_keyword(['srodowisk'])
final_df1['bezrobo'] = check_speech_keyword(['bezrobo'])
final_df1['sprawiedliw'] = check_speech_keyword(['sprawiedliw'])
final_df1['sad'] = check_speech_keyword(['sąd'])
final_df1['trybuna'] = check_speech_keyword(['trybuna'])
final_df1['kodeks'] = check_speech_keyword(['kodeks'])
final_df1['decentraliz'] = check_speech_keyword(['decentraliz'])
final_df1['lokalny'] = check_speech_keyword(['lokalny'])
final_df1['gminny'] = check_speech_keyword(['gminny'])
final_df1['samorząd'] = check_speech_keyword(['samorząd'])
final_df1['narodow'] = check_speech_keyword(['narodow'])
final_df1['aborcj'] = check_speech_keyword(['aborcj'])
final_df1['vitro'] = check_speech_keyword(['vitro'])
final_df1['wyzna'] = check_speech_keyword(['wyzna'])
final_df1['kości'] = check_speech_keyword(['kości'])
final_df1['Europ'] = check_speech_keyword(['Europ'])
final_df1['miedzynarod'] = check_speech_keyword(['miedzynarod'])
final_df1['zagranicz'] = check_speech_keyword(['zagranicz'])
final_df1['rolnict'] = check_speech_keyword(['rolnict'])
final_df1['agricult'] = check_speech_keyword(['wiejski', 'wieś'])
final_df1['konstytu'] = check_speech_keyword(['konstytu'])
final_df1['edukacj'] = check_speech_keyword(['edukacj'])
final_df1['zdrowi'] = check_speech_keyword(['zdrowi'])
final_df1['elderly'] = check_speech_keyword(['senior', 'starsz'])
final_df1['transport'] = check_speech_keyword(['transport'])
final_df1['budownictw'] = check_speech_keyword(['budownictw'])
final_df1['childcare'] = check_speech_keyword(['dzieci', 'dziećmi'])
final_df1['młodzie'] = check_speech_keyword(['młodzie'])
final_df1['rodzin'] = check_speech_keyword(['rodzin'])

final_df1['komisje'] = final_df['comittees']

def check_commitee_keyword(keywords):
    n_com = []
    for ii in final_df['comittees'].tolist():

        if np.any(pd.isna(ii)):
            n_com.append(None)
        else:
            ii = list(set(ii))
            ii = [ix for ix in ii if 'Podkomisja' not in ix]
            ii_tmp = [ix for ix in ii if check_keywords(ix, keywords)]
            n_com.append(len(ii_tmp))
    return n_com

final_df1['komisja_ASW'] = check_commitee_keyword(['Administra', 'Wewnętrz'])
final_df1['komisja_CNT'] = check_commitee_keyword(['Cyfryzacji', 'Technolo'])
final_df1['komisja_ESK'] = check_commitee_keyword(['do Energi', 'Aktyw'])
final_df1['komisja_KOP'] = check_commitee_keyword(['Kontrol'])
final_df1['komisja_PET'] = check_commitee_keyword(['Petycji'])
final_df1['komisja_KSS'] = check_commitee_keyword(['Służb Specjalnych'])
final_df1['komisja_SUE'] = check_commitee_keyword(['Europejsk'])
final_df1['komisja_ENM'] = check_commitee_keyword(['Edukacji'])
final_df1['komisja_EPS'] = check_commitee_keyword(['Etyki Pose'])
final_df1['komisja_FPB'] = check_commitee_keyword(['Finans'])
final_df1['komisja_GOR'] = check_commitee_keyword(['Gospodark'])
final_df1['komisja_GMZ'] = check_commitee_keyword(['Morski'])
final_df1['komisja_INF'] = check_commitee_keyword(['Infrastruktur'])
final_df1['komisja_KFS'] = check_commitee_keyword(['Fizyczne', 'Sport'])
final_df1['komisja_KSP'] = check_commitee_keyword(['Kultur'])
final_df1['komisja_LPG'] = check_commitee_keyword(['za Granic'])
final_df1['komisja_MNE'] = check_commitee_keyword(['Mniejszoś'])
final_df1['komisja_OBN'] = check_commitee_keyword(['Obron'])
final_df1['komisja_OSZ'] = check_commitee_keyword(['Środowisk'])
final_df1['komisja_ODK'] = check_commitee_keyword(['Odpowiedzialno'])
final_df1['komisja_PSN'] = check_commitee_keyword(['Senior'])
final_df1['komisja_PSR'] = check_commitee_keyword(['Społeczne'])
final_df1['komisja_RSP'] = check_commitee_keyword(['Regulaminowa'])
final_df1['komisja_RRW'] = check_commitee_keyword(['Rolnictw'])
final_df1['komisja_STR'] = check_commitee_keyword(['Terytorial', 'Regionaln'])
final_df1['komisja_SZA'] = check_commitee_keyword(['Zagraniczny'])
final_df1['komisja_SPC'] = check_commitee_keyword(['Sprawiedliwo'])
final_df1['komisja_UST'] = check_commitee_keyword(['Ustawodawc'])
final_df1['komisja_ZDR'] = check_commitee_keyword(['Zdrowi'])

final_df1['delegacje_nazwy'] = final_df['groups']
final_df1['zespoly_nazwy'] = final_df['teams']


final_df.to_excel("res.xlsx", engine='xlsxwriter')
final_df1.iloc[::-1].to_excel("res1.xlsx", engine='xlsxwriter')