#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import logging
import requests
import fitz
import camelot
import time
import bs4
import pandas as pd
from unidecode import unidecode
from itertools import groupby
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from src.driver_utils import create_driver

def belediye_pdf_op(script_loc):
    il_merkez = []
    filter_dictionary = {"bb_bld": ["ctl00$cph1$CografiBirimControl$imgBtnBuyuksehirSayisi", 
                                    "Buyuksehir_Belediyeleri.pdf", 
                                    0, 
                                    lambda text: [unidecode(text[i + 2]).lower() 
                                                  for i, _ in enumerate(text[:-2]) 
                                                  if 'BÜYÜKŞEHİR ' in text[i] and 'BELEDİYESİ' in text[i + 1]]], 
                         "il_bld": ["ctl00$cph1$CografiBirimControl$imgButtonIlBelediyeSayisi", 
                                    "Il_Belediyeleri.pdf", 
                                    1, 
                                    lambda text: [unidecode(text[i - 1]).lower() 
                                                  for i, item in enumerate(text) 
                                                  if item == 'TÜRKİYE']], 
                         "bel_bld": ["ctl00$cph1$CografiBirimControl$imgButtonBeldeBelediyesiSayisi", 
                                     "Belde_Belediyeleri.pdf", 
                                     2, 
                                     lambda text: [(unidecode(text[i - 1]).lower(), unidecode(text[i - 2]).lower(), unidecode(text[i - 5]).lower()) 
                                                   for i, item in enumerate(text) 
                                                   if item == 'TÜRKİYE'], 
                                     lambda belde_list: {(il, ilce): [
                                                        belde for (il_, ilce_, belde) in belde_list 
                                                        if il_ == il and ilce_ == ilce] 
                                                        for (il, ilce, _) in belde_list}], 
                         "bb_ilce": ["ctl00$cph1$CografiBirimControl$imgButtonBuyukSehitIlceBldSayisi", 
                                     "Buyuksehir_Ilceleri.pdf", 
                                     3, 
                                     lambda text: [(unidecode(text[i - 1]).lower(), unidecode(text[i - 2]).lower()) 
                                                   for i, item in enumerate(text) 
                                                   if item == 'TÜRKİYE'], 
                                     lambda bb_ilce_list: {bb: [ilce for (bb_item, ilce) in bb_ilce_list if bb_item == bb] for bb, _ in bb_ilce_list}], 
                         "il_ilce": ["ctl00$cph1$CografiBirimControl$imgIlceBelediyesiSayisi", 
                                     "Il_Ilceleri.pdf", 
                                     4, 
                                     lambda text: [(unidecode(text[i - 1]).lower(), unidecode(text[i - 2].split(' BELEDİYESİ ')[1]).lower()) 
                                                   for i, item in enumerate(text) 
                                                   if item == 'TÜRKİYE'], 
                                     lambda il_ilce_list: {il: [ilce for (il_item, ilce) in il_ilce_list if il_item == il] for il, _ in il_ilce_list}]
    }
    folder_name = 'PDF_dosyalari'
    folder_path = script_loc / folder_name
    logging.info(folder_path)
    folder_path.mkdir(parents=True, exist_ok=True)
    url = "https://www.e-icisleri.gov.tr/Anasayfa/MulkiIdariBolumleri.aspx"
    
    def obtain_pdf(driver, button_name, new_filename, download_dir):
        try:
            pdf_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.NAME, button_name)))
            pdf_button.click()
            time.sleep(3)
            downloaded_file = os.path.join(download_dir, 'Belediye_Listesi.pdf')
            new_file_path = os.path.join(download_dir, new_filename)
            if os.path.exists(downloaded_file):
                os.rename(downloaded_file, new_file_path)
                logging.info(f"{new_filename} download complete.")
            else:
                logging.warning(f"{new_filename} download failed.")
        except FileExistsError:
            logging.warning(f'File {new_filename} already exists.')
        except Exception as e:
            logging.error(f"Unhandled error: {e}")

    if len(os.listdir(folder_path)) == 0:
        logging.info('Driver initiated')
        driver = create_driver(download_dir=folder_path)
        driver.get(url)
        try:
            for key, value in sorted(filter_dictionary.items(), key=lambda item: item[1][2]):
                obtain_pdf(driver, value[0], value[1], folder_path)
        except Exception as e:
            logging.error(f"An error occurred during PDF download: {e}")
        driver.close()
        driver.quit()
    else:
        logging.info('Files found in folder, skipping download process')
    combined_text = ''
    for key, value in sorted(filter_dictionary.items(), key=lambda item: item[1][2]):
        file_path = os.path.join(folder_path, value[1])
        document = fitz.open(file_path)
        num_pages = document.page_count
        for i in range(num_pages):
            page = document.load_page(i)
            text = page.get_text()
            combined_text += text
    full_text_split = combined_text.split('\n')
    result = [list(g) for k, g in groupby(full_text_split, lambda x: x == '1') if not k]
    processed_data = {}
    for key, value in sorted(filter_dictionary.items(), key=lambda item: item[1][2]):
        processed_data[key] = value[3](result[value[2]])
    belde_list = processed_data['bel_bld']
    bb_list = processed_data['bb_bld']
    bb_ilce = processed_data['bb_ilce']
    il_list = processed_data['il_bld']
    il_ilce_merkezsiz = processed_data['il_ilce']
    for item in il_list:
        il_merkez.append((item, "merkez"))
    il_ilce = il_ilce_merkezsiz + il_merkez
    full_list = belde_list + bb_list + bb_ilce + il_list + il_ilce
    il_ilce_belde_sozluk = filter_dictionary['bel_bld'][4](belde_list)
    buyuksehir_ilce_sozluk = filter_dictionary['bb_ilce'][4](bb_ilce)
    il_ilce_sozluk = filter_dictionary['il_ilce'][4](il_ilce)
    sehir_listesi = bb_list + il_list
    ilce_listesi = {**buyuksehir_ilce_sozluk, **il_ilce_sozluk}
    for index, tupleitem in enumerate(full_list):
        try:
            if tupleitem[2] == "savkoy":
                templist = list(tupleitem)
                logging.debug(f"Previous state: {templist}")
                templist[2] = "sav"
                full_list[index] = tuple(templist)
                logging.debug(f"New state: {full_list[index]}")
        except IndexError:
            pass
    return {'belde_list': belde_list, 
            'bb_list': bb_list, 
            'bb_ilce': bb_ilce, 
            'il_list': il_list, 
            'il_ilce': il_ilce, 
            'sehir_listesi': sehir_listesi, 
            'ilce_listesi': dict(sorted(ilce_listesi.items())), 
            'buyuksehir_ilce_sozluk': buyuksehir_ilce_sozluk, 
            'il_ilce_sozluk': il_ilce_sozluk, 
            'il_ilce_belde_sozluk': il_ilce_belde_sozluk, 
            'full_list': full_list}

def download_and_process_sege_pdfs(script_loc):
    sege_folder_name = 'SEGE Verisi'
    il_page_areas = {1: [('45', ['50,660,230,530'])],
                     2: [('45', ['50,530,230,300'])],
                     3: [('45', ['50,300,230,100'])],
                     4: [('45', ['50,100,230,30']), ('45', ['310,660,450,510'])],
                     5: [('45', ['310,510,450,300'])],
                     6: [('45', ['310,300,450,50'])]}
    ilce_page_areas = {1: [('42', ['50,590,300,50']), ('42', ['320,590,600,40'])], 
                       2: [('42', ['600,650,900,50']), ('42', ['910,650,1250,50']), 
                           ('43', ['50,670,300,50']), ('43', ['320,670,600,40']), 
                           ('43', ['600,670,900,450']), ('43', ['910,670,1250,470'])], 
                       3: [('44', ['50,650,300,50']), ('44', ['320,650,600,50']), 
                           ('44', ['600,670,900,50']), ('44', ['910,670,1250,50']), 
                           ('45', ['50,670,300,430']), ('45', ['320,670,600,450'])], 
                       4: [('45', ['600,650,900,50']), ('45', ['910,650,1250,50']), 
                           ('46', ['50,670,300,50']), ('46', ['320,670,600,40']), 
                           ('46', ['600,670,900,50']), ('46', ['910,670,1250,130'])], 
                       5: [('47', ['50,650,300,50']), ('47', ['320,650,600,50']), 
                           ('47', ['600,670,900,50']), ('47', ['910,670,1250,50']), 
                           ('48', ['50,670,300,50']), ('48', ['320,670,600,90'])], 
                       6: [('48', ['600,650,900,50']), ('48', ['910,650,1250,50']), 
                           ('49', ['50,670,300,270']), ('49', ['320,670,600,290'])]}
    sege_filter_dict = {'il': ('https://www.sanayi.gov.tr/assets/pdf/birimler/2017-il-sege.pdf', 'İl-Sege-2017.pdf', il_page_areas), 
                        'ilce': ('https://www.sanayi.gov.tr/assets/pdf/birimler/2022-ilce-sege.pdf', 'İlce-Sege-2022.pdf', ilce_page_areas)}

    def download_sege_pdf(url, full_path):
        full_path.parent.mkdir(parents=True, exist_ok=True)
        response = requests.get(url)
        with open(full_path, 'wb') as f:
            f.write(response.content)

    def process_sege_pdf(path, page, coordinate, sege):
        column_dict = {3: ['Sıra', 'İl Adı', 'Skor'], 
                       4: ['Sıra', 'İl Adı', 'İlçe Adı', 'Skor']}
        tables = camelot.read_pdf(str(path), pages=page, flavor='stream', table_areas=coordinate)
        if not tables:
            raise ValueError(f"No tables found on page {page} with area {coordinate}")    
        df = tables[0].df
        df.columns = column_dict[len(df.columns)]
        df['İl Adı'] = df['İl Adı'].apply(lambda x: unidecode(x).lower())
        df['Skor'] = df['Skor'].apply(lambda x: x.replace(',', '.')).astype(float)
        df['Sıra'] = df['Sıra'].astype(int)
        if 'İlçe Adı' in df.columns:
            df['İlçe Adı'] = df['İlçe Adı'].apply(lambda x: unidecode(x).lower())
        df['SEGE'] = sege
        df.set_index('Sıra', inplace=True)
        return df

    def process_all_pages(path, page_areas, sege):
        return [process_sege_pdf(path, page, coordinate, sege) for page, coordinate in page_areas]

    for key, (url, file_name, _) in sege_filter_dict.items():
        full_path = script_loc / sege_folder_name / file_name
        download_sege_pdf(url, full_path)
    sege_dfs = {key: pd.concat([df for sege, page_areas in page_areas_dict.items()
                                for df in process_all_pages(script_loc / sege_folder_name / file_name, page_areas, sege)])
                for key, (_, file_name, page_areas_dict) in sege_filter_dict.items()}
    sege_dfs['ilce'].loc[394, ['İlçe Adı']] = '19 mayis'
    sege_dfs['il'].set_index(['İl Adı'], inplace=True)
    sege_dfs['ilce'].set_index(['İl Adı', 'İlçe Adı'], inplace=True)
    return sege_dfs['il'], sege_dfs['ilce']

def get_party_list(url='https://tr.wikipedia.org/wiki/2024_Türkiye_yerel_seçimleri'):
    reqget = requests.get(url)
    soup = bs4.BeautifulSoup(reqget.text, 'lxml')
    element = soup.select('.wikitable.sortable.mw-uncollapsed.unsortable tbody tr td :is(b > a, a > b)')
    party_list = []
    for e in element:
        party_list.append(unidecode(e.getText()).lower())
    party_list.append('bagimsiz toplam oy')
    return party_list

