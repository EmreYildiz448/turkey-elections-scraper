#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import shutil
import logging
import bs4
import time
import threading
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from unidecode import unidecode
from src.driver_utils import create_driver

thread_local = threading.local()

file_operation_lock = threading.Lock()

def download_rename_ysk(province_dict, download_dir, driver_id, folder_type, council_dict, main_max_retries=3):
    inner_dict = {}
    year = download_dir.name.split('_')[0]
    
    close_popup_xpath = '//*[@id="myModalClose"]/span'
    dropdown_xpath = '//*[@id="navbarDropdown"]'
    municipal_presidency_2019 = '//div[@id="collapse0"]//div[contains(text(), "Mart 2019")]'
    municipal_presidency_2024 = '//div[@id="collapse0"]//div[contains(text(), "Mart 2024")]'
    council_specific_dropdown_xpath = '//*[@id="heading1"]/h5/a'
    council_specific_xpath_2019 = '//div[@id="collapse1"]//div[contains(text(), "Mart 2019")]'
    council_specific_xpath_2024 = '//div[@id="collapse1"]//div[contains(text(), "Mart 2024")]'
    council_general_dropdown_xpath = '//*[@id="heading2"]/h5/a'
    council_general_xpath_2019 = '//div[@id="collapse2"]//div[contains(text(), "Mart 2019")]'
    council_general_xpath_2024 = '//div[@id="collapse2"]//div[contains(text(), "Mart 2024")]'
    metropolis_dropdown_xpath = '//*[@id="heading3"]/h5/a'
    metropolis_presidency_xpath_2019 = '//*[@id="collapse3"]//div[contains(text(), "Mart 2019")]'
    metropolis_presidency_xpath_2024 = '//*[@id="collapse3"]//div[contains(text(), "Mart 2024")]'
    election_results_xpath_2019 = '//*[@id="accordionSidebar"]/li[14]/a'
    election_results_xpath_2024 = '//*[@id="accordionSidebar"]/li[13]/a'
    download_xpath = '//*[@id="kadinErkekOraniBar"]/div[3]/div/button[1]'   
    metropolis_download_button = '//*[@id="kadinErkekOraniBar"]/div[2]/div/button[1]'
    back_button_xpath = '//*[@id="content"]/div/div[2]/div/div/div[1]/button'
    election_quit_xpath = '//*[@id="content"]/app-topbar/nav/div[1]/div/button'
    election_results_il_meclisi_xpath_2019 = '//*[@id="accordionSidebar"]/li[13]/a'
    election_results_buyuksehir_xpath_2019 = '//*[@id="accordionSidebar"]/li[12]/a'
    election_results_il_meclisi_xpath_2024 = '//*[@id="accordionSidebar"]/li[12]/a'
    election_results_buyuksehir_xpath_2024 = '//*[@id="accordionSidebar"]/li[11]/a'
    language_button = '//*[@id="dropdownMenuButton"]'
    turkish_button = '//*[@id="tr"]'
    filter_dict_buttons = {'2019': {
        'belediye_baskanligi': [close_popup_xpath, language_button, turkish_button, dropdown_xpath, municipal_presidency_2019, election_results_xpath_2019, download_xpath, False], 
        'belediye_meclisi': [close_popup_xpath, language_button, turkish_button, dropdown_xpath, council_specific_dropdown_xpath, council_specific_xpath_2019, election_results_xpath_2019, download_xpath, True], 
        'il_meclisi': [close_popup_xpath, language_button, turkish_button, dropdown_xpath, council_general_dropdown_xpath, council_general_xpath_2019, election_results_il_meclisi_xpath_2019, download_xpath, True], 
        'buyuksehir_baskanligi': [close_popup_xpath, language_button, turkish_button, dropdown_xpath, metropolis_dropdown_xpath, metropolis_presidency_xpath_2019, election_results_buyuksehir_xpath_2019, metropolis_download_button, False]},
                           '2024': {
        'belediye_baskanligi': [close_popup_xpath, language_button, turkish_button, dropdown_xpath, municipal_presidency_2024, election_results_xpath_2024, download_xpath, False], 
        'belediye_meclisi': [close_popup_xpath, language_button, turkish_button, dropdown_xpath, council_specific_dropdown_xpath, council_specific_xpath_2024, election_results_xpath_2024, download_xpath, True], 
        'il_meclisi': [close_popup_xpath, language_button, turkish_button, dropdown_xpath, council_general_dropdown_xpath, council_general_xpath_2024, election_results_il_meclisi_xpath_2024, download_xpath, True], 
        'buyuksehir_baskanligi': [close_popup_xpath, language_button, turkish_button, dropdown_xpath, metropolis_dropdown_xpath, metropolis_presidency_xpath_2024, election_results_buyuksehir_xpath_2024, metropolis_download_button, False]}}

    # Initialize thread-local variables for retry counts
    if not hasattr(thread_local, 'retry_count'):
        thread_local.retry_count = 0
    if not hasattr(thread_local, 'max_retry'):
        thread_local.max_retry = main_max_retries
    
    unique_download_dir = download_dir / folder_type / str(driver_id)
    unique_download_dir.mkdir(parents=True, exist_ok=True)
    
    driver = create_driver(headless=True, download_dir=unique_download_dir)

    buttons_to_click_list = filter_dict_buttons[year][folder_type][:-2]
    council_scraping_confirmation = filter_dict_buttons[year][folder_type][-1]
    download_button_path = filter_dict_buttons[year][folder_type][-2]

    previous_file_content = None
    
    def download_main_table(driver, unique_download_dir, folder_type):
        thread_local.retry_count = 0
        while thread_local.retry_count < thread_local.max_retry:
            try:
                download_button = wait_until_clickable_xpath(driver, download_xpath, click=False)
                driver.execute_script("arguments[0].scrollIntoView();", download_button)
                driver.execute_script("arguments[0].click();", download_button)
            except Exception as e:
                logging.error(f"Error during main table download action: {e}")
                thread_local.retry_count += 1
                continue
            logging.info(f'Driver no {driver_id}: {folder_type}: Ana tablo indiriliyor...')
            time.sleep(1)
            downloaded_file = unique_download_dir / "SecimSonucIl.xls"
            if os.path.exists(downloaded_file):
                if is_file_empty(downloaded_file):
                    logging.warning(f"Driver no {driver_id}: Main table file is empty. Retrying download... (Attempt {thread_local.retry_count + 1}/{thread_local.max_retry})")
                    os.remove(downloaded_file)
                    thread_local.retry_count += 1
                else:
                    new_file_name = unique_download_dir / f"{year}_{folder_type}_genel_sonuclari.xls"
                    os.rename(downloaded_file, new_file_name)
                    return True
            else:
                logging.error("Main table file not found after download attempt.")
                thread_local.retry_count += 1
        return False

    def wait_for_overlay_to_disappear(driver):
        try:
            WebDriverWait(driver, 10).until(EC.invisibility_of_element_located((By.CLASS_NAME, "ngx-overlay")))
        except Exception as e:
            logging.warning(f"Overlay did not disappear: {e}")
            raise
    
    def wait_until_clickable_xpath(driver, xpath, click=True):
        try:
            wait_for_overlay_to_disappear(driver)
            element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, xpath)))
            if click:
                element.click()
            return element
        except Exception as e:
            logging.error(f"Error clicking button {xpath}: {e}")
            raise

    def is_file_empty(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
                return 'rowspan="2"' not in content
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")
            return True

    def read_file_content(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return file.read()
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")
            return None

    def validate_and_rename_file(downloaded_file, new_file_name, current_file_content, unique_download_dir, previous_file_content, driver_id, province):
        if (driver_id == 1 and len(os.listdir(str(unique_download_dir))) == 2) or len(os.listdir(str(unique_download_dir))) == 1:
            previous_file_content = current_file_content
        
        if os.path.exists(downloaded_file):
            if is_file_empty(downloaded_file):
                logging.warning(f"Driver no {driver_id}: File for {province} is empty. Retrying...")
                os.remove(downloaded_file)
                return False, previous_file_content
            elif current_file_content == previous_file_content and len(os.listdir(str(unique_download_dir))) > 2:
                logging.warning(f"Driver no {driver_id}: File for {province} is a duplicate. Retrying...")
                os.remove(downloaded_file)
                return False, previous_file_content
            else:
                os.rename(downloaded_file, new_file_name)
                logging.info(f"Driver no {driver_id}: File for {province} successfully downloaded.")
                return True, current_file_content
        else:
            logging.error(f"Driver no {driver_id}: File for {province} not found.")
            return False, previous_file_content

    def select_province(driver, province_xpath, province, driver_id):
        logging.debug(f'Driver no {driver_id}: Selecting province "{province}"...')
        province_button = wait_until_clickable_xpath(driver, province_xpath, click=False)
        time.sleep(0.1)
        driver.execute_script("arguments[0].dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));", province_button)

    def navigate_back_to_map(driver, back_button_xpath, driver_id):
        logging.debug(f"Driver no {driver_id}: Navigating back to the map...")
        back_button = wait_until_clickable_xpath(driver, back_button_xpath, click=False)
        driver.execute_script("arguments[0].scrollIntoView();", back_button)
        driver.execute_script("arguments[0].click();", back_button)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "map")))

    def download_action(driver, download_button_path, unique_download_dir, province, driver_id, previous_file_content, year, folder_type):
        """
        Perform the download action: click the button, read the file, validate and rename.
        """
        try:
            # Click the download button
            download_button = wait_until_clickable_xpath(driver, download_button_path, click=False)
            driver.execute_script("arguments[0].click();", download_button)
            logging.info(f"Driver no {driver_id}: Initiated download for {province}.")
            time.sleep(1)  # Allow time for the download to start
            
            # Define file paths
            downloaded_file = unique_download_dir / "SecimSonucIlce.xls"
            new_file_name = unique_download_dir / f"{year}_{province}_{folder_type}_sonuclari.xls"
            
            # Read the content of the downloaded file
            current_file_content = read_file_content(downloaded_file)
            
            # Validate and rename the file
            file_valid, updated_file_content = validate_and_rename_file(
                downloaded_file, new_file_name, current_file_content, unique_download_dir, 
                previous_file_content, driver_id, province
            )
            return file_valid, updated_file_content
        except Exception as e:
            logging.error(f"Driver no {driver_id}: Error during download action for {province}: {e}")
            return False, previous_file_content

    def scrape_council_data(driver, council_scraping_confirmation, year, folder_type, council_dict):
        """
        Scrape council data from the page if confirmation is enabled.
    
        Args:
            driver (webdriver): Selenium WebDriver instance.
            council_scraping_confirmation (bool): Whether council scraping is enabled.
            year (str): Election year.
            folder_type (str): Folder type (e.g., 'belediye_meclisi').
            council_dict (dict): Dictionary to store council data.
        """
        if council_scraping_confirmation:
            try:
                # Wait for the required elements to load
                WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".col-2.mb-3")))
                logging.info("Scraping council data...")
                
                # Parse the page source
                page_source = driver.page_source
                soup = bs4.BeautifulSoup(page_source, 'html.parser')
                app_root = soup.find('app-root')
    
                # Extract data from the council elements
                inner_dict = {}
                for item in app_root.find_all(class_='col-2 mb-3'):
                    key = unidecode(item.find(class_='mt-2 mb-0 font-weight-bold d-block').get_text(strip=True)).lower()
                    value = item.find(class_='mt-2 font-weight-light mb-0').get_text(strip=True)
                    inner_dict[key] = value
                
                # Update the council dictionary
                council_dict[f'{year}_{folder_type}'] = inner_dict
                logging.info(f"Council data for {folder_type} in {year} scraped successfully.")
            except Exception as e:
                logging.error(f"Error while scraping council data: {e}")

    def retry_logic(operation, *args, **kwargs):
        """
        Retry logic for a given operation using thread-local retry variables.
        """
        while thread_local.retry_count < thread_local.max_retry:
            try:
                logging.info(f"Driver {driver_id} - Attempt {thread_local.retry_count + 1}/{thread_local.max_retry} for operation: {operation.__name__}")
                result = operation(*args, **kwargs)
                if result:  # Success is determined by the operation
                    return result
            except Exception as e:
                logging.error(f"Driver {driver_id} - Error during {operation.__name__} (Attempt {thread_local.retry_count + 1}): {e}")
            thread_local.retry_count += 1  # Ensure retry count increments
            logging.debug(f"Driver {driver_id} - Retry count incremented to {thread_local.retry_count}.")
            time.sleep(1)  # Pause before the next attempt
        logging.error(f"Operation {operation.__name__} failed after {thread_local.max_retry} attempts.")
        return None

    def cleanup_and_retry(province_dict, download_dir, driver_id, folder_type, council_dict, unique_download_dir):
        """
        Handles cleanup and retries for the `download_rename_ysk` function.
        """
        try:
            logging.info(f"Retry count = {thread_local.retry_count}")
            logging.info(f"Max retries = {thread_local.max_retry}")
    
            if thread_local.retry_count >= thread_local.max_retry:
                logging.warning('Maximum retry attempts reached, process is abandoned.')
                return  # Stop further retries
    
            logging.info("Attempting retry...")
            logging.info(f"Deleting folder: {unique_download_dir}")
            try:
                shutil.rmtree(str(unique_download_dir))  # Remove incomplete download folder
            except Exception as shutil_error:
                logging.error(f"CRITICAL ERROR: Error during folder deletion: {shutil_error}")
    
            # Retry the main function with incremented thread-local retry count
            thread_local.retry_count += 1
            download_rename_ysk(province_dict, download_dir, driver_id, folder_type, council_dict)
    
        except Exception as cleanup_error:
            logging.error(f"CRITICAL ERROR: Error during retry or cleanup: {cleanup_error}")
    
    try:
        # Navigate to the main page and close pop-ups
        logging.info(f"Driver no {driver_id}: Navigating to the main page...")
        driver.get("https://acikveri.ysk.gov.tr/anasayfa")
        logging.info(f"Driver no {driver_id}: Closing the initial pop-up window...")
        for button_xpath in buttons_to_click_list:
            wait_until_clickable_xpath(driver, button_xpath)
    
        # Download main table for driver 1
        if driver_id == 1:
            if not download_main_table(driver, unique_download_dir, folder_type):
                logging.error(f"Driver no {driver_id}: Failed to download a non-empty main table file after multiple attempts.")
                return
            else:
                logging.info(f'Driver no {driver_id}: Main table downloaded. Proceeding to subtables...')
    
        # Scrape council data
        scrape_council_data(driver, council_scraping_confirmation, year, folder_type, council_dict)
    
        # Iterate through provinces
        for reg_num, province in province_dict.items():
            province_xpath = f'//*[@class="city" and @il_id="{reg_num}"]'
            select_province(driver, province_xpath, province, driver_id)
    
            # Download data for the province with retries
            file_check, previous_file_content = retry_logic(
                download_action, driver, download_button_path, 
                unique_download_dir, province, driver_id,
                previous_file_content, year, folder_type
            )
            
            if not file_check:
                logging.error(f"Driver no {driver_id}: Maximum retries reached for {province}. Skipping.")
    
            # Navigate back to the map
            navigate_back_to_map(driver, back_button_xpath, driver_id)
            
    except Exception as e:
        logging.error(f"Driver no {driver_id}: Critical error: {e}")
        cleanup_and_retry(
            province_dict, download_dir, driver_id, folder_type, council_dict,
            unique_download_dir
        )

def process_province_dict(tum_iller_plaka_dict, il_list):
    il_plaka_dict = {}
    buyuksehir_plaka_dict = {}
    for key, item in tum_iller_plaka_dict.items():
        modified_item = unidecode(item).lower()
        if modified_item in il_list:
            il_plaka_dict[key] = modified_item
        else:
            buyuksehir_plaka_dict[key] = modified_item
        tum_iller_plaka_dict[key] = modified_item
    return il_plaka_dict, buyuksehir_plaka_dict

def split_dict(input_dict, split_count):
    items = list(input_dict.items())
    chunk_size = len(items) // split_count
    remainder = len(items) % split_count
    splits = []
    start = 0
    for i in range(split_count):
        end = start + chunk_size + (1 if i < remainder else 0)
        splits.append(dict(items[start:end]))
        start = end
    return splits

