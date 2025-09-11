#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")  # TensorFlow/TFLite C++ logs
os.environ.setdefault("GLOG_minloglevel", "3")      # absl/glog verbosity
os.environ.setdefault("ABSL_LOG_SEVERITY", "3")     # absl severity floor

import sys
import logging
import signal
import threading
import gc
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    # Running in a PyInstaller bundle
    project_root = Path(sys._MEIPASS)
else:
    # Running in a normal environment
    project_root = Path(__file__).resolve().parent.parent

sys.path.append(str(project_root))

from src.config import tum_iller_plaka_dict, missing_results_dict, known_empty_urls
from src.driver_utils import create_driver, terminate_chrome_processes
from src.ntv_scraper import get_all_urls, scrape_to_df, remove_known_empty_urls, retry_scraping, replace_empty_dataframes, separate_dictionary
from src.ysk_scraper import download_rename_ysk, process_province_dict, split_dict
from src.other_scrapers import belediye_pdf_op, download_and_process_sege_pdfs, get_party_list
from src.data_processing import (
    excel_to_df, dataframe_ysk_update, df_subpart_update, df_to_excel, excel_to_df_ysk, remove_empty_province_dfs, 
    find_shortcoming_2019, councilor_dict_update, results_per_municipality_df, summary_election_results
)

def handle_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.critical("Uncaught Exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_uncaught_exceptions

def signal_handler(sig, frame):
    print("\nTermination signal received. Forcing immediate shutdown...", flush=True)
    terminate_chrome_processes()  # Clean up Chrome processes.
    os._exit(0)  # Immediate termination.

signal.signal(signal.SIGINT, signal_handler)  # Register the new signal handler

# Redirect all stdout and stderr to the logger
class StreamToLogger:
    SUPPRESS_PATTERNS = (
        "DevTools listening on ws://",
        "Created TensorFlow Lite XNNPACK delegate",
        "Attempting to use a delegate that only supports",
        "WARNING: All log messages before absl::InitializeLog() is called are written to STDERR",
        "voice_transcription.cc:58] Registering VoiceTranscriptionCapability",
        "Failed to create GLES3 context, fallback to GLES2",
        "ContextResult::kFatalFailure: Failed to create shared context for virtualization",
    )

    def __init__(self, logger, log_level):
        self.logger = logger
        self.log_level = log_level
        self._last = None  # for simple dedupe

    def write(self, message: str):
        msg = message.rstrip()
        if not msg:
            return
        # Drop known noisy lines outright
        for pat in self.SUPPRESS_PATTERNS:
            if pat in msg:
                return
        # Collapse exact duplicates back-to-back (handles the page logs double-print)
        if msg == self._last:
            return
        self._last = msg
        self.logger.log(self.log_level, msg)

    def flush(self):
        pass

def setup_logging():
    logs_folder = "logs"
    os.makedirs(logs_folder, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_folder, f"log_{timestamp}.txt")
    
    # Create handlers
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.ERROR)  # File gets full logs with (logging.DEBUG)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)  # Console gets only INFO+ logs

    # Create formatter and add it to handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    # Add handlers to root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)  # Set the global log level
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)
    
    # Suppress specific logs
    logging.getLogger("selenium").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("WDM").setLevel(logging.ERROR)          # webdriver-manager
    logging.getLogger("pdfminer").setLevel(logging.ERROR)
    logging.getLogger("camelot").setLevel(logging.ERROR)      
    logging.getLogger("camelot.core").setLevel(logging.ERROR) 
    logging.getLogger("absl").setLevel(logging.ERROR)
    logging.getLogger("tensorflow").setLevel(logging.ERROR)

    # Redirect stdout and stderr
    sys.stdout = StreamToLogger(logging.getLogger(), logging.INFO)
    sys.stderr = StreamToLogger(logging.getLogger(), logging.ERROR)

class MunicipalityData:
    def __init__(self, full_list, bb_list, il_list, full_province_list, dataframes_full_pull,
                 dataframes_2019, dataframes_2024, stats_19, stats_24, sege_ilce, sege_il, party_list):
        self.full_list = full_list
        self.bb_list = bb_list
        self.il_list = il_list
        self.full_province_list = full_province_list
        self.dataframes_full_pull = dataframes_full_pull
        self.dataframes_2019 = dataframes_2019
        self.dataframes_2024 = dataframes_2024
        self.stats_19 = stats_19
        self.stats_24 = stats_24
        self.sege_ilce = sege_ilce
        self.sege_il = sege_il
        self.party_list = party_list

class ElectionSummaryData:
    def __init__(self, b_ilce_sum_2024, b_ilce_sum_2019, b_buy_sum_2024, b_buy_sum_2019,
                 m_ilce_sum_2019, m_ilce_sum_2024, m_il_sum_2019, m_il_sum_2024,
                 belediye_meclis_uye_sayilari, il_meclis_uye_sayilari, dataframes_full_pull):
        self.b_ilce_sum_2024 = b_ilce_sum_2024
        self.b_ilce_sum_2019 = b_ilce_sum_2019
        self.b_buy_sum_2024 = b_buy_sum_2024
        self.b_buy_sum_2019 = b_buy_sum_2019
        self.m_ilce_sum_2019 = m_ilce_sum_2019
        self.m_ilce_sum_2024 = m_ilce_sum_2024
        self.m_il_sum_2019 = m_il_sum_2019
        self.m_il_sum_2024 = m_il_sum_2024
        self.belediye_meclis_uye_sayilari = belediye_meclis_uye_sayilari
        self.il_meclis_uye_sayilari = il_meclis_uye_sayilari
        self.dataframes_full_pull = dataframes_full_pull

def main():
    # Set up logging
    setup_logging()
    logging.info("Starting the application...")
    # Use project_root as the base directory
    script_loc = project_root

    try:
        # Step 1: Perform preliminary data operations
        logging.info("Initiating relevant data operations...")
        results = belediye_pdf_op(script_loc)
        bb_list, il_list, full_list, full_province_list = results['bb_list'], results['il_list'], results['full_list'], results['sehir_listesi']

        # Step 2: Download and process SEGE data
        sege_il, sege_ilce = download_and_process_sege_pdfs(script_loc)
        logging.debug(f"SEGE province example:\n{sege_il.head()}")
        logging.debug(f"SEGE county example:\n{sege_ilce.head()}")

        # Step 3: Fetch party list and URLs
        party_list = get_party_list()
        il_urls, tum_ilceler_urls, merkez_ilce_urls, ilce_urls, belde_urls, tum_urls = get_all_urls()

        # Step 4: Scrape election results
        logging.info("Scraping election results - NTV")
        dataframes_full, error_urls, empty_urls = {}, [], []
        dataframes_full, new_error_urls, new_empty_urls = scrape_to_df(tum_urls, party_list)
        error_urls.extend(new_error_urls)
        empty_urls.extend(new_empty_urls)

        # Step 5: Process missing results
        for key, items in missing_results_dict.items():
            dataframes_full[key] = replace_empty_dataframes(votes=items[0], parties=items[1], candidates=items[2])
        remove_known_empty_urls(empty_urls, known_empty_urls)
        dataframes_full = retry_scraping(dataframes_full, error_urls, empty_urls, party_list)

        # Step 6: Separate data into categories
        dataframes_il, dataframes_ilce, dataframes_belde, dataframes_list = separate_dictionary(dataframes_full)

        # Step 7: Process provinces and create dictionaries
        il_plaka_dict, buyuksehir_plaka_dict = process_province_dict(tum_iller_plaka_dict, il_list)
        tum_iller_plaka_dict_list = split_dict(tum_iller_plaka_dict, 5)
        il_plaka_dict_list = split_dict(il_plaka_dict, 5)
        buyuksehir_plaka_dict_list = split_dict(buyuksehir_plaka_dict, 5)

        # Step 8: Download YSK data
        logging.info("Scraping election results - YSK")
        meclis_uye_sayilari = {}
        folder_name_2019 = '2019_verisi'
        folder_name_2024 = '2024_verisi'
        folder_path_2019 = script_loc / folder_name_2019
        folder_path_2024 = script_loc / folder_name_2024
        execution_dict = {
            (folder_path_2019, folder_path_2024): {
                'belediye_baskanligi': tum_iller_plaka_dict_list,
                'belediye_meclisi': tum_iller_plaka_dict_list,
                'il_meclisi': il_plaka_dict_list,
                'buyuksehir_baskanligi': buyuksehir_plaka_dict_list
            }
        }

        for folder_paths, exec_dict in execution_dict.items():
            for f_path in folder_paths:
                for exec_type, province_list in exec_dict.items():
                    with ThreadPoolExecutor(max_workers=len(province_list)) as executor:
                        for i, p_dict in enumerate(province_list, start=1):
                            executor.submit(download_rename_ysk, p_dict, f_path, i, exec_type, meclis_uye_sayilari)

        terminate_chrome_processes()

        # Step 9: Process 2019 and 2024 YSK data
        logging.info("Executing excel_to_df_ysk...")
        dataframes_2019, dataframes_2019_stats, _, party_translation = excel_to_df_ysk(folder_path_2019)
        dataframes_2024, dataframes_2024_stats, unique_party_set, _ = excel_to_df_ysk(folder_path_2024)
        logging.info("Executing dataframe_ysk_update...")
        diff_dict_2019 = dataframe_ysk_update(dataframes_full, dataframes_2019, party_translation, bb_list, '2019')
        diff_dict_2024 = dataframe_ysk_update(dataframes_full, dataframes_2024, unique_party_set, bb_list, '2024')
        logging.debug("Results of value updating process:")
        for key, value in diff_dict_2019.items():
            logging.debug(f"{key}: {value}")
        for key, value in diff_dict_2024.items():
            logging.debug(f"{key}: {value}")

        # Step 10: Update and finalize data
        logging.info("Executing df_subpart_update...")
        for df_dict in dataframes_list:
            df_subpart_update(dataframes_full, df_dict)
        logging.info("Executing df_to_excel...")
        df_to_excel(dataframes_il, dataframes_ilce, dataframes_belde, script_loc)
        logging.info("Executing excel_to_df...")
        dataframes_full_pull = excel_to_df()
        logging.debug("List of all keys found in DataFrame dictionary:")
        for key in dataframes_full_pull:
            logging.debug(key)
        logging.info("Executing remove_empty_province_dfs...")
        remove_empty_province_dfs(dataframes_full, dataframes_il, il_list)
        logging.info("Executing find_shortcoming_2019...")
        find_shortcoming_2019(dataframes_full_pull)

        # Step 11: Update councilor counts
        logging.info("Executing councilor_dict_update...")
        yeni_meclis_uye_sayilari, belediye_meclis_uye_sayilari, il_meclis_uye_sayilari = councilor_dict_update(meclis_uye_sayilari)

        # Step 12: Create MunicipalityData class object to simplify parameter entry
        municipality_data = MunicipalityData(
            full_list, bb_list, il_list, full_province_list, dataframes_full_pull,
            dataframes_2019, dataframes_2024, dataframes_2019_stats, dataframes_2024_stats, sege_ilce, sege_il, party_list
        )

        # Step 13: Generate results summaries
        logging.info("Exporting full reports...")
        b_sum_2019, b_ilce_sum_2019, b_buy_sum_2019 = results_per_municipality_df(municipality_data, 'baskanlik', '2019', script_loc, True)
        m_sum_2019, m_ilce_sum_2019, m_il_sum_2019 = results_per_municipality_df(municipality_data, 'meclis', '2019', script_loc, True)
        b_sum_2024, b_ilce_sum_2024, b_buy_sum_2024 = results_per_municipality_df(municipality_data, 'baskanlik', '2024', script_loc, True)
        m_sum_2024, m_ilce_sum_2024, m_il_sum_2024 = results_per_municipality_df(municipality_data, 'meclis', '2024', script_loc, True)

        # Step 14: Create ElectionSummaryData class object to simplify parameter entry
        election_data = ElectionSummaryData(
            b_ilce_sum_2024, b_ilce_sum_2019, b_buy_sum_2024, b_buy_sum_2019,
            m_ilce_sum_2019, m_ilce_sum_2024, m_il_sum_2019, m_il_sum_2024,
            belediye_meclis_uye_sayilari, il_meclis_uye_sayilari, dataframes_full_pull,
        )

        # Final step: Export summaries
        logging.info("Exporting summary reports...")
        genel_ozet, genel_ozet_list = summary_election_results(election_data, party_list, 'genel_ozet', script_loc, True, bb_list)
        belediye_baskanligi, belediye_baskanligi_list = summary_election_results(election_data, party_list, 'belediye_baskanligi', script_loc, True, bb_list)
        buyuksehir_baskanligi, buyuksehir_baskanligi_list = summary_election_results(election_data, party_list, 'buyuksehir_baskanligi', script_loc, True, bb_list)
        belediye_meclisleri, belediye_meclisleri_list = summary_election_results(election_data, party_list, 'belediye_meclisleri', script_loc, True, bb_list)
        il_meclisleri, il_meclisleri_list = summary_election_results(election_data, party_list, 'il_meclisleri', script_loc, True, bb_list)

    except Exception as e:
        logging.critical(f"Unhandled exception in main: {e}")
    finally:
        logging.info("Execution completed.")
        terminate_chrome_processes()
        gc.collect()

if __name__ == "__main__":
    main()

