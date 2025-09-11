#!/usr/bin/env python
# coding: utf-8

# In[ ]:

from selenium.webdriver.chrome.service import Service

class QuietChromeService(Service):
    def command_line_args(self):
        # Add ChromeDriver flags to lower its own verbosity
        base = super().command_line_args()
        return base + ["--log-level=SEVERE", "--append-log"]  # (optional) keep appending

import time
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import logging
import os
import psutil
import subprocess

selenium_pids = set()

def create_driver(headless=True, deny_process=False, download_dir=None, chrome_log_path=None, max_retries=3):
    """
    Create and configure a Chrome WebDriver instance with optional logging and behavior settings.

    Args:
        headless (bool): Run the browser in headless mode.
        deny_process (bool): Apply restrictions to reduce resource consumption.
        download_dir (str or Path): Directory for downloads.
        chrome_log_path (str or Path): Path to store Chrome logs.

    Returns:
        WebDriver: Configured WebDriver instance.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            chrome_options = Options()

            # Headless mode
            if headless:
                chrome_options.add_argument("--headless")

            # Basic performance settings
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")

            # Resource reduction settings
            if deny_process:
                chrome_options.add_argument("--autoplay-policy=no-user-gesture-required")
                chrome_options.add_argument("--disable-plugins")
                chrome_options.add_argument("--blink-settings=imagesEnabled=false")
                chrome_options.add_argument("--disable-popup-blocking")
                chrome_options.add_argument("--disable-site-isolation-trials")
                chrome_options.add_argument("--disable-dev-shm-usage")
                chrome_options.add_argument("--disable-software-rasterizer")
                chrome_options.add_argument("--renderer-process-limit=1")

            # Download directory preferences
            if download_dir:
                prefs = {
                    "download.default_directory": str(download_dir),
                    "plugins.always_open_pdf_externally": True,
                    "profile.default_content_setting_values.automatic_downloads": 1
                }
                chrome_options.add_experimental_option("prefs", prefs)

            # Suppress Chrome noise
            chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
            chrome_options.add_argument("--log-level=3")       # chrome: fatal only
            chrome_options.add_argument("--disable-logging")   # chrome: suppress extra logs
            chrome_options.add_argument("--remote-debugging-pipe")

            # Decide if we should log ChromeDriver output
            if os.getenv("SCRAPER_DRIVER_LOGS", "0") == "1":
                # Create a log file only if requested
                if chrome_log_path is None:
                    project_root = Path(__file__).resolve().parent.parent
                    logs_folder = project_root / "logs"
                    logs_folder.mkdir(exist_ok=True)
                    timestamp = time.strftime("%Y%m%d_%H%M%S")
                    chrome_log_path = logs_folder / f"chrome_driver_{timestamp}.log"
                log_output_path = str(chrome_log_path)
            else:
                # Suppress logs completely
                log_output_path = os.devnull

            service = QuietChromeService(
                ChromeDriverManager().install(),
                log_output=log_output_path
            )

            # Hide the extra console window on Windows
            if hasattr(service, "creation_flags"):
                service.creation_flags = subprocess.CREATE_NO_WINDOW

            driver = webdriver.Chrome(service=service, options=chrome_options)

            # Track Selenium PID
            selenium_pids.add(driver.service.process.pid)

            logging.info(f"WebDriver started: {driver}")
            if os.getenv("SCRAPER_DRIVER_LOGS", "0") == "1":
                logging.info(f"ChromeDriver log path: {log_output_path}")

            return driver
        except Exception as e:
            attempt += 1
            logging.error(f"Error initializing WebDriver (attempt {attempt}/{max_retries}): {e}")
            time.sleep(1)

    raise RuntimeError("Failed to initialize WebDriver after multiple attempts.")

def get_child_processes(parent_pid):
    """Get all child processes for a given parent PID."""
    children = []
    for proc in psutil.process_iter(['pid', 'ppid', 'name']):
        try:
            if proc.info['ppid'] == parent_pid:
                children.append(proc.info['pid'])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return children

def terminate_chrome_processes():
    for parent_pid in list(selenium_pids):  # Iterate over tracked parent PIDs.
        try:
            # Find child processes.
            child_pids = get_child_processes(parent_pid)
            
            # Terminate child processes.
            for child_pid in child_pids:
                try:
                    psutil.Process(child_pid).terminate()
                    logging.info(f"Terminated child process with PID {child_pid}")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    logging.warning(f"Failed to terminate child process with PID {child_pid}")
            
            # Terminate parent process.
            psutil.Process(parent_pid).terminate()
            logging.info(f"Terminated parent process with PID {parent_pid}")
        except psutil.NoSuchProcess:
            logging.info(f"Parent process with PID {parent_pid} no longer exists")
        except psutil.AccessDenied:
            logging.warning(f"Access denied to terminate parent process with PID {parent_pid}")
        finally:
            # Always remove the parent PID from tracking, regardless of success or failure.
            selenium_pids.remove(parent_pid)
