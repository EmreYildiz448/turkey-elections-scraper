# Turkey Elections Scraper

Concurrent Python/Selenium pipeline to scrape, clean, and export Turkey's **2019** and **2024** municipal (local) election results.  
The scraper downloads official PDF results, extracts structured tables, and saves them as CSV/Excel files for downstream analysis.

---

## Features
- **Multi-site scraping** - pulls data from NTV and YSK election result websites.
- **Concurrent processing** - uses `concurrent.futures` to parallelize province/county scrapes.
- **Automatic ChromeDriver management** - no manual driver downloads required.
- **Structured outputs** - creates organized folders of cleaned CSV/Excel data ready for analytics or visualization.
- (Optional) **driver logs** controlled by the `SCRAPER_DRIVER_LOGS` environment variable. (WIP)

---

## Project Structure
```
turkey-elections-scraper
|   README.md
|   requirements.txt
\---src
        config.py
        data_processing.py
        driver_utils.py
        main.py
        ntv_scraper.py
        other_scrapers.py
        ysk_scraper.py
        __init__.py
```
---

## Requirements
* Python **3.9+**  
* Google Chrome (latest stable release)
* Python dependencies listed in requirements.txt

Install Python dependencies:
```bash
pip install -r requirements.txt
```
---
## Installation & Setup

1. ### Clone the repository:
```bash
git clone https://github.com/EmreYildiz448/turkey-elections-scraper.git
cd turkey-elections-scraper
```

2. ### Create and activate a virtual environment:

Run this command first (all platforms):
```bash
python -m venv .venv
```

1. *On Windows*

    ```bash
    .venv\Scripts\activate
    ```

2. *On macOS/Linux*

    ```bash
    source .venv/bin/activate
    ```

3. ### Install dependencies:
```bash
pip install -r requirements.txt
```
---
## Usage

### Run the scraper:
```bash
python -m src.main
```
---
## Output

Upon successful completion, the scraper creates the folder hierarchy shown below.
These directories hold the scraped and processed election datasets in CSV/Excel format.

```
turkey-elections-scraper            # Local election results are organized first by election year.
    +2019_verisi                    # Each year folder contains four election type directories: 
    |   +belediye_baskanligi        Municipal (mayor), municipal council, metropolitan mayor and provincial council.
    |   |   +1                      # Within each election type directory are five numbered subfolders (1-5) 
    |   |   +2                      that hold per-province results ordered by official plate (license) number and alphabetical.
    |   |   +3                      # For all election type directories, subfolder 
    |   |   +4                      1 also contains the aggregated nationwide results for that election type.
    |   |   \5
    |   +belediye_meclisi           
    |   |   +1                      
    |   |   +2
    |   |   +3
    |   |   +4
    |   |   \5
    |   +buyuksehir_baskanligi      
    |   |   +1
    |   |   +2
    |   |   +3
    |   |   +4
    |   |   \5
    |   \il_meclisi                 
    |       +1
    |       +2
    |       +3
    |       +4
    |       \5
    +2024_verisi
    |   +belediye_baskanligi
    |   |   +1
    |   |   +2
    |   |   +3
    |   |   +4
    |   |   \5
    |   +belediye_meclisi
    |   |   +1
    |   |   +2
    |   |   +3
    |   |   +4
    |   |   \5
    |   +buyuksehir_baskanligi
    |   |   +1
    |   |   +2
    |   |   +3
    |   |   +4
    |   |   \5
    |   \il_meclisi
    |       +1
    |       +2
    |       +3
    |       +4
    |       \5
    +excel_files
    |   \general_results
    +logs
    +municipal_summary
    +PDF_dosyalari
    \SEGE Verisi
```
---
## Known Issues

- **Intermittent connection resets (NTV)**

NTV endpoints can occasionally return connection-reset errors in bursts. The scraper automatically retries each URL up to 3 times; failures after the third attempt are skipped and logged.

- **Rare worker thread hang (YSK)**

During YSK scraping, a worker thread can occasionally hang (deadlock).
Press Ctrl+C to invoke the built-in SIGINT handler: the program will immediately terminate (all threads exit) after cleaning up Chrome processes. Because this is a process-level exit, any remaining retries are bypassed. A more robust mitigation (watchdog/timeouts or process-isolated workers) is planned.

---
## Roadmap

- Convert Excel outputs to CSV by default.

- Add configuration options for maximum workers and logging levels.

- Integrate with the visualization GUI once the companion repository is live.

---

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you'd like to change.

---

## Acknowledgements

### Data sources:

[NTV Election Results](https://secim.ntv.com.tr/sitemap.xml)

[YSK (Supreme Election Council of the Republic of Türkiye)](https://acikveri.ysk.gov.tr/anasayfa)

[Ministry of Interior of the Republic of Türkiye](https://www.e-icisleri.gov.tr/Anasayfa/MulkiIdariBolumleri.aspx)

[Ministry of Industry and Technology of the Republic of Türkiye](https://www.sanayi.gov.tr/assets/pdf/birimler/2017-il-sege.pdf) (https://www.sanayi.gov.tr/assets/pdf/birimler/2022-ilce-sege.pdf)

[Wikipedia](https://tr.wikipedia.org/wiki/2024_Türkiye_yerel_seçimleri)

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---
