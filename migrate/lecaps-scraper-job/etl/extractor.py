import logging

import requests
from bs4 import BeautifulSoup

# --- Configuration ---
BASE_URL = "https://www.iamc.com.ar"
REPORTS_PAGE_URL = f"{BASE_URL}/informeslecap/"


def get_latest_report_url():
    """
    Fetches the main page and returns the URL of the latest report.
    """
    try:
        # In a production environment, it's recommended to use verify=True and handle SSL certificates properly.
        response = requests.get(REPORTS_PAGE_URL, verify=False) 
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        report_link = soup.find('div', class_='contenidoListado Acceso-Rapido').find('a')
        if report_link and report_link.has_attr('href'):
            return f"{BASE_URL}{report_link['href']}"
        else:
            logging.error("Could not find the latest report link.")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching the main page: {e}")
        return None

def get_pdf_url(report_url):
    """
    Fetches the report page and returns the URL of the PDF.
    """
    try:
        # In a production environment, it's recommended to use verify=True and handle SSL certificates properly.
        response = requests.get(report_url, verify=False) 
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        pdf_link = soup.find('a', class_='pdfDownload')
        if pdf_link and pdf_link.has_attr('href'):
            return pdf_link['href']
        else:
            logging.error(f"Could not find the PDF link on page: {report_url}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching the report page: {e}")
        return None

def download_pdf(pdf_url):
    """
    Downloads the PDF to a temporary file and returns the path.
    """
    try:
        # In a production environment, it's recommended to use verify=True and handle SSL certificates properly.
        response = requests.get(pdf_url, verify=False, stream=True) 
        response.raise_for_status()
        pdf_path = "/tmp/report.pdf"
        with open(pdf_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"PDF downloaded successfully to {pdf_path}")
        return pdf_path
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading the PDF: {e}")
        return None
