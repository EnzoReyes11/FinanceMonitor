import os
import requests
from bs4 import BeautifulSoup
import pdfplumber
from flask import Flask, jsonify, request
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO)

# --- Configuration ---
BASE_URL = "https://www.iamc.com.ar"
REPORTS_PAGE_URL = f"{BASE_URL}/informeslecap/"

# --- Helper Functions ---

def get_latest_report_url():
    """
    Fetches the main page and returns the URL of the latest report.
    """
    try:
        # The website has SSL certificate issues, so we disable verification.
        # In a production environment, a better approach would be to add the
        # certificate to the trust store.
        response = requests.get(REPORTS_PAGE_URL, verify=False)
        response.raise_for_status()  # Raise an exception for bad status codes
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the first link in the list of reports
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
        # The website has SSL certificate issues, so we disable verification.
        response = requests.get(report_url, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the PDF link
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
        # The website has SSL certificate issues, so we disable verification.
        response = requests.get(pdf_url, verify=False, stream=True)
        response.raise_for_status()

        # Create a temporary file to store the PDF
        pdf_path = "/tmp/report.pdf"
        with open(pdf_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logging.info(f"PDF downloaded successfully to {pdf_path}")
        return pdf_path
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading the PDF: {e}")
        return None

def parse_pdf(pdf_path):
    """
    Parses the PDF and extracts tables by parsing raw text.
    """
    data = {}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            full_text = ""
            for page in pdf.pages:
                full_text += page.extract_text() + "\n"

            # Define table titles and their headers
            table_definitions = {
                "LETRAS DEL TESORO CAPITALIZABLES EN PESOS (LECAP)": [
                    "Especie", "Fecha de Emisión", "Fecha de Pago", "Plazo al Vto (Días)", "Monto al Vto",
                    "Tasa de licitación", "Fecha", "Cotiz c/ VN 100", "Rendimiento del Período", "TNA", "TEA", "TEM", "DM (días)"
                ],
                "BONOS DEL TESORO CAPITALIZABLES EN PESOS (BONCAP)": [
                    "Especie", "Fecha de Emisión", "Fecha de Pago", "Plazo al Vto (Días)", "Monto al Vto",
                    "Tasa de licitación", "Fecha", "Cotiz c/ VN 100", "Rendimiento del Período", "TNA", "TEA", "TEM", "DM (días)"
                ],
                "BONOS DUALES": [
                    "bono", "Fecha de Emisión", "Fecha de Pago", "Monto al Vto", "Fecha",
                    "Cotiz c/ VN 100", "TEM FIJA", "TEM TAMAR", "Spread", "TIR"
                ]
            }

            # Get the text content of the entire PDF
            text_content = ""
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text_content += page.extract_text()

            # Find all table titles in the text
            lines = text_content.split('\n')

            for title, headers in table_definitions.items():
                table_data = []
                in_table = False
                for line in lines:
                    if title in line:
                        in_table = True
                        continue

                    if in_table:
                        # Check if the line is the start of another table
                        is_another_title = False
                        for other_title in table_definitions:
                            if other_title != title and other_title in line:
                                is_another_title = True
                                break
                        if is_another_title:
                            in_table = False
                            break

                        # A simple check for a data row
                        if re.match(r'^[A-Z]{1,4}\d{1,2}[A-Z]\d{1,2}', line.split(' ')[0]):
                            # This is a crude way to split the row, assuming space delimiters
                            values = line.split()
                            # This is a very fragile assumption about the number of columns
                            if len(values) >= len(headers):
                                table_data.append(dict(zip(headers, values)))

                if table_data:
                    data[title] = table_data
        return data

    except Exception as e:
        logging.error(f"Error parsing the PDF: {e}")
        return None
    finally:
        # Clean up the temporary file
        if os.path.exists(pdf_path):
            os.remove(pdf_path)

# --- Flask App ---

app = Flask(__name__)

@app.route('/', methods=['GET'])
def get_report_data():
    """
    Main endpoint to trigger the scraping process and return the data.
    """
    logging.info("Received request to fetch report data.")

    # 1. Get the latest report URL
    report_url = get_latest_report_url()
    if not report_url:
        return jsonify({"error": "Failed to get the latest report URL."}), 500

    # 2. Get the PDF URL from the report page
    pdf_url = get_pdf_url(report_url)
    if not pdf_url:
        return jsonify({"error": "Failed to get the PDF URL."}), 500

    # 3. Download the PDF
    pdf_path = download_pdf(pdf_url)
    if not pdf_path:
        return jsonify({"error": "Failed to download the PDF."}), 500

    # 4. Parse the PDF and extract data
    data = parse_pdf(pdf_path)
    if not data:
        return jsonify({"error": "Failed to parse the PDF."}), 500

    logging.info("Successfully fetched and parsed the report.")
    return jsonify(data)

if __name__ == '__main__':
    # Get the port from the environment variable, default to 8080
    port = int(os.environ.get('PORT', 8080))
    # Run the app, listening on all available interfaces
    app.run(host='0.0.0.0', port=port)
