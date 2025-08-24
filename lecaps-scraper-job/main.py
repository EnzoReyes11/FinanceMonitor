import logging
import os
import re
from datetime import datetime, timezone

import pdfplumber
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from google.cloud import bigquery

load_dotenv()

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

def parse_pdf(pdf_path):
    """
    Parses the PDF and extracts tables by parsing raw text.
    This function uses pdfplumber, as it proved to be more reliable than camelot for this specific PDF structure.
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
                ]
            }

            lines = full_text.split('\n')

            for title, headers in table_definitions.items():
                table_data = []
                in_table = False
                for line in lines:
                    if title in line:
                        in_table = True
                        continue

                    if in_table:
                        is_another_title = False
                        for other_title in table_definitions:
                            if other_title != title and other_title in line:
                                is_another_title = True
                                break
                        if is_another_title:
                            in_table = False
                            break

                        if re.match(r'^[A-Z]{1,4}\d{1,2}[A-Z]\d{1,2}', line.split(' ')[0]):
                            values = line.split()
                            if len(values) >= len(headers):
                                table_data.append(dict(zip(headers, values)))

                if table_data:
                    data[title] = table_data

            # Special parsing for BONOS DUALES
            bonos_duales_text_start = full_text.find("BONOS DUALES")
            if bonos_duales_text_start != -1:
                bonos_duales_text_end = full_text.find("2 - Índice Caución BYMA", bonos_duales_text_start)
                if bonos_duales_text_end == -1:
                    bonos_duales_text_end = len(full_text)

                bonos_duales_text = full_text[bonos_duales_text_start:bonos_duales_text_end]
                bonos_duales_data = []
                for line in bonos_duales_text.split('\n'):
                    # This regex is specific to the BONOS DUALES table format
                    match = re.match(r'^(?P<bono>[A-Z]{1,4}\d{1,2}[A-Z]\d{1,2})\s+(?P<fecha_emision>\d{1,2}-[A-Za-z]{3}-\d{2,4})\s+(?P<fecha_pago>\d{1,2}-[A-Za-z]{3}-\d{2,4})\s+(?P<plazo_vto>\d+)\s+(?P<monto_vto>[\d,.]+)\s+(?P<fecha>\d{1,2}-[A-Za-z]{3}-\d{2,4})\s+(?P<cotiz>[\d,.]+)\s+(?P<tem_fija>[\d.,]+%)\s+(?P<tem_tamar>[\d.,]+%)\s+(?P<spread>[\d.,]+%)\s+(?P<tir>[\d.,]+%)\s+(?P<dm>\d+)$', line)
                    if match:
                        bonos_duales_data.append(match.groupdict())
                if bonos_duales_data:
                    data["BONOS DUALES"] = bonos_duales_data

        return data

    except Exception as e:
        logging.error(f"Error parsing the PDF: {e}")
        return None
    finally:
        if os.path.exists(pdf_path):
            os.remove(pdf_path)

def store_in_bigquery(data, dry_run=False):
    """
    Stores the parsed data in BigQuery using MERGE and INSERT statements.
    """
    try:
        if dry_run:
            print("--- BigQuery Dry Run ---")
            # In dry run mode, we just print the queries that would be executed.
            # We don't need a BigQuery client.
            # The actual queries will be printed inside the loop.
        else:
            client = bigquery.Client()

        project_id = os.getenv("BQ_PROJECT_ID")
        dataset_id = "financeTools"
        fixed_income_table_id = f"{project_id}.{dataset_id}.byma_treasuries_fixed_income"
        daily_values_table_id = f"{project_id}.{dataset_id}.byma_treasuries_fixed_income_daily_values"

        for table_name, table_data in data.items():
            if "LECAP" in table_name or "BONCAP" in table_name:
                for row in table_data:
                    ticker_symbol = row.get("Especie")
                    type = "BONCAP" if "BONCAP" in table_name else "LECAP"

                    # MERGE into byma_tresuries_fixed_income
                    merge_query = f"""
                    MERGE `{fixed_income_table_id}` T
                    USING (SELECT @ticker_symbol as ticker_symbol, @issue_date as issue_date, @payment_date as payment_date, @amount_at_payment as amount_at_payment, @rate as rate, @type as type) S
                    ON T.asset_key = FARM_FINGERPRINT(CONCAT(S.ticker_symbol, '|', 'byma'))
                    WHEN NOT MATCHED THEN
                      INSERT (asset_key, ticker_symbol, issue_date, payment_date, amount_at_payment, rate, type) 
                      VALUES(FARM_FINGERPRINT(CONCAT(S.ticker_symbol, '|', 'byma')), S.ticker_symbol, PARSE_DATE('%Y-%m-%d', S.issue_date), PARSE_DATE('%Y-%m-%d', S.payment_date), S.amount_at_payment, S.rate, S.type)
                    """
                    query_params = [
                        bigquery.ScalarQueryParameter("ticker_symbol", "STRING", ticker_symbol),
                        bigquery.ScalarQueryParameter("issue_date", "STRING", datetime.strptime(row.get("Fecha de Emisión"), "%d-%b-%y").strftime("%Y-%m-%d")),
                        bigquery.ScalarQueryParameter("payment_date", "STRING", datetime.strptime(row.get("Fecha de Pago"), "%d-%b-%y").strftime("%Y-%m-%d")),
                        bigquery.ScalarQueryParameter("amount_at_payment", "NUMERIC", float(row.get("Monto al Vto").replace(",", "."))),
                        bigquery.ScalarQueryParameter("rate", "NUMERIC", float(row.get("Tasa de licitación").replace("%", "").replace(",", "."))),
                        bigquery.ScalarQueryParameter("type", "STRING", type)
                    ]

                    if dry_run:
                        print(merge_query)
                        print(query_params)
                    else:
                        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
                        client.query(merge_query, job_config=job_config).result()

                    # INSERT into byma_treasuries_fixed_income_daily_values
                    insert_query = f"""
                    INSERT INTO `{daily_values_table_id}` (asset_key, ticker_symbol, snapshot_date, snapshot_timestamp, ingestion_date, maturity_value, action_rate, price_per_100_nominal_value, period_yield, annual_percentage_rate, effective_annual_rate, effective_monthly_rate, modified_duration_in_days)
                    VALUES(FARM_FINGERPRINT(CONCAT(@ticker_symbol, '|', 'byma')), @ticker_symbol, PARSE_DATE('%Y-%m-%d', @snapshot_date), @snapshot_timestamp, @ingestion_date, @maturity_value, @action_rate, @price_per_100_nominal_value, @period_yield, @annual_percentage_rate, @effective_annual_rate, @effective_monthly_rate, @modified_duration_in_days)
                    """
                    query_params = [
                        bigquery.ScalarQueryParameter("ticker_symbol", "STRING", ticker_symbol),
                        bigquery.ScalarQueryParameter("snapshot_date", "STRING", datetime.strptime(row.get("Fecha"), "%d-%b-%y").strftime("%Y-%m-%d")),
                        bigquery.ScalarQueryParameter("snapshot_timestamp", "TIMESTAMP", datetime.now(timezone.utc).isoformat()),
                        bigquery.ScalarQueryParameter("ingestion_date", "TIMESTAMP", datetime.now(timezone.utc).isoformat()),
                        bigquery.ScalarQueryParameter("maturity_value", "NUMERIC", float(row.get("Monto al Vto").replace(",", "."))),
                        bigquery.ScalarQueryParameter("action_rate", "NUMERIC", float(row.get("Tasa de licitación").replace("%", "").replace(",", "."))),
                        bigquery.ScalarQueryParameter("price_per_100_nominal_value", "NUMERIC", float(row.get("Cotiz c/ VN 100").replace(",", "."))),
                        bigquery.ScalarQueryParameter("period_yield", "NUMERIC", float(row.get("Rendimiento del Período").replace("%", "").replace(",", "."))),
                        bigquery.ScalarQueryParameter("annual_percentage_rate", "NUMERIC", float(row.get("TNA").replace("%", "").replace(",", "."))),
                        bigquery.ScalarQueryParameter("effective_annual_rate", "NUMERIC", float(row.get("TEA").replace("%", "").replace(",", "."))),
                        bigquery.ScalarQueryParameter("effective_monthly_rate", "NUMERIC", float(row.get("TEM").replace("%", "").replace(",", "."))),
                        bigquery.ScalarQueryParameter("modified_duration_in_days", "NUMERIC", float(row.get("DM (días)")))
                    ]
                    if dry_run:
                        print(insert_query)
                        print(query_params)
                    else:
                        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
                        client.query(insert_query, job_config=job_config).result()

        if not dry_run:
            logging.info("Data successfully stored in BigQuery.")

    except Exception as e:
        logging.error(f"Error storing data in BigQuery: {e}")

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

    # 5. Store data in BigQuery
    dry_run = request.args.get('dry_run', 'false').lower() == 'true'
    logging.info(f"Calling store_in_bigquery with dry_run={dry_run}")
    store_in_bigquery(data, dry_run=dry_run)

    logging.info("Successfully fetched and parsed the report.")
    return jsonify(data)

@app.route('/test', methods=['GET'])
def get_report_data_html():
    """
    Main endpoint to trigger the scraping process and return the data as HTML tables.
    """
    logging.info("Received request to fetch report data as HTML.")

    # 1. Get the latest report URL
    report_url = get_latest_report_url()
    if not report_url:
        return "<h1>Error: Failed to get the latest report URL.</h1>", 500

    # 2. Get the PDF URL from the report page
    pdf_url = get_pdf_url(report_url)
    if not pdf_url:
        return "<h1>Error: Failed to get the PDF URL.</h1>", 500

    # 3. Download the PDF
    pdf_path = download_pdf(pdf_url)
    if not pdf_path:
        return "<h1>Error: Failed to download the PDF.</h1>", 500

    # 4. Parse the PDF and extract data
    data = parse_pdf(pdf_path)
    if not data:
        return "<h1>Error: Failed to parse the PDF.</h1>", 500

    # 5. Format data as HTML
    html = "<html><head><title>IAMC Report</title></head><body>"
    for title, table_data in data.items():
        html += f"<h1>{title}</h1>"
        if table_data:
            html += "<table border='1'>"
            # Headers
            html += "<tr>"
            for header in table_data[0].keys():
                html += f"<th>{header}</th>"
            html += "</tr>"
            # Rows
            for row in table_data:
                html += "<tr>"
                for value in row.values():
                    html += f"<td>{value}</td>"
                html += "</tr>"
            html += "</table>"
    html += "</body></html>"

    logging.info("Successfully fetched and parsed the report, returning HTML.")
    return html

if __name__ == '__main__':
    # Get the port from the environment variable, default to 8080
    port = int(os.environ.get('PORT', 8080))
    # Run the app, listening on all available interfaces
    app.run(host='0.0.0.0', port=port)
