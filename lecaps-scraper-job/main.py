import decimal
import logging
import os
import re
import uuid
from datetime import datetime, timedelta, timezone

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
                    "ticker_symbol", "fecha_emision", "fecha_pago", "plazo_vencimiento_dias", "monto_al_vencimiento",
                    "tasa_de_liquidacion", "fecha_cierre", "fecha_liquidacion", "precio_vn_100", "rendimiento_periodo", "tna", "tea", "tem", "dm_dias"
                ],
                "BONOS DEL TESORO CAPITALIZABLES EN PESOS (BONCAP)": [
                    "ticker_symbol", "fecha_emision", "fecha_pago", "plazo_vencimiento_dias", "monto_al_vencimiento",
                    "tasa_de_liquidacion", "fecha_cierre", "fecha_liquidacion", "precio_vn_100", "rendimiento_periodo", "tna", "tea", "tem", "dm_dias"
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
                            else:
                                logging.warning(f"Skipping row with insufficient values: {line}")

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

def parse_num(s: str) -> decimal.Decimal:
    """
    Parses a string number from IAMC format (e.g., "1.234,56") to a Decimal.
    It removes thousand separators ('.') and uses ',' as the decimal separator.
    """
    # drop thousands ".", use "," as decimal separator
    t = s.replace(".", "").replace(",", ".")
    return decimal.Decimal(t)


def transform_data(parsed_data):
    """
    Transforms raw parsed data into clean, typed rows for BigQuery.
    """
    fixed_income_rows = []
    daily_values_rows = []
    current_timestamp = datetime.now(timezone.utc)

    for table_name, table_data in parsed_data.items():
        if "LECAP" in table_name or "BONCAP" in table_name:
            instrument_type = "BONCAP" if "BONCAP" in table_name else "LECAP"
            for row in table_data:
                logging.debug(row)
                fixed_income_rows.append({
                    "ticker_symbol": row.get("ticker_symbol"),
                    "issue_date": str(datetime.strptime(row.get("fecha_emision"), "%d-%b-%y").date()),
                    "payment_date": str(datetime.strptime(row.get("fecha_pago"), "%d-%b-%y").date()),
                    "amount_at_payment": str(parse_num(row.get("monto_al_vencimiento"))),
                    "rate": str(parse_num(row.get("tasa_de_liquidacion").replace("%", ""))),
                    "type": instrument_type,
                })


                # Transform for daily_values table
                daily_values_rows.append({
                    # asset_key is intentionally omitted
                    "ticker_symbol": row.get("ticker_symbol"),
                    "snapshot_date": str(datetime.strptime(row.get("fecha_cierre"), "%d-%b-%y").date()),
                    "ingestion_timestamp": str(current_timestamp),
                    "maturity_value": str(parse_num(row.get("monto_al_vencimiento"))),
                    "action_rate": str(parse_num(row.get("tasa_de_liquidacion").replace("%", ""))),
                    "price_per_100_nominal_value": str(parse_num(row.get("precio_vn_100").replace(",", "."))),
                    "period_yield": str(parse_num(row.get("rendimiento_periodo").replace("%", ""))),
                    "annual_percentage_rate": str(parse_num(row.get("tna").replace("%", ""))),
                    "effective_annual_rate": str(parse_num(row.get("tea").replace("%", ""))),
                    "effective_monthly_rate": str(parse_num(row.get("tem").replace("%", ""))),
                    "modified_duration_in_days": int(parse_num(row.get("dm_dias"))),
                })

    return fixed_income_rows, daily_values_rows


def _load_and_transform(client, project_id, dataset_id, rows, schema, transform_query):
    """Helper function to load data to a temp table and run a transform query."""
    if not rows:
        return

    temp_table_name = f"temp_source_{uuid.uuid4().hex}"
    temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    try:
        # Load data into the temporary table
        logging.info(f"Loading {len(rows)} rows into temporary table {temp_table_id}")
        load_job = client.load_table_from_json(rows, temp_table_id, job_config=job_config)
        load_job.result()

        # Set an expiration on the temp table for auto-cleanup
        temp_table = client.get_table(temp_table_id)
        temp_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
        client.update_table(temp_table, ["expires"])

        # Execute the main transform query (MERGE or INSERT)
        final_query = transform_query.format(temp_table_id=temp_table_id)
        logging.info("Executing transform query...")
        query_job = client.query(final_query)
        query_job.result()
        logging.info("Transform query completed successfully.")

    except Exception as e:
        logging.error(f"Error during BigQuery load/transform process: {e}")
        logging.info(final_query)
        raise 
    finally:
        logging.info(f"Deleting temporary table {temp_table_id}")
        client.delete_table(temp_table_id, not_found_ok=True)



def load_data_to_bigquery(fixed_income_rows, daily_values_rows, dry_run=False):
    """
    Loads transformed data into BigQuery tables using a temporary table for the MERGE operation.
    """
    if not fixed_income_rows and not daily_values_rows:
        logging.info("No new data to load to BigQuery.")
        return

    project_id = os.getenv("BQ_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        raise RuntimeError("Missing BQ project id. Set BQ_PROJECT_ID or GOOGLE_CLOUD_PROJECT.")

    dataset_id = "financeTools"
    fixed_income_table_id = f"{project_id}.{dataset_id}.byma_treasuries_fixed_income"
    daily_values_table_id = f"{project_id}.{dataset_id}.byma_treasuries_fixed_income_daily_values"

    if dry_run:
        logging.info("--- BigQuery Dry Run ---")
        if fixed_income_rows:
            logging.info(f"Would MERGE {len(fixed_income_rows)} rows into {fixed_income_table_id}")
        if daily_values_rows:
            logging.info(f"Would INSERT {len(daily_values_rows)} rows into {daily_values_table_id}")
        return

    client = bigquery.Client()

    # --- Load Fixed Income Data using a Temporary Table ---
    if fixed_income_rows:
        fixed_income_schema = [
            bigquery.SchemaField("ticker_symbol", "STRING"),
            bigquery.SchemaField("issue_date", "STRING"),
            bigquery.SchemaField("payment_date", "STRING"),
            bigquery.SchemaField("amount_at_payment", "STRING"),
            bigquery.SchemaField("rate", "STRING"),
            bigquery.SchemaField("type", "STRING"),
        ]

        merge_query = f"""
                MERGE `{fixed_income_table_id}` T
                USING `{{temp_table_id}}` S
                ON T.asset_key = FARM_FINGERPRINT(S.ticker_symbol || '|byma')
                WHEN NOT MATCHED THEN
                  INSERT (asset_key, ticker_symbol, issue_date, payment_date, amount_at_payment, rate, type)
                  VALUES(
                    FARM_FINGERPRINT(S.ticker_symbol || '|byma'),
                    S.ticker_symbol,
                    CAST(S.issue_date AS DATE),
                    CAST(S.payment_date AS DATE),
                    CAST(S.amount_at_payment AS NUMERIC),
                    CAST(S.rate AS NUMERIC),
                    S.type
                  );
            """

        _load_and_transform(client, project_id, dataset_id, fixed_income_rows, fixed_income_schema, merge_query)


        # --- Load Daily Values Data ---
        if daily_values_rows:
            daily_values_schema = [
                bigquery.SchemaField("ticker_symbol", "STRING"),
                bigquery.SchemaField("snapshot_date", "STRING"),
                bigquery.SchemaField("ingestion_timestamp", "STRING"),
                bigquery.SchemaField("maturity_value", "STRING"),
                bigquery.SchemaField("action_rate", "STRING"),
                bigquery.SchemaField("price_per_100_nominal_value", "STRING"),
                bigquery.SchemaField("period_yield", "STRING"),
                bigquery.SchemaField("annual_percentage_rate", "STRING"),
                bigquery.SchemaField("effective_annual_rate", "STRING"),
                bigquery.SchemaField("effective_monthly_rate", "STRING"),
                bigquery.SchemaField("modified_duration_in_days", "INTEGER"),
            ]

            insert_query = f"""
                INSERT INTO `{daily_values_table_id}` (
                    asset_key, ticker_symbol, snapshot_date, ingestion_timestamp,
                    maturity_value, action_rate, price_per_100_nominal_value, period_yield,
                    annual_percentage_rate, effective_annual_rate, effective_monthly_rate,
                    modified_duration_in_days)
                SELECT
                    FARM_FINGERPRINT(S.ticker_symbol || '|byma') AS asset_key,
                    S.ticker_symbol,
                    CAST(S.snapshot_date AS DATE) AS snapshot_date,
                    CAST(S.ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp,
                    CAST(S.maturity_value AS NUMERIC) AS maturity_value,
                    CAST(S.action_rate AS NUMERIC) AS action_rate,
                    CAST(S.price_per_100_nominal_value AS NUMERIC) AS price_per_100_nominal_value,
                    CAST(S.period_yield AS NUMERIC) AS period_yield,
                    CAST(S.annual_percentage_rate AS NUMERIC) AS annual_percentage_rate,
                    CAST(S.effective_annual_rate AS NUMERIC) AS effective_annual_rate,
                    CAST(S.effective_monthly_rate AS NUMERIC) AS effective_monthly_rate,
                    S.modified_duration_in_days
                FROM `{{temp_table_id}}` S
            """
            _load_and_transform(client, project_id, dataset_id, daily_values_rows, daily_values_schema, insert_query)


def main(dry_run=False):
    """
    Main function to orchestrate the scraping and loading process.
    """
    logging.info("Starting scraping process...")
    report_url = get_latest_report_url()
    if not report_url:
        logging.error("Failed to get the latest report URL. Aborting.")
        return {"error": "Failed to get the latest report URL."}, 500

    pdf_url = get_pdf_url(report_url)
    if not pdf_url:
        logging.error("Failed to get the PDF URL. Aborting.")
        return {"error": "Failed to get the PDF URL."}, 500

    pdf_path = download_pdf(pdf_url)
    if not pdf_path:
        logging.error("Failed to download the PDF. Aborting.")
        return {"error": "Failed to download the PDF."}, 500

    parsed_data = parse_pdf(pdf_path)
    if not parsed_data:
        logging.error("Failed to parse the PDF. Aborting.")
        return {"error": "Failed to parse the PDF."}, 500

    fixed_income_rows, daily_values_rows = transform_data(parsed_data)

    load_data_to_bigquery(fixed_income_rows, daily_values_rows, dry_run=dry_run)

    logging.info("Scraping process completed successfully.")
    return parsed_data

app = Flask(__name__)

@app.route('/', methods=['GET'])
def get_report_data():
    """
    Main endpoint to trigger the scraping process and return the data.
    """
    logging.info("Received request to fetch report data.")
    try:
        dry_run = request.args.get('dry_run', 'false').lower() == 'true'
        result = main(dry_run=dry_run)

        if isinstance(result, tuple) and "error" in result[0]:
            logging.warning(f"A controlled error occurred: {result[0]['error']}")
            return jsonify(result[0]), result[1]

        logging.info("Successfully fetched, parsed, and loaded the report, returning JSON.")
        return jsonify(result)
    except Exception:
        # This catches any unhandled exceptions in the main logic
        logging.exception("An unexpected error occurred while processing the request.")
        return jsonify({"error": "An unexpected internal server error occurred."}), 500

@app.route('/test', methods=['GET'])
def get_report_data_html():
    """
    Main endpoint to trigger the scraping process and return the data as HTML tables.
    """
    logging.info("Received request to fetch report data as HTML.")
    try:
        data = main(dry_run=True)

        if isinstance(data, tuple) and "error" in data[0]:  # Check if main returned an error tuple
            logging.warning(f"A controlled error occurred: {data[0]['error']}")
            return f"<h1>Error: {data[0]['error']}</h1>", data[1]

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
    except Exception:
        logging.exception("An unexpected error occurred while processing the request for HTML.")
        return "<h1>Error: An unexpected internal server error occurred.</h1>", 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
