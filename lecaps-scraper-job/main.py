import logging
import os

import google.cloud.logging
from dotenv import load_dotenv
from etl import extractor, loader, transformer
from flask import Flask, jsonify, request

load_dotenv()

LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}

log_level_name = os.environ.get('LOG_LEVEL', 'INFO').upper()
log_level = LOG_LEVELS.get(log_level_name, logging.INFO)

if os.environ.get('K_SERVICE') is not None:
    client = google.cloud.logging.Client()
    client.setup_logging(log_level=log_level)
else:
    logging.basicConfig(level=log_level)


logging.info(f"Logger initialized with level: {log_level_name}")

def main(dry_run=False):
    """
    Main function to orchestrate the scraping and loading process.
    """
    logging.info("Starting scraping process...")

    report_url = extractor.get_latest_report_url()
    if not report_url:
        logging.error("Failed to get the latest report URL. Aborting.")
        return {"error": "Failed to get the latest report URL."}, 500

    pdf_url = extractor.get_pdf_url(report_url)
    if not pdf_url:
        logging.error("Failed to get the PDF URL. Aborting.")
        return {"error": "Failed to get the PDF URL."}, 500

    pdf_path = extractor.download_pdf(pdf_url)
    if not pdf_path:
        logging.error("Failed to download the PDF. Aborting.")
        return {"error": "Failed to download the PDF."}, 500

    parsed_data = transformer.parse_pdf(pdf_path)
    if not parsed_data:
        logging.error("Failed to parse the PDF. Aborting.")
        return {"error": "Failed to parse the PDF."}, 500

    fixed_income_rows, daily_values_rows = transformer.transform_data(parsed_data)

    loader.load_data_to_bigquery(fixed_income_rows, daily_values_rows, dry_run=dry_run)

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

        if isinstance(data, tuple) and "error" in data[0]: 
            logging.warning(f"A controlled error occurred: {data[0]['error']}")
            return f"<h1>Error: {data[0]['error']}</h1>", data[1]

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

