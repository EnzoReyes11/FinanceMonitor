import decimal
import logging
import os
import re
from datetime import datetime, timezone

import pdfplumber


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
