def alpha_vantage_handler(request):
    """HTTP request handler for fetching Alpha Vantage data.

    This function acts as an endpoint for a Flask application or Cloud Function.
    It supports both GET and POST methods to retrieve stock data.

    - GET: Expects a 'symbol' query parameter (e.g., /?symbol=AAPL).
    - POST: Expects a JSON body with a 'symbols' list (e.g., {"symbols": ["AAPL", "MSFT"]}).
      Note: Currently, it only processes and returns data for the *last* symbol in the list.

    Args:
        request (flask.Request): The incoming HTTP request object.

    Returns:
        A Flask Response object containing JSON data. On success, it returns
        the stock data. On failure, it returns a JSON error message with an
        appropriate HTTP status code. Returns None if environment variables
        are not set.
    """
    if not ALPHA_VANTAGE_API_TOKEN or not ALPHA_VANTAGE_API_URL:
        logging.error(
            "FATAL: ALPHA_VANTAGE_API_TOKEN and ALPHA_VANTAGE_URL environment variables are not set."
        )
        return

    if request.method == "POST":
        request_data = request.get_json()
        logging.debug(request_data)

        # Note: This loop will overwrite `symbol_data` on each iteration.
        # The final response will only contain data for the last symbol in the list.
        # TODO: Handle multiple symbols properly.
        for symbol in request_data.get("symbols", []):
            logging.info("Requested symbol: %s", symbol)
            symbol_data = _get_symbol_latest(symbol)

        if symbol_data is None:
            return jsonify({"error": "Internal Server Error"}), 500

        return jsonify(symbol_data)

    if request.method == "GET":
        logging.info("Request : %s", request.args)
        symbol = request.args.get("symbol") or ""

        symbol_data = _get_symbol_latest(symbol)

        if symbol_data is None:
            return jsonify({"error": "Internal Server Error"}), 500

        return jsonify(symbol_data)
