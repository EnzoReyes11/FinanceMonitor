#import os
import json
import logging
import os
import sys

from alphavantage.client import AlphaVantageClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

ALPHA_VANTAGE_API_TOKEN = os.environ.get("ALPHA_VANTAGE_API_TOKEN", "CHANGE ME IN ENV")

def main():
   client = AlphaVantageClient(ALPHA_VANTAGE_API_TOKEN)
   latest = client.get_latest_daily('GOOGL')
   print(latest)
   print("Completed Task")


if __name__ == "__main__":
   try:
      main()
   except Exception:
      message = (
          "Task test"
      )

      print(json.dumps({"message": message, "severity": "ERROR"}))
      sys.exit(1)