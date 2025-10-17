import logging
import os
from alphavantage.client import AlphaVantageClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
   client = AlphaVantageClient('sdas');
   print(f"Completed Task")


if __name__ == "__main__":
   try:
      main()
   except Exception as err:
      message = (
          f"Task test"
      )

      print(json.dumps({"message": message, "severity": "ERROR"}))
      sys.exit(1)