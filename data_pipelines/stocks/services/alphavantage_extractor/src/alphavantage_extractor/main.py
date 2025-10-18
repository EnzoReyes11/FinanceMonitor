#import os
import json
import logging
import sys

#from alphavantage.client import AlphaVantageClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
   #client = AlphaVantageClient('sdas');
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