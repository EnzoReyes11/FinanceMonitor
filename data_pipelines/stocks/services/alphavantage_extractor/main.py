
def main():
   print(f"Completed Task")


# Start script
if __name__ == "__main__":
   try:
      main()
   except Exception as err:
      message = (
          f"Task test"
      )

      print(json.dumps({"message": message, "severity": "ERROR"}))
      sys.exit(1)  # Retry Job Task by exiting the process