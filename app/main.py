import logging
from dotenv import load_dotenv
import time
import os
from products import products


if __name__ == "__main__":

    # Starting
    start_time = time.time()

    # 1. Start Timer
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    os.system("cls")
    logging.info("NuEPOS - Rokers Product Updater v1.0\n\n")

    # 1. Load environment variables from .env file
    logging.info(f"1/9: Setting up environment variables")
    load_dotenv(override=True)
    logging.debug(f'EPOS connection:{os.environ["NRU-EPOSCONNECTION"]}')
    logging.debug(f'WEB connection:{os.environ["NRU-WEBCONNECTION"]}')

    # Update products on website
    logging.info("2/9: Updating products on website")
    products()

    # Delete environment variables
    logging.info("8/9: Deleting environment variables")
    del os.environ["NRU-EPOSCONNECTION"]
    del os.environ["NRU-WEBCONNECTION"]

    # Finished.
    logging.info("9/9: Import complete in %.2f seconds." % (time.time() - start_time))
