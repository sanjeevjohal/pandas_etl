import logging
import os
from datetime import datetime

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a file handler with a filename suffixed with timestamp
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f"log_{current_time}.txt"
file_handler = logging.FileHandler(log_filename)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

# Log some messages
logger.debug('This is a debug message')
logger.info('This is an info message')
logger.warning('This is a warning message')
logger.error('This is an error message')
logger.critical('This is a critical message')

# Close the file handler to release the file resource
file_handler.close()

# Optionally, remove the file handler from the logger to avoid further logging to the file
logger.removeHandler(file_handler)
