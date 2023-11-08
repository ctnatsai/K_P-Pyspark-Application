import logging
from logging.handlers import RotatingFileHandler


# Create a logger object
logger = logging.getLogger('KommatiParaDataPrepApp')

# Set the log level
logger.setLevel(logging.INFO)

# Create a file handler object
file_handler = RotatingFileHandler('logs_config/logs/app.log', maxBytes=1000000, backupCount=10)

# Create a formatter object
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Set the formatter for the file handler
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)