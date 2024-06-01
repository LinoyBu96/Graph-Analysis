import logging
import os

LOG_FILE_TEMPLATE = 'log_{current_time}.log'
LOGS_DIR = 'logs'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

def setup_logging(current_time):
    # Create a unique log file for each run
    log_filename = LOG_FILE_TEMPLATE.format(current_time=current_time)
    log_full_path = os.path.join(LOGS_DIR, log_filename)

    logging.basicConfig(
        filename=log_full_path,  # Log file path
        format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',  # Log message format
        datefmt=LOG_DATE_FORMAT,  # Date format
        level=logging.INFO  # Logging level
    )

    # Console handler to output logs to the console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)