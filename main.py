import sys
import logging
from shared_modules.carrefour import extract_and_load as extract_and_load_carr
from shared_modules.lulu import extract_and_load as extract_and_load_lulu
from shared_modules.spinneys import extract_and_load as extract_and_load_spinn

NAME = 'MAIN'

LOG_FORMAT='%(asctime)s: %(name)s-%(funcName)s-%(levelname)s ==> %(message)s'
FORMATTER = logging.Formatter(LOG_FORMAT)

# LOGGING INITIALIZATIONS
logger = logging.getLogger(__name__)   # Will behave as the main logger for all the auxillary loggers
logger.setLevel(logging.DEBUG)

# # Console handler for streaming logs into the output console
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(FORMATTER)
ch.setLevel(logging.INFO)
logger.addHandler(ch)

logger.propagate = False
 
if __name__ == '__main__':


    # logger.info("Scraping spinneys now")
    # extract_and_load_spinn(n_threads=2)

    # logger.info("Scraping lulu now")
    # extract_and_load_lulu(n_threads=3)

    # logger.info("Scraping carrefour now")
    # extract_and_load_carr(n_threads=4)

   
