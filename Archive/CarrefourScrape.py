import os
import sys
import time
import shutil
import pathlib
import logging
import traceback
import configparser
import pandas as pd
from random import shuffle
from threading import Thread
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from shared_modules.datetime_utilities import dt_to_string
from concurrent.futures import ThreadPoolExecutor, as_completed
from shared_modules.ops_utilities import add_inj_date, check_update 
from shared_modules.email_utilities import send_mail, send_mail_update
from shared_modules.snowflake_dataloader import connect_to_db, est_connection, orchestrate
from shared_modules.utilities import convert_data_carrefour, extract_data_carrefour, load_more_carrefour, find_cards_carrefour

# Configuration initialization
config = configparser.ConfigParser()
config.read('config.ini')

# Inititializing globals
NAME = 'CARREFOUR'
STATUS = []
STATUSES = []
INJESTION_DELIVERY_CONFIRMATION = ['kdb081293@gmail.com', 'lakshman.bmln@gmail.com']
SNW_USER = config['SNOWFLAKE']['USER']
SNW_ACC = config['SNOWFLAKE']['ACCOUNT']
SNW_PWD = config['SNOWFLAKE']['PASSWORD']
SNW_CHW = config['SNOWFLAKE']['WAREHOUSE']
SNW_USR = config['SNOWFLAKE']['USER_ROLE']
SNW_DB = config['SNOWFLAKE']['DATABASE']
SNW_SCH = config['SNOWFLAKE']['SCHEMA']
API_QY = config['SENDGRID']['API_KEY']
NUM_WORKERS = int(ThreadPoolExecutor()._max_workers/2)

# Other Globals
TEMPFILE_PATH = "TempStage"
TEMP_STAGE_PATH = f'{NAME}/TEMP_STAGE'

# Defining temp folder location paths to stage the injested files
INT_STAGE_PATH = os.path.join(
        TEMPFILE_PATH, 
        TEMP_STAGE_PATH
        )

# Creating relevant paths
pathlib.Path(INT_STAGE_PATH).mkdir(parents=True, exist_ok=True)
INJESTION_DELIVERY_CONFIRMATION  = ['kdb081293@gmail.com', 'prajwalram8@gmail.com']

LOG_FORMAT='%(asctime)s: %(name)s-%(funcName)s-%(levelname)s ==> %(message)s'
FORMATTER = logging.Formatter(LOG_FORMAT)

# LOGGING INITIALIZATIONS
logger = logging.getLogger(__name__)    # Will behave as the main logger for all the auxillary loggers
logger.setLevel(logging.DEBUG)

# Defining a module level file handler
fh = logging.FileHandler(os.path.join(INT_STAGE_PATH,f'{NAME}.log'), 'w+')  #For Logs
fh.setFormatter(FORMATTER)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

# # Console handler for streaming logs into the output console
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(FORMATTER)
ch.setLevel(logging.INFO)
logger.addHandler(ch)

logger.propagate = False

# Links to scrape information from
links ={
        'Dairy & eggs':'https://www.carrefouruae.com/mafuae/en/c/F21600000',
        'Fish & Seafood':'https://www.carrefouruae.com/mafuae/en/c/F1640000',
        'Meat & Poultry':'https://www.carrefouruae.com/mafuae/en/c/F1670000',
        'Food to Go':'https://www.carrefouruae.com/mafuae/en/c/F1650000',
        'Chilled Food Counter':'https://www.carrefouruae.com/mafuae/en/c/F1620000',
        'Fruits':'https://www.carrefouruae.com/mafuae/en/c/F11660100',
        'Vegetables':'https://www.carrefouruae.com/mafuae/en/c/F11660500',
        'Herbs':'https://www.carrefouruae.com/mafuae/en/c/F11660200',
        'Fresh Box':'https://www.carrefouruae.com/mafuae/en/c/F11660200',
        'Hydroponic Farming':'https://www.carrefouruae.com/mafuae/en/c/F11660700',
        'Rice Pasta & Noodles':'https://www.carrefouruae.com/mafuae/en/c/F1701200',
        'Biscuits & Cakes':'https://www.carrefouruae.com/mafuae/en/c/F1710000',
        'Sugar & Home Baking':'https://www.carrefouruae.com/mafuae/en/c/F1701300',
        'Breakfast & Cereals':'https://www.carrefouruae.com/mafuae/en/c/F1720000',
        'Chips Dips & Snacks':'https://www.carrefouruae.com/mafuae/en/c/F1730000',
        'Condiments & Dressing':'https://www.carrefouruae.com/mafuae/en/c/F1750000',
        'Jams':'https://www.carrefouruae.com/mafuae/en/c/F1770000',
        'Nuts':'https://www.carrefouruae.com/mafuae/en/c/F1780000',
        'World Specialities':'https://www.carrefouruae.com/mafuae/en/c/F1790000',
        'Candy':'https://www.carrefouruae.com/mafuae/en/c/F1740100',
        'Chocolates':'https://www.carrefouruae.com/mafuae/en/c/F1740200',
        'Gums & Mints':'https://www.carrefouruae.com/mafuae/en/c/F1740300',
        'Cooking ingredients':'https://www.carrefouruae.com/mafuae/en/c/F1760000',
        'Coffee':'https://www.carrefouruae.com/mafuae/en/c/F1510000',
        'Tea':'https://www.carrefouruae.com/mafuae/en/c/F1560000',
        'Juices':'https://www.carrefouruae.com/mafuae/en/c/F1520000',
        'Kids Drink':'https://www.carrefouruae.com/mafuae/en/c/F1530000',
        'Powered Drinks':'https://www.carrefouruae.com/mafuae/en/c/F1540000',
        'Soft Drinks':'https://www.carrefouruae.com/mafuae/en/c/F1550000',
        'Water':'https://www.carrefouruae.com/mafuae/en/c/F1570000',
        'Baby Healthcare':'https://www.carrefouruae.com/mafuae/en/c/F1010000',
        'Baby Bath':'https://www.carrefouruae.com/mafuae/en/c/F1020000',
        'Baby Feed':'https://www.carrefouruae.com/mafuae/en/c/F1030000',
        'Milk Food & Juice':'https://www.carrefouruae.com/mafuae/en/c/F1040000',
        'Nursery and safety':'https://www.carrefouruae.com/mafuae/en/c/F1050000',
        'Baby Travel':'https://www.carrefouruae.com/mafuae/en/c/F1060000',
        'Tins Jars & Packets':'https://www.carrefouruae.com/mafuae/en/c/F1714000',
        'Cleaning Supplies':'https://www.carrefouruae.com/mafuae/en/c/NF3020000',
        'Disposables & Napkins':'https://www.carrefouruae.com/mafuae/en/c/NF3030000',
        'Food Storage Foil and Cling':'https://www.carrefouruae.com/mafuae/en/c/NF3040000',
        'Candles & Air freshners':'https://www.carrefouruae.com/mafuae/en/c/NF3010000',
        'Laundry Detergents':'https://www.carrefouruae.com/mafuae/en/c/NF3080000',
        'Tissues':'https://www.carrefouruae.com/mafuae/en/c/NF3090000',
        'kitchen & Toilet Rolls':'https://www.carrefouruae.com/mafuae/en/c/NF3100000',
        'Garbage Bags':'https://www.carrefouruae.com/mafuae/en/c/NF3060000',
        'Arabic Bread':' https://www.carrefouruae.com/mafuae/en/c/F1610100',
        'Sweets':'https://www.carrefouruae.com/mafuae/en/c/F1610200',
        'Bread & Rolls':'https://www.carrefouruae.com/mafuae/en/c/F1610300',
        'Crackers & Bread Sticks':'https://www.carrefouruae.com/mafuae/en/c/F1610400',
        'Croissants & Cakes':'https://www.carrefouruae.com/mafuae/en/c/F1610500',
        'Donuts & Muffins':'https://www.carrefouruae.com/mafuae/en/c/F1610600',
        'Frozen Food':'https://www.carrefouruae.com/mafuae/en/c/F6000000',
        'Bio & Organic Food':'https://www.carrefouruae.com/mafuae/en/c/F1200000',
        'Bathroom & Laundry':'https://www.carrefouruae.com/mafuae/en/c/NF8010000',
        'Dental Care':'https://www.carrefouruae.com/mafuae/en/c/NF2010000',
        'Makeup & Nails':'https://www.carrefouruae.com/mafuae/en/c/NF2040000',
        'Suncare & Travel':'https://www.carrefouruae.com/mafuae/en/c/NF2090000',
        'Ladies Hair removal':'https://www.carrefouruae.com/mafuae/en/c/NF2011000',
        'Mens Grooming':'https://www.carrefouruae.com/mafuae/en/c/NF2050000',
        'Makeup':'https://www.carrefouruae.com/mafuae/en/c/NF2110000',
        'Personal Care':'https://www.carrefouruae.com/mafuae/en/c/NF2070000',
        'Natural Personal Care':'https://www.carrefouruae.com/mafuae/en/c/NF2112000',
        'Bath and Body':'https://www.carrefouruae.com/mafuae/en/c/NF2080000',
        'Face & Body Skin care':'https://www.carrefouruae.com/mafuae/en/c/NF2020000',
        'Hair Care':'https://www.carrefouruae.com/mafuae/en/c/NF2030000',
        'Petcare':'https://www.carrefouruae.com/mafuae/en/c/F1100000'
    }

def timeit(f):

    def timed(*args, **kw):

        ts = time.time()
        result = f(*args, **kw)
        te = time.time()

        logging.info('func:%r args:[%r, %r] took: %2.4f sec' % \
          (f.__name__, args, kw, te-ts))
        return result

    return timed

# your main monitoring program goes here

class dispatch():

    def __init__(self, links=links):
        self.og_links = links
    
    def dispatcher(self):
        '''
        When the function is called, a link is returned 
        '''
        links = self.og_links
        s = list(links.keys())
        shuffle(s)
        try:
            random_category = s[0]
            url = links.pop(random_category)
            return random_category, url
        except IndexError:
            return None

def get_page_data(url, card_class="css-b9nx4o"):
    
    # Initializating the web driver
    options = Options()
    options.add_argument("--headless")
    options.binary_location = r"C:\Users\SachinBasavanakattim\AppData\Local\Mozilla Firefox\firefox.exe"
    
    driverService = Service(r"Drivers\geckodriver.exe")
    driver = webdriver.Firefox(service=driverService, options=options)
    
    # Get url from the link_dispatcher
    url_link = url

    # Call the url
    try:
        driver.get(url_link[-1])
    except:
        print(f"{url_link[-1]} Not found")
        return None 

    # Render page and get page source
    page_source = load_more_carrefour(driver=driver)

    # Find cards objects from the page source
    cds = find_cards_carrefour(pg = page_source, card_class = card_class)

    # Extract data from the cards
    list_dict = extract_data_carrefour(cards=cds)
    
    # Convert list of dictionaries into dataframe
    df = convert_data_carrefour(ld = list_dict)

    # Append category
    df['CATEGORY'] = url_link[0]

    logger.info(f"{df.shape} records from link {url} created.")
    
    return df, url_link[0]


def main(do):
    '''
    The main function of I/O
    '''
    value = do.dispatcher()
    while (value is not None):
        df = get_page_data(url=value)
        print(value)
        value = do.dispatcher()
        df[0].to_csv(f"TempStage\{df[-1]}_{ datetime.today().strftime('%Y-%m-%d')}")
    pass

def main_2(do):
    '''
    The alt main function to be executed on a single thread for the concurrent module method
    '''
    value = do.dispatcher()
    out_df = pd.DataFrame()
    while (value is not None):
        print(f"URL being scrapped: {value}")
        df = get_page_data(url=value)
        value = do.dispatcher()
        out_df = pd.concat([out_df,df[0]], ignore_index=True)
    return out_df


def m_threader(num_threads=2, f_obj=main, args = [dispatch()]):
    '''
    Function to carry out threading of the I/O intensive task
    Note: To be used if the extracted dataframe objects are memory intensive
    '''
    threads = []
    for _ in range(num_threads):
        t = Thread(target = f_obj, args = args, daemon=True)
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()

    return None

@timeit
def m_thread_pooler(num_threads=NUM_WORKERS, f_obj=main_2, args = dispatch()):
    out_df = pd.DataFrame()
    with ThreadPoolExecutor() as executor:
        results = [executor.submit(f_obj, args) for _ in range(num_threads)]

        for f in as_completed(results):
            out_df = pd.concat([out_df,f.result()], ignore_index=True)

    return out_df


if __name__ == '__main__':

    # output = m_thread_pooler()
    # output.to_csv(f"TempStage\DataExtract_Carrefour_{datetime.today().strftime('%Y-%m-%d')}.csv", sep="\t")

    output = pd.read_csv("TempStage\DataExtract_Carrefour_2022-09-10.csv", sep='\t')
    output = output[['PRODUCTNAME', 'PRODUCTPRICE_ORIGINAL', 'QUANTITY',
       'ORIGIN', 'PRODUCTPRICE_DISCOUNT', 'CATEGORY']].copy()

    if output.shape[0] > 0:
        output = add_inj_date(output)
        conn = connect_to_db(
                    user=SNW_USER,
                    password=SNW_PWD,
                    account=SNW_ACC,
                    session_parameter={
                        "QUERY_TAG": f"Python data scrape load - {NAME}"
                        }
                    )
        est_connection(
                        conn, 
                        database=SNW_DB,
                        compute_wh=SNW_CHW,
                        schema=SNW_SCH, 
                        user_role=SNW_USR
                        )

        STATUS = list(
            orchestrate(conn=conn,
                df=output, 
                table_name=f'RAW_{NAME}_MAIN', 
                database=SNW_DB,
                csv_filename=f"RAW_{NAME}_MAIN",
                csv_file_path=INT_STAGE_PATH,
                data_stage=f'DATA_STAGE_{NAME}')[0]
                )
        # Update the status to include derived data points
        STATUS = check_update(STATUS,  dt_to_string(datetime.today().strftime('%Y-%m-%d')), dt_to_string(datetime.today().strftime('%Y-%m-%d')))
        # Append to master status list
        STATUSES.extend([STATUS])

        try:
            # Update the ingestion status based on the final status recieved
            ingestion_status = pd.DataFrame(
                            data=STATUSES, 
                            columns=['stage','status', 'rows_parsed', 'rows_loaded', 'error_limit', 'errors_seen', 
                            'first_error', 'first_error_line', 'first_error_character', 'first_error_column_name', 
                            'Load_status', 'sq_start', 'sq_end']
                            )
            
            # Writing ingestion status to csv
            ING_STAT_FILE_NAME = f"ING_STAT_{NAME}.csv"
            ingestion_status.to_csv(os.path.join(INT_STAGE_PATH, ING_STAT_FILE_NAME))
    
            if isinstance(ingestion_status, pd.DataFrame):
                # Send Mail
                try:
                    send_mail(
                        FILE_PATHS=[
                            os.path.join(INT_STAGE_PATH,ING_STAT_FILE_NAME), 
                            os.path.join(INT_STAGE_PATH,f'{NAME}.log')
                            ], 
                        DEL_LIST=INJESTION_DELIVERY_CONFIRMATION, 
                        FILE_NAMES=[ING_STAT_FILE_NAME, NAME], 
                        NAME=NAME,
                        API_QY=API_QY)
                    logger.info(f"Mail to the delivery list {INJESTION_DELIVERY_CONFIRMATION} successfully tiggered")
                except:
                    logger.error(f"Logs load for {NAME} into internal stage unsuccessfully Uncaught Exception: {traceback.format_exc()}")
                # Send Mail
            else:
                try:
                    send_mail_update(
                        DEL_LIST=INJESTION_DELIVERY_CONFIRMATION, 
                        API_QY=API_QY,
                        NAME=NAME)
                    logger.info(f"Mail to the delivery list {INJESTION_DELIVERY_CONFIRMATION} successfully tiggered")
                except:
                    logger.error(f"Logs load for {NAME} into internal stage unsuccessfully Uncaught Exception: {traceback.format_exc()}")
            
        except:
            # Removing the temporary stage directory
            shutil.rmtree(INT_STAGE_PATH, ignore_errors=True)
            logger.info("Local stage directory removed successfully")
                