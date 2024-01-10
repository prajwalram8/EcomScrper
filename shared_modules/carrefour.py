import os
import time
import shutil
import pathlib
import logging
import traceback
import configparser
import pandas as pd
from random import shuffle
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from shared_modules.datetime_utilities import dt_to_string
from shared_modules.ops_utilities import add_inj_date, check_update 
from shared_modules.email_utilities import send_mail, send_mail_update
from concurrent.futures import ThreadPoolExecutor, as_completed
from shared_modules.snowflake_dataloader import connect_to_db, est_connection, orchestrate
from shared_modules.utilities import clean_data_carrefour, clean_data_lulu, convert_data_carrefour, extract_data_carrefour, load_more_carrefour, find_cards_carrefour


# Configuration initialization
config = configparser.ConfigParser()
config.read('shared_modules\config.ini')

# Inititializing globals
NAME = 'CARREFOUR'
SNW_USER = config['SNOWFLAKE']['USER']
SNW_ACC = config['SNOWFLAKE']['ACCOUNT']
SNW_PWD = config['SNOWFLAKE']['PASSWORD']
SNW_CHW = config['SNOWFLAKE']['WAREHOUSE']
SNW_USR = config['SNOWFLAKE']['USER_ROLE']
SNW_DB = config['SNOWFLAKE']['DATABASE']
SNW_SCH = config['SNOWFLAKE']['SCHEMA']
API_QY = config['SENDGRID']['API_KEY']

# Other Globals
STATUS = []
STATUSES = []
TEMPFILE_PATH = "TempStage"
LOGS_PATH = "AppLogs"
TEMP_STAGE_PATH = f'{NAME}_TEMP_STAGE'
NUM_WORKERS = int(ThreadPoolExecutor()._max_workers/2)
INT_STAGE_PATH = os.path.join(TEMPFILE_PATH, TEMP_STAGE_PATH)
ING_STAT_FILE_NAME = f"ING_STAT_{NAME}.csv"
LOG_FILE_PATH = os.path.join(LOGS_PATH, NAME)
LOG_FILE_NAME = os.path.join(LOG_FILE_PATH, f"{datetime.today().strftime('%Y_%m_%d')}.log")
INJESTION_DELIVERY_CONFIRMATION  = ['sonalikannan7@gmail.com','kdb081293@gmail.com']
LOG_FORMAT='%(asctime)s: %(name)s-%(funcName)s-%(levelname)s ==> %(message)s'
FORMATTER = logging.Formatter(LOG_FORMAT)

# Creating relevant paths
pathlib.Path(INT_STAGE_PATH).mkdir(parents=True, exist_ok=True)
pathlib.Path(LOG_FILE_PATH).mkdir(parents=True, exist_ok=True)

# Logging Initializations
logger = logging.getLogger('__main__.' + __name__)
logger.setLevel(logging.DEBUG)

# Defining a module level file handler
fh = logging.FileHandler(LOG_FILE_NAME, 'w+')  #For Logs
fh.setFormatter(FORMATTER)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)



# Links to scrape information from
links = {
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
            logger.info(f"Dispateched {random_category}, {len(links)} categories remaining ")
            return random_category, url
        except IndexError:
            return None

def get_page_data(url, card_class="css-b9nx4o"):
    
    # Initializating the web driver
    options = Options()
    # options.add_argument("--headless")
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

    # Deleting cookies and closing the driver
    driver.delete_all_cookies()
    driver.quit()

    logger.info(f"{df.shape} records from link {url} created.")
    
    return df, url_link[0]


def main(do):
    '''
    The alt main function to be executed on a single thread for the concurrent module method
    '''
    value = do.dispatcher()
    out_df = pd.DataFrame()
    while (value is not None):
        df = get_page_data(url=value)
        value = do.dispatcher()
        out_df = pd.concat([out_df,df[0]], ignore_index=True)
        # print(out_df)
    return out_df


@timeit
def m_thread_pooler(num_threads=NUM_WORKERS, f_obj=main, args = dispatch()):
    '''
    Function to manage the allocation of functions to different threads and consolidate the individual results into single output
    '''
    out_df = pd.DataFrame()
    with ThreadPoolExecutor() as executor:
        results = [executor.submit(f_obj, args) for _ in range(num_threads)]
        for f in as_completed(results):
            out_df = pd.concat([out_df,f.result()], ignore_index=True)
    
    out_df = clean_data_carrefour(out_df)
    return out_df


def extract_and_load(n_threads=NUM_WORKERS):
    '''
    Funtion to call the extraction function and load the data extracted into the data warehouse
    '''
    output = m_thread_pooler(num_threads=n_threads)

    try:
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
                    table_name=f'FINAL_{NAME}_MAIN', 
                    database=SNW_DB,
                    csv_filename=f"FINAL_{NAME}_MAIN",
                    csv_file_path=INT_STAGE_PATH,
                    data_stage=f'DATA_STAGE_{NAME}')[0]
                    )
            # Update the status to include derived data points
            STATUS = check_update(STATUS,  dt_to_string(datetime.today().strftime('%Y-%m-%d')), dt_to_string(datetime.today().strftime('%Y-%m-%d')))
            # Append to master status list
            STATUSES.extend([STATUS])

            logger.info(f"{NAME} with {output.shape} loaded to warehouse sucessfully")

            try:
                # Update the ingestion status based on the final status recieved
                ingestion_status = pd.DataFrame(
                                data=STATUSES, 
                                columns=['stage','status', 'rows_parsed', 'rows_loaded', 'error_limit', 'errors_seen', 
                                'first_error', 'first_error_line', 'first_error_character', 'first_error_column_name', 
                                'Load_status', 'sq_start', 'sq_end']
                                )
                
                # Writing ingestion status to csv
                ingestion_status.to_csv(os.path.join(INT_STAGE_PATH, ING_STAT_FILE_NAME))
        
                if isinstance(ingestion_status, pd.DataFrame):
                    # Send Mail
                    try:
                        send_mail(
                            FILE_PATHS=[
                                os.path.join(INT_STAGE_PATH,ING_STAT_FILE_NAME), 
                                LOG_FILE_NAME
                                ], 
                            DEL_LIST=INJESTION_DELIVERY_CONFIRMATION, 
                            FILE_NAMES=[ING_STAT_FILE_NAME, NAME], 
                            NAME=NAME,
                            API_QY=API_QY)
                        logger.info(f"Mail to the delivery list {INJESTION_DELIVERY_CONFIRMATION} successfully tiggered")
                    except:
                        logger.error(f"Logs load for {NAME} into internal stage unsuccessfully Uncaught Exception: {traceback.format_exc()}")
                    # Send Mail
            except:
                logger.warning("Injestion file needs to be checked")
                try:
                    send_mail_update(
                        FILE_PATHS=[
                            LOG_FILE_NAME
                        ],
                        FILE_NAMES=[NAME],
                        DEL_LIST=INJESTION_DELIVERY_CONFIRMATION, 
                        API_QY=API_QY,
                        NAME=NAME)
                    logger.info(f"Mail to the delivery list {INJESTION_DELIVERY_CONFIRMATION} successfully tiggered")
                except:
                    logger.error(f"Logs load for {NAME} into internal stage unsuccessfully Uncaught Exception: {traceback.format_exc()}")
            
    except:
        # Send mail informing that the scraping and loading activity was not successful
        logging.info("Scrapping and loading activity unsuccessful")
        try:
            send_mail_update(
                FILE_PATHS=[
                    LOG_FILE_NAME
                ],
                FILE_NAMES=[NAME],
                DEL_LIST=INJESTION_DELIVERY_CONFIRMATION, 
                API_QY=API_QY,
                NAME=NAME)
            logger.info(f"Mail to the delivery list {INJESTION_DELIVERY_CONFIRMATION} successfully tiggered")
        except:
            logger.error(f"Logs load for {NAME} into internal stage unsuccessfully Uncaught Exception: {traceback.format_exc()}")

    # Removing the temporary stage directory (INT_STAGE_PATH)
    shutil.rmtree(INT_STAGE_PATH, ignore_errors=True)
    logger.info("Local stage directory removed successfully")

