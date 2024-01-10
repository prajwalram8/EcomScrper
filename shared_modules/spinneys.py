import os
import time
import shutil
import pathlib
import logging
import traceback
import numpy as np
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
from shared_modules.utilities import find_product_spinney, prcreate_spinney
from shared_modules.utilities import clean_data_spinney

# Configuration initialization
config = configparser.ConfigParser()
config.read('shared_modules\config.ini')

# Inititializing globals
NAME = 'SPINNEYS'
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
LOG_FILE_NAME = os.path.join(LOG_FILE_PATH, f"{datetime.today().strftime('%Y-%m-%d')}.log")
INJESTION_DELIVERY_CONFIRMATION  = ['kdb081293@gmail.com','sonalikannan7@gmail.com']
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
    'Baby and toddler':'https://www.spinneys.com/en-ae/catalogue/category/baby-toddler_303/',
    'Bakery':'https://www.spinneys.com/en-ae/catalogue/category/bakery_333/',
    'Beauty and Cosmetics':'https://www.spinneys.com/en-ae/catalogue/category/beauty-cosmetics_344/',
    'Beverages':'https://www.spinneys.com/en-ae/catalogue/category/beverages_357/',
    'Butchery':'https://www.spinneys.com/en-ae/catalogue/category/butchery_380/',
    'Dairy':'https://www.spinneys.com/en-ae/catalogue/category/dairy-deli-chilled-foods_399/',
    'FoodCupboard':'https://www.spinneys.com/en-ae/catalogue/category/food-cupboard_460/',
    'Flower':'https://www.spinneys.com/en-ae/catalogue/category/flower_1042/',
    'Fruits&Veggies':'https://www.spinneys.com/en-ae/catalogue/category/fruit-vegetables_515/',
    'Frozen':'https://www.spinneys.com/en-ae/catalogue/category/frozen_682/',
    'Home&Leisure':'https://www.spinneys.com/en-ae/catalogue/category/home-leisure_692/',
    'Household':'https://www.spinneys.com/en-ae/catalogue/category/household_717/',
    'Non-Muslim':'https://www.spinneys.com/en-ae/catalogue/category/non-muslim_563/',
    'Petcare':'https://www.spinneys.com/en-ae/catalogue/category/petcare_566/',
    'Seafood':'https://www.spinneys.com/en-ae/catalogue/category/seafood_589/',
    'Toiletries':'https://www.spinneys.com/en-ae/catalogue/category/toiletries-health_594/',
    'World foods':'https://www.spinneys.com/en-ae/catalogue/category/world-foods_780/'
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

def get_page_data(url):
    
    # Initializating the web driver
    options = Options()
    options.add_argument("--headless")
    options.binary_location = r"C:\Users\SachinBasavanakattim\AppData\Local\Mozilla Firefox\firefox.exe"
    
    driverService = Service(r"Drivers\geckodriver.exe")
    driver = webdriver.Firefox(service=driverService, options=options)
    
    # Get url from the link_dispatcher
    url_link = url
    print(url_link)
    print(url_link[-1])

    # Call the url
    try:
        driver.get(url_link[-1])
    except:
        print(f"{url_link[-1]} Not found")
        return None 

    k=2 #Iterating variable for page number

    #Every Product has its Name, Price, Category to which it belongs to 
    final_df = pd.DataFrame(np.nan,index=[0],columns=['PRODUCTNAME','PRODUCTPRICE','CATEGORY'])
    
    while True:
        url=driver.current_url

        #Parse to HTML
        pg_source=driver.page_source 
        #print(type.pg_source)

        #Match the product class from HTML Page Source
        products=find_product_spinney(pg_source)

        #Scrap the product information
        pagepr=prcreate_spinney(products)

        #Navigate to next page in the same category
        selector=f'{url_link[-1]}?page={k}'
        df = pd.DataFrame(pagepr)

        df['CATEGORY'] = url_link[0] 

        final_df = pd.concat([final_df,df], ignore_index=True)

        #Get the next page
        driver.get(selector)
        driver.implicitly_wait(2)

        #Checker for last page in the same category
        url=driver.current_url
        if (url==url_link[-1]):
            break
        driver.implicitly_wait(2)
        k+=1

    # Deleting Cookies and closing driver 
    driver.delete_all_cookies()
    driver.quit()

    logger.info(f"{final_df.shape} records from link {url} created.")

    return final_df, url_link[0]


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
        
    out_df = clean_data_spinney(out_df)
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
            
    out_df = clean_data_spinney(out_df)
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