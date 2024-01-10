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
from shared_modules.utilities import convert_data_lulu, extract_data_lulu_new, find_cards_lulu, load_more_lulu,clean_data_lulu

# Configuration initialization
config = configparser.ConfigParser()
config.read('shared_modules\config.ini')

# Inititializing globals
NAME = 'LULU'
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
    'Breakfast&spreads':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-breakfast-spreads/c/HY00214912',
    'Beverages':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-beverage/c/HY94000100',
    'WorldFoods':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-world-foods/c/HY00214976',
    'FrozenFood':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-frozen/c/HY00215064',
    'CannedFood':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-canned-foods/c/HY00214920',
    'HomeBaking&Sweeteners':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-home-baking-sweeteners/c/HY00214950',
    'Table&Sauces':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-dressings-table-sauces/c/HY00214940',
    'Pasta&rice':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-rice-pasta-noodles/c/HY00214956',
    'Biscuits&Confectionary':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-biscuits-confectionery/c/HY00214970',
    'Chips&Snacks':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-chips-snacks/c/HY00214962',
    'Cooking Ingredients':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-cooking-ingredients/c/HY00214932',
    'SpecialityFood':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-cupboard-speciality/c/HY00215240',
    'Dairy':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-dairy-eggs-cheese/c/HY00216087',
    'Bakery':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-bakery/c/HY00216088',
    'Fresh Juice & Salad':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-juice-salads/c/HY00216142',
    'Fruits&Vegetables':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-fruits-vegetables/c/HY00216090',
    'FreshMeat&Seafood':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-meat-seafood/c/HY00216146',
    'FreshChicken':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-chicken-poultry/c/HY00216089',
    'Delicatessen':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-delicatessen/c/HY00216151',
    'Ready Meals':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-ready-meals/c/HY00216155',
    'Flowers':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-plants-flowers/c/HY00600500',
    'Baby Products':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-baby-products/c/HY00217459',
    'Cleaning':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-household-cleaning/c/HY00215078',
    'Air Freshner':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-household-air-fresheners/c/HY00215124',
    'Food Storage':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-household-storage/c/HY00215142',
    'Paper Goods':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-household-paper-goods/c/HY00215156',
    'Electrical Goods':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-household-electrical-accessories/c/HY00217083',
    'Laundry':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-household-laundry/c/HY00215092',
    'Home Essentials':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-household-home-essentials/c/HY00215110',
    'Pets':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-household-pets/c/HY00215252',
    'Facial & Skin Care':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-health-beauty-face-body-skincare/c/HY00215016',
    'Dental Health Care':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-health-beauty-dental-care/c/HY00215002',
    'Mens caring':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-health-beauty-men-s-grooming/c/HY00215026',
    'Hair care':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-grocery-health-beauty-hair-care/c/HY00215008',
    'Make up':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-health-beauty-make-up/c/HY00217431',
    'Premium Perfumes':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-health-beauty-premium-perfumes/c/HY00217012',
    'Feminine care':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-health-beauty-feminine-care/c/HY00215046',
    'Bath':'https://www.luluhypermarket.com/en-ae/grocery-fresh-food-health-beauty-bath-shower-soap/c/HY00214999'   
    }

def timeit(f):

    def timed(*args, **kw):

        ts = time.time()
        result = f(*args, **kw)
        te = time.time()

        logging.debug('func:%r args:[%r, %r] took: %2.4f sec' % \
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
    page_source = load_more_lulu(driver=driver)

    # Find cards objects from the page source
    cds = find_cards_lulu(pg_source = page_source)

    # Extract data from the cards
    list_dict = extract_data_lulu_new(driver=driver, cards=cds)
    
    # Convert list of dictionaries into dataframe
    df = convert_data_lulu(ld = list_dict)

    # Append category
    df['CATEGORY'] = url_link[0]

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

    out_df = clean_data_lulu(out_df)
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
