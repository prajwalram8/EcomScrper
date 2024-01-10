#Importing required libraries
from cmath import nan
from locale import format_string
import re
import time
import logging
import itertools
import numpy as np
import pandas as pd
from num2words import num2words
from unidecode import unidecode
from logging import exception
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By

#Web scraping Libraries
from bs4 import BeautifulSoup

logger = logging.getLogger('__main__.' + __name__)

# -----------------------------------------------------------------------COMMON UTILITIES-----------------------------------------------------------------------------------

# COMMON UDFs

# Covert a giving string input into upper case
import re

def to_upper(s):
    return s.upper()

# Carry out a string split operation on a given string
def split(s):
    return s.split()

# Convert a given token into words if it is a digit
def token_to_words(c):
    reg=re.match("\d+\.*\d*",c)
    if reg:
        return num2words(c)
    else:
        return c

# Seperate number for characters in a given string
def separate_number_chars(s):
    s=s.replace("-","TO")
    res = re.split('([-+]?\d+\.\d+)|([-+]?\d+)', s.strip())
    res_f = [r.strip() for r in res if r is not None and r.strip() != '']
    return res_f

# Seperate the digits from characters in the given string
def seperate_number_chars_sentence(s):
    s = s.split()
    s = list(map(separate_number_chars, s))
    if any(isinstance(x, list) for x in s):
        new_lst = []
        for a in s:
            if type(a)!=list:
                new_lst.append(a)
            else:
                new_lst.extend(a)
        return new_lst
    else:
        return s

# Convert numbers into words in a given string
def number_to_words(s):
  s = seperate_number_chars_sentence(s)
  s = list(map(token_to_words, s))
  return ' '.join(s).replace(',','').replace('-',' ').replace('%','PERCENT').upper()

def quant_deal_multiplepacket(x):
    '''
    Function : Function to deal Quantities mentioned as additives or multiple pieces or packets
               
    Input : QUANTITY
    
    Output : QUANTITY with added or multiplied quantity values
    
    '''
    p='\d+ x \d+ Packet|\d+ x \d+pcs|\d+[s]* x \d+ *Piece[s]*|x \d+ \d+pcs|\d+Sticks x \d+pcs'
    flags = re.IGNORECASE
    match = re.findall(p,x,flags)
    # print(match)
    if match:
        total = re.findall('\d+',x)
        total_p=1
        for j in total:
            total_p=total_p*int(j)
            # print(x,"-")
            x=x.replace(x,'')
        i=''+str(total_p)
        return i
    else:
        return x

def quant_deal_plusmultiple(x):
    '''
    Function : Function to deal Quantities mentioned as additives or multiple pieces or packets
               
    Input : QUANTITY
    
    Output : QUANTITY with added or multiplied quantity values
    
    '''
    x = str(x)
    count_pieces_multiply = x.count('x')
    count_pieces_plus = x.count('+')
    if count_pieces_multiply>1:
        # print(x,'multiple')
        total = re.findall('\d+',x)
        total_p=1
        for j in total:
            total_p=total_p*int(j)
            x=x.replace(x,'')
        i=''+str(total_p)
        return i
    elif count_pieces_plus==1:
        # print(x,'sum')
        total = re.findall('\d+',x)
        total_p=0
        for j in total:
            total_p=total_p+int(j)
            x=x.replace(x,'')
        i=''+str(total_p)
        return i
    else:
        x=' '.join(x.split())
        return x

def replace_abbreviate(x):
    g = ['G','GM','GRAMS']
    l = ['L','LTR','LT','LITRE','LITER','LTRS']
    gal = ['GAL','GALLONS','GALLON','GALLAN']
    if x in g:
        return 'GRAMS'
    elif x=='KG':
        return 'KILOGRAMS'
    elif x=='MG':
        return "MILLIGRAMS"
    elif x in l:
        return 'LITRES'
    elif x=='ML':
        return "MILLILITRES"
    elif x=='CL':
        return "CENTILITRES"
    elif x=='KL':
        return "KILOLITRES"
    elif x=='OZ':
        return 'OUNCE'
    elif x in gal:
        return 'GALLONS'
    elif x=='LB':
        return 'POUNDS'
    else:
        return 'FAILED'

#Extract the Units from Size (Alphabets) and get the Fullforms - If the alphabet is not a unit, FAILED will be returned
def units_of_measurement(x):
    p ='[a-zA-Z]+'
    flags = re.IGNORECASE
    string = re.findall(p, x, flags)
    if string:
        unit = []
        for i in string:
            i = i.upper()
            units = replace_abbreviate(i)
            unit.append(units)
        return " + ".join(unit)
    else:
        return x

    # -----------------------------------------------------------------------CARREFOUR UTILITIES-----------------------------------------------------------------------------------

def load_more_carrefour(driver,speed=6):
    '''
    @Sonali: Please convert the execute script to fstring and parametrize key css components
    
    '''
    BACK_TO_TOP_BAR_HEIGHT = "'.css-1hpnayn'"
    GLOSSARY_BOX_HEIGHT = "'.css-2mwalt'"
    FOOTER_HEIGHT = "'.css-tx5kky'"
    LOAD_MORE_BUTTON_CLASS = "'.css-1n3fqy0'"

    while True:
        try:
            driver.execute_script(f'let difference = document.querySelector({BACK_TO_TOP_BAR_HEIGHT}).scrollHeight \
                                + document.querySelector({GLOSSARY_BOX_HEIGHT}).scrollHeight + document.querySelector({FOOTER_HEIGHT}).scrollHeight;\
                                  let total_height = document.body.scrollHeight; window.scrollTo(0,total_height);\
                                  window.scrollBy(total_height,-difference);document.querySelector({LOAD_MORE_BUTTON_CLASS}).click();')

            # driver.execute_script("let difference = document.querySelector('.css-1hpnayn').scrollHeight \
            #                       + document.querySelector('.css-2mwalt').scrollHeight + document.querySelector('.css-tx5kky').scrollHeight;\
            #                       let total_height = document.body.scrollHeight; window.scrollTo(0,total_height);\
            #                       window.scrollBy(total_height,-difference);document.querySelector('.css-1n3fqy0').click();\
            #                       ")
            time.sleep(7)
        except:
            break
    driver.implicitly_wait(5)
    new_height=driver.execute_script("return document.body.scrollHeight")
    current_scroll_position=0
    while True:
        current_scroll_position += speed
        driver.execute_script("window.scrollBy({}, {});".format(new_height,-8))
        new_height = driver.execute_script("return document.documentElement.scrollTop")
        if new_height!=0:
            continue
        else:
            time.sleep(2)
            break

    page = driver.page_source
    
    return page
        
def find_cards_carrefour(pg, card_class="css-b9nx4o"):
    # Parsing the source into a html format
    soup = BeautifulSoup(pg, 'html.parser')
    cards = soup.find_all("div", {"class": card_class})
    return cards

def extract_data_carrefour(cards):
    list_dict = []
    for card in cards:
        data={}
        
        try:
            productname = card.find("div", {"class": "css-yqd9tx"}).find(
                "div", {"class": "css-11qbfb"}).find(
                "div", {"class": "css-1nhiovu"}).find("a").string
            # print(productname)
            data['PRODUCTNAME'] = unidecode(productname)
        except AttributeError:
            data['PRODUCTNAME'] = np.NaN
        
        # discount=card.find("div",{"class":"css-1jjmgyu"}).text
            
        # if discount:
            
        #     try:
        #         data['PRODUCTPRICE_ORIGINAL'] = card.find("div", {"class": "css-yqd9tx"}).find(
        #             "div", {"class": "css-11qbfb"}).find(
        #             "div", {"class": "css-1ian0zx"}).find(
        #             "div", {"class": "css-iqeby6"}).text
        #     except AttributeError:
        #         data['PRODUCTPRICE_ORIGINAL'] = np.NaN
            

        #     try:
        #         data['PRODUCTPRICE_DISCOUNT'] = card.find("div", {"class": "css-yqd9tx"}).find(
        #             "div", {"class": "css-11qbfb"}).find(
        #             "div", {"class": "css-1ian0zx"}).find(
        #             "div", {"class": "css-2a09gr"}).text
        #     except AttributeError:
        #         data['PRODUCTPRICE_DISCOUNT'] = np.NaN
       
        # else:
        #     try:
        #         data['PRODUCTPRICE_ORIGINAL'] = card.find("div", {"class": "css-yqd9tx"}).find(
        #             "div", {"class": "css-11qbfb"}).find(
        #             "div", {"class": "css-1ian0zx"}).find(
        #             "div", {"class": "css-fzp91j"}).text
        #     except AttributeError:
        #             data['PRODUCTPRICE_ORIGINAL'] = np.NaN
        
        try:
            data['PRODUCTPRICE_ORIGINAL'] = card.find("div", {"class": "css-yqd9tx"}).find(
                "div", {"class": "css-11qbfb"}).find(
                "div", {"class": "css-1ian0zx"}).find(
                "div", {"class": "css-fzp91j"}).text
        except AttributeError:
                try:
                    data['PRODUCTPRICE_ORIGINAL'] = card.find("div", {"class": "css-yqd9tx"}).find(
                                                            "div", {"class": "css-11qbfb"}).find(
                                                            "div", {"class": "css-1ian0zx"}).find(
                                                            "div", {"class": "css-iqeby6"}).text
                except:
                    data['PRODUCTPRICE_ORIGINAL'] = np.NaN
                
                try:
                    data['PRODUCTPRICE_DISCOUNT'] = card.find("div", {"class": "css-yqd9tx"}).find(
                        "div", {"class": "css-11qbfb"}).find(
                        "div", {"class": "css-1ian0zx"}).find(
                        "div", {"class": "css-2a09gr"}).text
                except AttributeError:
                    data['PRODUCTPRICE_DISCOUNT'] = np.NaN
         
        try:
            data['QUANTITY'] = card.find("div", {"class": "css-yqd9tx"}).find(
                "div", {"class": "css-11qbfb"}).find(
                "div", {"class": "css-1ueix7b"}).text
        except AttributeError:
            data['QUANTITY'] = data['PRODUCTNAME'].apply(lambda x:(re.findall(r'\d+[a-zA-Z ]*\d*',x)[0]))
        try:
            data['ORIGIN'] = card.find("div", {"class": "css-yqd9tx"}).find(
                    "div", {"class": "css-11qbfb"}).find(
                    "div", {"class": "css-4u8vpj"}).text
        except AttributeError:
            data['ORIGIN'] = np.NaN
        
        list_dict.append(data)
    return list_dict


def convert_data_carrefour(ld):
    df = pd.DataFrame(ld)
    return df

# PREPROCESSING FOR CARREFOUR

def add_quantity_carrefour(string):
    '''
    Function : Extract Quantity from Raw Size and Quantity Information Scraped from website
    
    Input : Sizes
    
    Output : Quantity from Sizes - In QUANTITY Column
    
    '''
    pattern1=''' \d* *piece[s]* per KG| *x *\d*| *x *Pack of \d*|\d* count| *\d* *Pieces| 
    *\d+ Tea {0,1}Bag[s]*|pack of \d*|\d+ Bag[s]*|Pack of \d+|\d+ Piece|\d* Rolls|\d+ Sachet[s]*|\d* Cups'''
    
    flags= re.IGNORECASE
    reg=re.findall(pattern1,string,flags)
    try:
        m_string=reg[0]
        m_string=str(m_string)
    except:
        return "None"
    # print(string,m_string)
    return m_string

def product_spec_extractor_size_carrefour(s, p=" \d*\.*\+*\-* *\d* *(?=g|kg|ml|l|litre|oz|Ml|Litre|Kg|Oz|kcal)[0-9]*[a-zA-Z]*"):
    '''
    Function : Extract Size specified in Product Name apart from the scraped Information
    
    Input : Product Names
    
    Output : Sizes from Product Names - In SIZE-FROM-NAME Column
    
    '''
    flags= re.IGNORECASE
    string = re.findall(p, s,flags)
    if string:
        return ''.join(string)
    else:
        return 'None'
    
def product_spec_extractor_quant_carrefour(s, p=" \d* *(?=pcs|pkt|pc|Packet|Bunch|Pieces|Piece|Pcs|pk|Sheets|Diapers|wipes|Rolls|Cups)[0-9]+[a-zA-Z]*$|\d+ *x| \d+ *s| x *\d+|[p|P]ack of \d+|\d *[x|X] *\d*pcs|\d+ Tea {0,1}Bags|x\d+"):
    '''
    Function : Extract Quantity specified in Product Name apart from the scraped Information
    
    Input : Product Names
    
    Output : Quantity from Product Names - In QUANTITY-FROM-NAME Column
    
    '''
    flags= re.IGNORECASE
    string = re.findall(p, s,flags)
    if string:
        return ''.join(string)
    else:
        return 'None' 
    
def product_spec_extractor_size_mod_carrefour(s, p="\d+ *\.*\w* *(?=g|kg|ml|l|litre|oz|Ml|Litre|Kg|Oz|kcal)[0-9]*[a-zA-Z]*"):
    '''
    Function : To Clean the Extracted Size from Product Names
    
    Input : SIZE-FROM-NAME
    
    Output : Cleaned SIZE-FROM-NAME
    
    '''
    
    flags= re.IGNORECASE
    string = re.findall(p, s,flags)
    # print(string)
    if string:
        return '+'.join(string)
    else:
        return 'None' 

def product_spec_extractor_dim_carrefour(s, p=" \(*\d*\.*\d* *x *\d*\.*\d* *x* *\d*\.*\d* *(?=cm|millimeter|centimeter|inch|sqft)[a-z]*\)*"):                      
    '''
    Function : Extract Dimensional Information from the Product Names such height*weight*depth
    
    Input : PRODUCTNAME
    
    Output : DIMENSION
    
    '''
    
    flags= re.IGNORECASE
    string = re.findall(p, s, flags)
    if string:
        return ''.join(string)
    else:
        return 'None'
    
def num2words1_carrefour(s):
    '''
    Function : Replace the Size and Stage numbers by words in order to avoid being identified as Size Info
    
    Input : PRODUCTNAME
    
    Output : PRODUCTNAME
    
    '''
    string = re.findall(r'size \d|stage \d',s)
    if string:
        size=str(string[0][-1])
        num = num2words(size)
        #print(size,num)
        s = re.sub('size \d','size '+str(num),s)
        return s
    else:
        return s

def check_white_label_carrefour(x):
    '''
    Function : Identify White Label Products - Those manufactured and sold under the Source Name itself
    
    Input : PRODUCTNAME
    
    Output : COMMENTS Column 
    
    '''
    match=re.match('CARREFOUR',x)
    if match:
        return 'WHITE-LABEL-PRODUCT'
    else:
        return np.nan

def price_clean_carrefour(x):
    '''
    Function : Clean Product Prices which is Extracted in the format of AED X
    
    Input : ORIGINAL AND DISCOUNT PRICES
    
    Output : CLEANED PRICES
    
    '''
    try:
        m_string = re.findall(r'\d*\.\d*',str(x))[0]
        return m_string
    except:
        return '0'

def clean_size_mod_carrefour(df):
    
    '''
    Function : Certain Sizes extracted the Number from the Product Names. With the help of Units of measurement that failed,
               If Units is Failed , Function removes the corresponding Numeric value from Size which is not a Size
               
    Input : Dataframe - Remove in SIZE-MOD-MID based on UNITS_OF_MEASURMENT
    
    Output : A list with updated Size Values 
    
    '''
    l=[]
    for i,j in zip(df['SIZE_ACT_CLEANED'],df['UNITS_OF_MEASUREMENT']):
        i = str(i)
        if 'FAILED' in j:
                j = list(j.split('+'))
                j = [x.strip() for x in j]
                # print(j)
                index = j.index('FAILED')
                try:
                    i_l = list(re.split(r"[+ \(&]", i))
#                 print(i_l)
                    del i_l[index]
                    new_i = i.replace(i,''.join(i_l))
                    to_return = new_i
                except:
                    to_return = i
#                 print("to_return new",to_return)
        elif j=='FAILED':
            to_return=''
        else:
            to_return = i
        l.append(to_return)
    return l

def add_department_carrefour(data):
    '''
    Function : To Add Manually assigned Department from Categories that aligns with Grandiose E-commerce Listings
    
    Input : Dataframe
    
    Output : Dataframe with DEPARTMENT Column
    
    '''

    data.loc[(data['CATEGORY']=='World Specialities') , ['DEPARTMENT']] = 'WORLD FOOD'
    data.loc[(data['CATEGORY']=='Dairy & eggs') , ['DEPARTMENT']] = 'DAIRY AND EGGS'
    data.loc[(data['CATEGORY']=='Fruits') |
                (data['CATEGORY']=='Vegetables') |
                (data['CATEGORY']== 'Herbs') |
                (data['CATEGORY']=='Fresh Box') |
            (data['CATEGORY']== 'Hydroponic Farming') , ['DEPARTMENT']] = 'FRUITS AND VEGETABLES'

    data.loc[(data['CATEGORY']=='Arabic Bread') |
            (data['CATEGORY']=='Sweets') |
            (data['CATEGORY']=='Bread & Rolls') |
            (data['CATEGORY']=='Crackers & Bread Sticks') |
            (data['CATEGORY']=='Croissants & Cakes') |
            (data['CATEGORY']=='Donuts & Muffins')
            , ['DEPARTMENT']] = 'BAKERY'
    
    data.loc[(data['CATEGORY']=='Bakery') , ['DEPARTMENT']] = 'BAKERY'

    data.loc[(data['CATEGORY']=='Meat & Poultry') , ['DEPARTMENT']] = 'MEAT & POULTRY'
    data.loc[(data['CATEGORY']=='Fish & Seafood') , ['DEPARTMENT']] = 'SEAFOOD'
    data.loc[(data['CATEGORY']=='Frozen Food') , ['DEPARTMENT']] = 'FROZEN'
    data.loc[(data['CATEGORY']=='Coffee') |
            (data['CATEGORY']=='Juices') |
                (data['CATEGORY']=='Tea') |
                (data['CATEGORY']== 'Water') |
                (data['CATEGORY']=='Soft Drinks') |
            (data['CATEGORY']=='Powered Drinks') |
            (data['CATEGORY']=='Kids Drink')
            , ['DEPARTMENT']] = 'BEVERAGES'

    data.loc[(data['CATEGORY']=='Baby Healthcare') |
            (data['CATEGORY']=='Baby Bath') |
            (data['CATEGORY']=='Baby Feed') |
            (data['CATEGORY']=='Milk Food & Juice') |
            (data['CATEGORY']=='Nursery and safety') |
            (data['CATEGORY']=='Baby Travel')
            , ['DEPARTMENT']] = 'BABY PRODUCTS'
    data.loc[(data['CATEGORY']=='Dental Care') |
            (data['CATEGORY']=='Ladies Hair removal') |
            (data['CATEGORY']=='Face & Body Skin care') |
            (data['CATEGORY']=='Hair Care') |
            (data['CATEGORY']=='Makeup & Nails') |
            (data['CATEGORY']=='Mens Grooming') |
            (data['CATEGORY']=='Personal Care') |
            (data['CATEGORY']=='Bath and Body') |
            (data['CATEGORY']=='Suncare & Travel') |
            (data['CATEGORY']=='Toiletries & Perfumes') |
            (data['CATEGORY']=='Makeup') |
            (data['CATEGORY']=='Natural Personal Care') 
            , ['DEPARTMENT']] = 'HEALTH AND BEAUTY'

    data.loc[(data['CATEGORY']=='Petcare') , ['DEPARTMENT']] = 'PET CARE'
    data.loc[(data['CATEGORY']=='Food to Go') |
            (data['CATEGORY']=='Chilled Food Counter') |
            (data['CATEGORY']=='Rice Pasta & Noodles') |
            (data['CATEGORY']=='Biscuits & Cakes') |
            (data['CATEGORY']=='Sugar & Home Baking') |
            (data['CATEGORY']=='Breakfast & Cereals') |
            (data['CATEGORY']=='Chips Dips & Snacks') |
            (data['CATEGORY']=='Condiments & Dressing') |
            (data['CATEGORY']=='Jams') |
            (data['CATEGORY']=='Nuts') |
            (data['CATEGORY']=='Candy') |
            (data['CATEGORY']=='Chocolates') |
            (data['CATEGORY']=='Gums & Mints') |
            (data['CATEGORY']=='Cooking Ingredients') |
            (data['CATEGORY']=='Bio & Organic Food') |
            (data['CATEGORY']=='Tins Jars & Packets')
            , ['DEPARTMENT']] = 'GROCERY'

    data.loc[(data['CATEGORY']=='Candles & Air freshners') |
            (data['CATEGORY']=='Cleaning Supplies') |
            (data['CATEGORY']=='Disposables & Napkins') |
            (data['CATEGORY']=='Food Storage Foil and Cling') |
            (data['CATEGORY']=='Eco Friendly') |
            (data['CATEGORY']=='Garbage Bags') |
            (data['CATEGORY']=='Insect & Pest Control') |
            (data['CATEGORY']=='Laundry Detergents') |
            (data['CATEGORY']=='Tissues') |
            (data['CATEGORY']=='kitchen & Toilet Rolls') |
            (data['CATEGORY']=='Bathroom & Laundry') 
            , ['DEPARTMENT']] = 'HOUSEHOLD'
    return data

def clean_data_carrefour(df):

    df = df.dropna(subset='PRODUCTNAME').reset_index(drop=True)
    #Clean Price Information
    df['PRODUCTPRICE_ORIGINAL'] = df['PRODUCTPRICE_ORIGINAL'].replace(np.nan,'0.0')

    df['PRODUCTPRICE_DISCOUNT'] = df['PRODUCTPRICE_DISCOUNT'].replace(np.nan,'0.0')

    df['PRODUCTPRICE_ORIGINAL']=df['PRODUCTPRICE_ORIGINAL'].apply(lambda x:price_clean_carrefour(x))

    df['PRODUCTPRICE_DISCOUNT']=df['PRODUCTPRICE_DISCOUNT'].apply(lambda x:price_clean_carrefour(x))

     #Convert Size No to words to avoid confusion in Quant
    df['PRODUCTNAME'] = df['PRODUCTNAME'].apply(lambda x:num2words1_carrefour(str(x)))

    df['PRODUCTNAME'] = df['PRODUCTNAME'].apply(lambda x:str(x).replace("\'",''))

    df['PRODUCTNAME'] = df['PRODUCTNAME'].apply(lambda x:' '.join(x.split()))

    #Rename the already recorded quantity column as SIZE
    df.rename(columns = {'QUANTITY':'RAW_SIZE'}, inplace = True)

    df['RAW_SIZE'] = df['RAW_SIZE'].apply(lambda x:str(x).strip())

    #Remove the Keyword Size and replace empty sizes with 0
    df['SIZE'] = df['RAW_SIZE'].apply(lambda x:(re.sub(r'Size:',"",str(x))))

    df['SIZE'] = df['SIZE'].replace('nan','0')

    df['SIZE_FROM_NAME'] = df['PRODUCTNAME'].apply(lambda x:product_spec_extractor_size_carrefour(s=x))

    df['SIZE_MOD'] = df['SIZE_FROM_NAME'].apply(lambda x:product_spec_extractor_size_mod_carrefour(s=x))

    df['SIZE_MOD'] = df['SIZE_MOD'].apply(lambda x:str(x).strip())

    #Extract Dimension Information and remove dimension before applying quantity
    df['DIMENSION'] = df['PRODUCTNAME'].apply(lambda x:product_spec_extractor_dim_carrefour(s=x))

    df['PRODUCTNAME1'] = df['PRODUCTNAME'].copy()

    df['PRODUCTNAME1'] = df.apply(lambda x: x['PRODUCTNAME1'].replace(x['DIMENSION'],''), axis=1) 

    #Extract Quantity Information from Size 
    df['SIZE-MID'] = df['SIZE'].copy()

    df['QUANTITY'] = df['SIZE-MID'].apply(lambda x:add_quantity_carrefour(str(x)))

    df['SIZE'] = df.apply(lambda x: x['SIZE'].replace(x['QUANTITY'],''), axis=1)

    df['QUANTITY_FROM_NAME'] = df['PRODUCTNAME1'].apply(lambda x:product_spec_extractor_quant_carrefour(s=x))

#     df['QUANTITY'] = df['QUANTITY'].apply(lambda x:(re.sub(r'x|X','',str(x))))

    df['SIZE'] = df.apply(lambda x: x['SIZE'].replace(x['QUANTITY_FROM_NAME'],''), axis=1)

    df['PRODUCTNAME_MOD'] = df.apply(lambda x: x['PRODUCTNAME'].replace(x['SIZE_MOD'],' '), axis=1)

    df['PRODUCTNAME_MOD'] = df.apply(lambda x: x['PRODUCTNAME_MOD'].replace(x['QUANTITY'],' '), axis=1)

    df['PRODUCTNAME_MOD'] = df.apply(lambda x: x['PRODUCTNAME_MOD'].replace(x['QUANTITY_FROM_NAME'],' '), axis=1)
    
#     df['PRODUCTNAME_MOD'] = df.apply(lambda x: x['PRODUCTNAME_MOD'].replace(x['DIMENSION'],''), axis=1) 

    #Combine the 2 Size columns and 2 Quantity Columns
    df['SIZE'] = df['SIZE'].replace('0',np.nan)
    df['SIZE_MOD'] = df['SIZE_MOD'].replace('None',np.nan)

    df['QUANTITY'] = df['QUANTITY'].replace('0',np.nan)
    df['QUANTITY'] = df['QUANTITY'].replace('None',np.nan)
    df['QUANTITY_FROM_NAME'] = df['QUANTITY_FROM_NAME'].replace('None',np.nan)

    df['SIZE_ACT_CLEANED'] = (
        df['SIZE'].combine_first(df['SIZE_MOD']))

    df['QUANT_ACT'] = (
        df['QUANTITY'].combine_first(df['QUANTITY_FROM_NAME']))

    df['SIZE_ACT_CLEANED']= df['SIZE_ACT_CLEANED'].apply(
    lambda x:re.sub(r'(Appro[x]* \d+ piece[s]* per KG)|(Average Nutritional)|(Appro.)|(Approx.)|(\d* *[c|C]ount)|(Pack Of \d*)|(Pack of \d*)|x|X|\d+ Tea Bag[s]*| *\d+ Piece[s]*| \d* Cups| \d* Rolls|\d+ Sachet[s]*','',str(x)))
    
    df['SIZE_ACT_CLEANED'] = df['SIZE_ACT_CLEANED'].apply(lambda x:str(x).replace(' ',''))

    df['QUANTITY'] = df['QUANTITY'].apply(lambda x:quant_deal_plusmultiple(str(x)))
    
    df['QUANTITY'] = df['QUANTITY'].apply(lambda x:re.sub('pkt',' Packet',x))

    df['QUANTITY'] = df['QUANTITY'].apply(lambda x:quant_deal_multiplepacket(str(x)))
    
    df['QUANT_ACT'] = df['QUANT_ACT'].apply(lambda x:re.sub(r'x|X|s$|S$| [c|C]ount| [p|P]iece',' ',str(x)))

    df['QUANT_ACT'] = df['QUANT_ACT'].apply(lambda x:str(x).upper())

    df['UNITS_OF_MEASUREMENT'] = df['SIZE_ACT_CLEANED'].apply(lambda x:units_of_measurement(x))
    
    l = clean_size_mod_carrefour(df)
    
    df1 = pd.DataFrame({'SIZE_ACT':l})

    df['SIZE_ACT'] = df1.copy() 

    df['UNITS_OF_MEASUREMENT_FAILED'] = df['UNITS_OF_MEASUREMENT'].copy()

    df['UNITS_OF_MEASUREMENT'] = df['UNITS_OF_MEASUREMENT'].apply(lambda x:re.sub(r'FAILED|\d+','',str(x)))
    
    df['UNITS_OF_MEASUREMENT'] = df['UNITS_OF_MEASUREMENT'].apply(lambda x:re.sub(r'\+',' ',str(x)))

    df['UNITS_OF_MEASUREMENT'].replace(np.nan,'')

    df['PRODUCTNAME_MOD'] = df.apply(lambda x: x['PRODUCTNAME_MOD'].replace(x['SIZE_ACT'],''), axis=1)
    
#     df['PRODUCTNAME_MOD'] = df.apply(lambda x: x['PRODUCTNAME_MOD'].replace(x['QUANT_ACT'],''), axis=1)

    df['SIZE_ACT'] = df['SIZE_ACT'].apply(lambda x:re.sub(r'[a-zA-Z]+','',str(x)))
    
    df.loc[(df['UNITS_OF_MEASUREMENT']==''),['SIZE_ACT']] = ''
    
    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:str(x).rstrip('PACK OF'))
    
    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:' '.join(x.split()))
    
    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:str(x).strip().upper())

    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:number_to_words(x))

    df['COMMENTS'] = df['PRODUCTNAME_MOD'].apply(lambda x:check_white_label_carrefour(x))

    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:str(x).replace("\,",''))
    
    df = add_department_carrefour(df)
    
    df = df.replace("None",'')

    df = df.replace("nan",'')

    df = df.replace("0.0",'')

    df[['PRODUCTPRICE_ORIGINAL','PRODUCTPRICE_DISCOUNT']] = df[['PRODUCTPRICE_ORIGINAL','PRODUCTPRICE_DISCOUNT']].replace('',0)

    df = df[['PRODUCTNAME','PRODUCTNAME_MOD','PRODUCTPRICE_ORIGINAL','PRODUCTPRICE_DISCOUNT','ORIGIN','DIMENSION','RAW_SIZE','SIZE','SIZE_MOD',
    'QUANTITY','QUANTITY_FROM_NAME','SIZE_ACT','UNITS_OF_MEASUREMENT','UNITS_OF_MEASUREMENT_FAILED','QUANT_ACT','CATEGORY','DEPARTMENT','COMMENTS']]
    
    return df
    
    
# -----------------------------------------------------------------------LULU UTILITIES-----------------------------------------------------------------------------------
def load_more_lulu(driver,speed=5.5):
    current_scroll_position, new_height= 0, 1
    try:
        button1=driver.find_elements("xpath","//*[@id='js-cookie-notification']/div/div[2]/button")
        button2=driver.find_elements("xpath","/html/body/main/main/header/section[2]/div[1]/div/div/div/section/div/div/div[2]/ul/li[1]/button")
        button1[0].click()
        button2[0].click()
    except:
        print('No popups')
    time.sleep(2)
        
    while current_scroll_position <= new_height:
        #print(current_scroll_position,new_height)
        current_scroll_position += speed
        driver.execute_script("window.scrollTo(0, {});".format(current_scroll_position))
        wait(driver, 15).until(ec.presence_of_all_elements_located((By.CLASS_NAME, 'product-box')))
        new_height = driver.execute_script("return document.body.scrollHeight")

    #Scroll and load
    wait(driver, 15).until(ec.presence_of_all_elements_located((By.CLASS_NAME, 'product-box')))
    driver.execute_script("window.scrollTo(0, 0)")
    pg_source = driver.page_source
    return pg_source
    
def xpath_soup(element):
    """
    Generate xpath of soup element
    :param element: bs4 text or node
    :return: xpath as string
    """
    components = []
    child = element if element.name else element.parent
    for parent in child.parents:
        """
        @type parent: bs4.element.Tag
        """
        previous = itertools.islice(parent.children, 0, parent.contents.index(child))
        xpath_tag = child.name
        xpath_index = sum(1 for i in previous if i.name == xpath_tag) + 1
        components.append(xpath_tag if xpath_index == 1 else '%s[%d]' % (xpath_tag, xpath_index))
        child = parent
    components.reverse()
    return '/%s' % '/'.join(components)

def extract_data_variant(driver,card,href):
    list_dict=[]
    
    for (card,link) in zip(card,href):

        try:
            driver.get(link)
        # driver.execute_script(f"location.href='{link}';")
            time.sleep(3)
        except:
            print(link,'is the error')
            
        page=driver.page_source
        soup = BeautifulSoup(page,"html.parser")
        
        try:
            button_list=soup.find("div", {"class": "product-description"}).find(
                    "div", {"class": "switch-variant flex-wrap"}).find_all("a")

            for buttoni in button_list:

                data={}

                element=xpath_soup(buttoni)
                button=driver.find_elements("xpath",element)
                button[0].click()
                time.sleep(3)

                page=driver.page_source
                soup = BeautifulSoup(page,"html.parser")


                try:
                    productname_l = soup.find("div", {"class": "product-description"}).find(
                                "h1", {"class": "product-name"}).text
                    data['PRODUCTNAME'] = unidecode(productname_l)
                except AttributeError:
                    data['PRODUCTNAME'] = np.NaN

                if len(card.find(
                    "div", {"class": "product-content"}).find("div", {"class": "product-desc"}).find("p", {"class": "product-price has-icon"}).find_all("span")) == 1:
                    try:

                        data['PRODUCTPRICE_ORIGINAL'] = soup.find(
                        "div", {"class": "product-description"}).find(
                        "div", {"class": "row mb-3"}).find(
                        "div",{"class": "col-auto"}).find(
                        "div",{"class": "price-tag detail"}).find(
                        "span",{"class": "current"}).find(
                        "span",{"class":"item price"}).find("span").text

                    except AttributeError:
                        data['PRODUCTPRICE_ORIGINAL'] = np.NaN   
                else:
                    try:
                        data['PRODUCTPRICE_ORIGINAL'] = soup.find(
                        "div", {"class": "product-description"}).find(
                        "div", {"class": "row mb-3"}).find(
                        "div",{"class": "col-auto"}).find(
                        "div",{"class": "price-tag detail"}).find(
                        "span",{"class": "off"}).text
                    except AttributeError:
                        data['PRODUCTPRICE_ORIGINAL'] = np.NaN
                    try:
                        data['PRODUCTPRICE_DISCOUNT'] = soup.find(
                        "div", {"class": "product-description"}).find(
                        "div", {"class": "row mb-3"}).find(
                        "div",{"class": "col-auto"}).find(
                        "div",{"class": "price-tag detail"}).find(
                        "span",{"class": "current"}).find(
                        "span",{"class":"item price"}).find("span").text
                    except AttributeError:
                        data['PRODUCTPRICE_DISCOUNT'] = np.NaN  
                list_dict.append(data)
        except:
                print("No Buttons")
        #list_dict.append(data)
    return (list_dict)


def find_cards_lulu(pg_source, card_class="product-box"):
    # Parsing the source into a html format
    soup = BeautifulSoup(pg_source, 'html.parser')
    cards = soup.find_all("div", {"class": card_class})
    return cards

def extract_data_lulu_new(driver,cards):
    list_dict = []
    l=[]
    h=[]
    for card in cards:
        data = {}
        packvariant=card.find(
            "div", {"class": "product-content"}).find(
            "div", {"class": "product-desc"}).find(
            "div",{"class":"product-pack-variants"}).find(
            "div",{"class":"switch-variant size-variant-round d-none d-lg-flex selection-ul"}).find(
            "div",{"class":"item"})

        if (packvariant is not None):
            l.append(card)
            #link = elem.get_attribute('href')
            href=card.find("div", {"class": "product-img"}).find("a",{"class":"js-gtm-product-link"})
            element=xpath_soup(href)
            #print(element)
            element = driver.find_elements("xpath",element)
            #print(element)
            href=element[0].get_attribute('href')
            #print(href)
            h.append(href)
            
        else:
            try:
                productname_l2 = card.find(
                "div", {"class": "product-content"}).find("div", {"class": "product-desc"}).find("h3").text
                data['PRODUCTNAME'] = unidecode(productname_l2)
            except AttributeError:
                data['PRODUCTNAME'] = np.NaN

            if len(card.find(
                "div", {"class": "product-content"}).find("div", {"class": "product-desc"}).find("p", {"class": "product-price has-icon"}).find_all("span")) == 1:
                try:
                    data['PRODUCTPRICE_ORIGINAL'] = card.find(
                        "div", {"class": "product-content"}).find(
                        "div", {"class": "product-desc"}).find(
                        "p", {"class": "product-price has-icon"}).find_all("span")[0].text
                except AttributeError:
                    data['PRODUCTPRICE_ORIGINAL'] = np.NaN
                data['PRODUCTPRICE_DISCOUNT'] = np.NaN
            else:
                try:
                    data['PRODUCTPRICE_ORIGINAL'] = card.find(
                        "div", {"class": "product-content"}).find(
                        "div", {"class": "product-desc"}).find(
                        "p", {"class": "product-price has-icon"}).find_all("span",{"class":"old-price"})[0].text
                except AttributeError:
                    data['PRODUCTPRICE_ORIGINAL'] = np.NaN
                try:
                    data['PRODUCTPRICE_DISCOUNT'] = card.find(
                        "div", {"class": "product-content"}).find("div", {"class": "product-desc"}).find(
                        "p", {"class": "product-price has-icon"}).find_all("span")[1].text
                except AttributeError:
                    data['PRODUCTPRICE_DISCOUNT'] = np.NaN
        list_dict.append(data)
        
    variant_list=extract_data_variant(driver,l,h)
    list_dict=list_dict+variant_list
    return list_dict

def extract_data_lulu_old(cards):
    list_dict = []
    for card in cards:
        data = {}
        try:
            data['PRODUCTNAME'] = card.find(
                "div", {"class": "product-content"}).find("div", {"class": "product-desc"}).find("h3").text
        except AttributeError:
            data['PRODUCTNAME'] = np.NaN
            
        if len(card.find(
            "div", {"class": "product-content"}).find("div", {"class": "product-desc"}).find("p", {"class": "product-price has-icon"}).find_all("span")) == 1:
            try:
                data['PRODUCTPRICE_ORIGINAL'] = card.find(
                    "div", {"class": "product-content"}).find(
                    "div", {"class": "product-desc"}).find(
                    "p", {"class": "product-price has-icon"}).find_all("span")[0].text
            except AttributeError:
                data['PRODUCTPRICE_ORIGINAL'] = np.NaN
            data['PRODUCTPRICE_DISCOUNT'] = np.NaN
        else:
            try:
                data['PRODUCTPRICE_ORIGINAL'] = card.find(
                    "div", {"class": "product-content"}).find(
                    "div", {"class": "product-desc"}).find(
                    "p", {"class": "product-price has-icon"}).find_all("span",{"class":"old-price"})[0].text
            except AttributeError:
                data['PRODUCTPRICE_ORIGINAL'] = np.NaN
            try:
                data['PRODUCTPRICE_DISCOUNT'] = card.find(
                    "div", {"class": "product-content"}).find("div", {"class": "product-desc"}).find(
                    "p", {"class": "product-price has-icon"}).find_all("span")[1].text
            except AttributeError:
                data['PRODUCTPRICE_DISCOUNT'] = np.NaN
        list_dict.append(data)
    return list_dict


def convert_data_lulu(ld):
    df = pd.DataFrame(ld)
    df = df.loc[~((df['PRODUCTNAME'] == '0'))]
    return df    
    
# PREPROCESSING FOR LULU   
    
def product_spec_extractor_quant_lulu(s, p=" \d* *(?=teabags|pcs|pkt|pc|Packet|Bunch|Pieces|Pcs|Sheets|Rolls)[0-9]*[a-zA-Z]*$|\d+ *x| \d+ *s| x *\d+|pack of \d+|\d x \d*pcs|\d+\+\d*$"):
    '''
    Function : Extract Quantity specified in Product Name
    
    Input : Product Names
    
    Output : Quantity from Product Names - In QUANTITY Column
    
    '''
    flags= re.IGNORECASE
    string = re.findall(p, s,flags)
    if string:
        return ''.join(string)
    else:
        return 'None' 
    
def product_spec_extractor_size_lulu(s, p=" \d*\.*\+*\-* *\d*(?=g|kg|ml|l|litre|oz|s|Ml|ML|Litre|Kg|cl|gal|gallon[s]*|gallan[s]*)[0-9]*[a-zA-Z]*"):
    '''
    Function : Extract Size specified in Product Name
    
    Input : Product Names
    
    Output : Sizes from Product Names - In SIZE Column
    '''
    flags= re.IGNORECASE
    string = re.findall(p, s,flags)
    if string:
        return ''.join(string)
    else:
        return '1'
    
def product_spec_extractor_size_mod_lulu(s, p=" \d+\.*\-*\d* *(?=g|gm|kg|l|ml|litre|oz|cl|lb|gal|ltr|ltrs|mg|gallon[s]*|gallan[s]*)[a-zA-Z]+|kg"):
#                                     \d+ *\.*\w*[ LtrlsSkKGMmgxozitre.-]*"):
    '''
    Function : To Clean the Extracted Size from Product Names
    
    Input : SIZE ( Extracted from ProductNames )
    
    Output : Cleaned SIZE in SIZE-MOD-MID
    
    '''
    flags= re.IGNORECASE
    string = re.findall(p, s,flags)
    if string:
        return '+'.join(string)
    else:
        return 'None'
    
                                     
def product_remove_size_lulu(s, p=" \d+\.*\d* *(?=g|G|gm|kg|l|ml|litre|oz|cl|gal|gallon[s]*|gallan[s]*)[a-zA-Z]+"):
#                         \d+\.*\-* *\d*(?=g|kg|ml|l|litre|oz|s|Ml|ML|Litre|Kg)[0-9]*[a-zA-Z]*"):
    flags= re.IGNORECASE
    string = re.findall(p, s,flags)
    if string:
        for i in string:
            s = s.replace(i,'')
        return s
    else:
        return s
    
def product_spec_extractor_dim_lulu(s, p=" \d+(?=cm|m|mm|sq)[\.a-z]* *x *\d+(?=cm|m|mm|sq)[\.a-z]* *x* *\d*(?=cm|m|mm|sq)[\.a-z]*"):
    '''
    Function : Extract Dimensional Information from the Product Names such height*weight*depth
    
    Input : PRODUCTNAME
    
    Output : DIMENSION
    
    '''
    flags= re.IGNORECASE
    string = re.findall(p, s,flags)
    if string:
        return ''.join(string)
    else:
        return 'None'
     
def num2words1_lulu(s):
    '''
    Function : Replace the Size and Stage numbers by words in order to avoid being identified as Size Info
    
    Input : PRODUCTNAME
    
    Output : PRODUCTNAME
    
    '''
    flags = re.IGNORECASE
    string = re.findall(r'Size \d|Stage \d',s,flags)
    if string:
        size=str(string[0][-1])
        num = num2words(size)
        s = re.sub('Size \d','Size '+str(num),s, flags)
        s = re.sub('Stage \d','Stage '+str(num),s, flags)
        return s
    else:
        return s

def quant_deal_lulu(s):
    if s==' Pieces' or s==' Packets' or s==' Bunches' or s==' Packet':
        s= re.sub(r' Pieces| Packet[s]*| Bunches','None',s)
        return s
    else:
        return s

def price_clean_lulu(x):
    '''
    Function : Clean Product Prices which is Extracted in the format of AED X
    
    Input : ORIGINAL AND DISCOUNT PRICES
    
    Output : CLEANED PRICES
    
    '''
    try:
        m_string = re.findall(r'\d*\.\d*',str(x))[0]
        return m_string
    except:
        return '0'

def clean_size_mod_lulu(df):
    
    '''
    Function : Certain Sizes extracted the Number from the Product Names. With the help of Units of measurement that failed,
               If Units is Failed , Function removes the corresponding Numeric value from Size which is not a Size
               
    Input : Dataframe - Remove in SIZE-MOD-MID based on UNITS_OF_MEASURMENT
    
    Output : A list with updated Size Values 
    
    '''
    l=[]
    for i,j in zip(df['SIZE_MOD_MID'],df['UNITS_OF_MEASUREMENT']):
        i = str(i)
        if 'FAILED +' in j:
                j = list(j.split('+'))
                index = j.index('FAILED ')
                i_l = list(i.split('+'))
                del i_l[index]
                new_i = i.replace(i,''.join(i_l))
                to_return = new_i
#                 print("to_return new",to_return)
        elif j=='FAILED':
            to_return=''
        else:
            to_return = i
        l.append(to_return)
    return l

# Function to add unified Department 
def add_department_lulu(data):

    '''
    Function : To Add Manually assigned Department from Categories that aligns with Grandiose E-commerce Listings
    
    Input : Dataframe
    
    Output : Dataframe with DEPARTMENT Column
    
    '''

    data.loc[(data['CATEGORY']=='WorldFoods') , ['DEPARTMENT']] = 'WORLD FOOD'
    data.loc[(data['CATEGORY']=='Fruits&Vegetables') |
            (data['CATEGORY']=='Fresh Juice & Salad')
            , ['DEPARTMENT']] = 'FRUITS AND VEGETABLES'
    data.loc[(data['CATEGORY']=='Dairy') , ['DEPARTMENT']] = 'DAIRY AND EGGS'
    data.loc[(data['CATEGORY']=='Bakery') , ['DEPARTMENT']] = 'BAKERY'
    data.loc[(data['CATEGORY']=='FreshChicken') , ['DEPARTMENT']] = 'MEAT & POULTRY'
    data.loc[(data['CATEGORY']=='FreshMeat&Seafood') , ['DEPARTMENT']] = 'SEAFOOD'
    data.loc[(data['CATEGORY']=='FrozenFood') , ['DEPARTMENT']] = 'FROZEN'
    data.loc[(data['CATEGORY']=='Beverages') , ['DEPARTMENT']] = 'BEVERAGES'
    data.loc[(data['CATEGORY']=='Chips&Snacks') |
            (data['CATEGORY']=='SpecialityFood') |
            (data['CATEGORY']=='CannedFood') |
            (data['CATEGORY']=='HomeBaking&Sweeteners') |
            (data['CATEGORY']=='Delicatessen') |
            (data['CATEGORY']=='Ready Meals') |
            (data['CATEGORY']=='Table&Sauces') |
            (data['CATEGORY']=='Pasta&rice') |
            (data['CATEGORY']=='Biscuits&Confectionary') |
            (data['CATEGORY']=='Cooking Ingredients') |
            (data['CATEGORY']=='Breakfast&spreads') 
            , ['DEPARTMENT']] = 'GROCERY'
    data.loc[(data['CATEGORY']=='Laundry') |
            (data['CATEGORY']=='Cleaning') |
            (data['CATEGORY']=='Air Freshner') |
            (data['CATEGORY']=='Paper Goods') |
            (data['CATEGORY']=='Home Essentials') |
            (data['CATEGORY']=='Food Storage') 
            , ['DEPARTMENT']] = 'HOUSEHOLD'
    data.loc[(data['CATEGORY']=='Baby Products') , ['DEPARTMENT']] = 'BABY PRODUCTS'
    data.loc[(data['CATEGORY']=='Pets') , ['DEPARTMENT']] = 'PET CARE'
    data.loc[(data['CATEGORY']=='Electrical Goods') |
            (data['CATEGORY']=='Flowers')
            , ['DEPARTMENT']] = 'MISCELLANEOUS'
    data.loc[(data['CATEGORY']=='Dental Health Care') |
            (data['CATEGORY']=='Make up') |
            (data['CATEGORY']=='Premium Perfumes') |
            (data['CATEGORY']=='Bath') |
            (data['CATEGORY']=='Hair care') |
            (data['CATEGORY']=='Mens caring') |
            (data['CATEGORY']=='Facial & Skin Care') |
            (data['CATEGORY']=='Feminine care') 
            , ['DEPARTMENT']] = 'HEALTH AND BEAUTY'
    return data

def check_white_label_lulu(x):
    '''
    Function : Identify White Label Products - Those manufactured and sold under the Source Name itself
    
    Input : PRODUCTNAME
    
    Output : COMMENTS Column 
    
    '''
    match=re.match('LULU',x)
    if match:
        return 'WHITE-LABEL-PRODUCT'
    else:
        return np.nan

def clean_data_lulu(df):

    df = df.dropna(subset='PRODUCTNAME').reset_index(drop=True)
    
    #Remove Extra Spaces and commas within the Product Names
    df['PRODUCTNAME'] = df['PRODUCTNAME'].apply(lambda x:" ".join(x.split()))

    df['PRODUCTNAME']=df['PRODUCTNAME'].apply(lambda x:str(x).replace("\'",''))

    #Convert Size No to words to avoid confusion in Quant
    df['PRODUCTNAME'] = df['PRODUCTNAME'].apply(lambda x:num2words1_lulu(str(x)))

    #Extract Level 1 of Size Information from Product Name
    df['SIZE'] = df['PRODUCTNAME'].apply(lambda x:product_spec_extractor_size_lulu(s=x))

    #Extract Dimension Information and remove dimension before applying quantity
    df['DIMENSION'] = df['PRODUCTNAME'].apply(lambda x:product_spec_extractor_dim_lulu(s=x))

    df['PRODUCTNAME1'] = df['PRODUCTNAME'].copy()

    df['PRODUCTNAME1'] = df.apply(lambda x: x['PRODUCTNAME1'].replace(x['DIMENSION'],''), axis=1) 

    #Extract Quantity Information from Product Name
    df['QUANTITY'] = df['PRODUCTNAME1'].apply(lambda x:product_spec_extractor_quant_lulu(s=x))

    #Clean Extracted Size Information
    df['SIZE'] = df['SIZE'].apply(lambda x:str(x).rstrip())

    df['SIZE_MOD_MID'] = df['SIZE'].apply(lambda x:product_spec_extractor_size_mod_lulu(s=x))

    #df['SIZE_MOD_MID'] = df['SIZE_MOD_MID'] .apply(lambda x:re.sub('1$','None',x))

    df['UNITS_OF_MEASUREMENT'] = df['SIZE_MOD_MID'].apply(lambda x:units_of_measurement(x))

    # Remove Size and Quant Information from the Product Name

    df['QUANTITY'] = df['QUANTITY'].apply(lambda x:re.sub('pkt',' Packet',x))

    df['QUANTITY'] = df['QUANTITY'].apply(lambda x:quant_deal_multiplepacket(str(x)))

    df['QUANTITY'] = df['QUANTITY'].apply(lambda x:quant_deal_lulu(x))

    df['PRODUCTNAME_MOD'] = df.apply(lambda x: x['PRODUCTNAME'].replace(x['SIZE_MOD_MID'],''), axis=1)
    
    df['SIZE_MOD_MID'] = df['SIZE_MOD_MID'].apply(lambda x:re.sub(r'[a-zA-Z]+','',x))

    df['PRODUCTNAME_MOD'] = df.apply(lambda x: x['PRODUCTNAME_MOD'].replace(x['QUANTITY'],''), axis=1)

    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:product_remove_size_lulu(x))

    l = clean_size_mod_lulu(df)

    df_size = pd.DataFrame({'SIZE_MOD':l})

    df['SIZE_MOD'] = df_size.copy()  
    
    df['UNITS_OF_MEASUREMENT_FAILED'] = df['UNITS_OF_MEASUREMENT'].copy()

    df['UNITS_OF_MEASUREMENT'] = df['UNITS_OF_MEASUREMENT'].apply(lambda x:re.sub('FAILED +|FAILED','',x))

    #Remove Trailing Whitespaces from Product Name
    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:str(x).lstrip())

    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:str(x).rstrip())

    #Clean Price Data
    df['PRODUCTPRICE_ORIGINAL']=df['PRODUCTPRICE_ORIGINAL'].apply(lambda x:str(x).replace(u'\xa0',u' '))

    df['PRODUCTPRICE_DISCOUNT']=df['PRODUCTPRICE_DISCOUNT'].apply(lambda x:str(x).replace(u'\xa0',u' '))

    df=df.replace('nan','0.0')

    df['PRODUCTPRICE_ORIGINAL']=df['PRODUCTPRICE_ORIGINAL'].apply(lambda x:price_clean_lulu(x))

    df['PRODUCTPRICE_DISCOUNT']=df['PRODUCTPRICE_DISCOUNT'].apply(lambda x:price_clean_lulu(x))
    
    df = add_department_lulu(df)
    
    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:' '.join(x.split()))

    df['PRODUCTNAME'] = df['PRODUCTNAME'].apply(lambda x:str(x).strip())

    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:x.upper())

    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:number_to_words(x))

    df['COMMENTS'] = df['PRODUCTNAME_MOD'].apply(lambda x:check_white_label_lulu(x))

    df['PRODUCTNAME-MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:str(x).replace("\,",' '))

    df['QUANTITY'] = df['QUANTITY'].apply(lambda x:re.sub('\+$|\++$','None',x))

    df['QUANTITY'] = df['QUANTITY'].apply(lambda x:quant_deal_plusmultiple(str(x)))
    
    df['QUANTITY'] = df['QUANTITY'].apply(lambda x:re.sub('x|X|Piece[s]*|pcs|pc|s$|S$|Pc','',x))

    df = df.replace("None",'')

    df = df.replace("0.0",'')

    df[['PRODUCTPRICE_ORIGINAL','PRODUCTPRICE_DISCOUNT']] = df[['PRODUCTPRICE_ORIGINAL','PRODUCTPRICE_DISCOUNT']].replace('',0)
    
    df = df[['PRODUCTNAME','PRODUCTNAME_MOD','PRODUCTPRICE_ORIGINAL',
             'PRODUCTPRICE_DISCOUNT','SIZE','SIZE_MOD','UNITS_OF_MEASUREMENT','UNITS_OF_MEASUREMENT_FAILED'
             ,'QUANTITY','DIMENSION','CATEGORY','DEPARTMENT','COMMENTS']]
    
    return df
#------------------------------------------------------------------------- SPINNEYS UTILITIES  -----------------------------------------------------------------------------

def find_product_spinney(pg_source, product_class="js-product-wrapper product-bx"):
    # Parsing the source into a html format to sort the product class
    try:
        soup = BeautifulSoup(pg_source, 'html.parser')
        prod = soup.find_all("div", {"class": product_class})
        return prod
    except exception as e:
        logger.error(f'f"The following error has was raised {exception},exc_info=True')

def prcreate_spinney(products):
    try:
        list_dict = []
        # Get the value of Product-name and Product-price from the product class objects.
        for pr in products:
            data = {}
            try:
                productname_s = pr.find(
                    "div", {"class": "product-info"}).find("p", {"class": "product-name"}).text
                data['PRODUCTNAME'] = unidecode(productname_s)
            except AttributeError:
                data['PRODUCTNAME'] = np.NaN
            try:
                data['PRODUCTPRICE'] = pr.find(
                    "div", {"class": "product-info"}).find("p", {"class": "product-price"}).text
            except AttributeError:
                data['PRODUCTPRICE'] = np.NaN
            list_dict.append(data)
        return(list_dict)

    except exception as e:
        logger.error(f'f"The following error has was raised {exception},exc_info=True')


def product_spec_extractor_size_mod_spinney(s, p=" \d+\.*\d* *(?=g|gm|kg|l|ml|litre|oz|cl|lb|gal|ltr|ltrs|mg)[a-zA-Z]+|kg|\d+g"):
    '''
    Function : To Clean the Extracted Size from Product Names
    
    Input : SIZE ( Extracted from ProductNames )
    
    Output : Cleaned SIZE in SIZE-MOD-MID
    
    '''
    flags= re.IGNORECASE
    string = re.findall(p, s,flags)
    #print(string)
    if string:
        return ''.join(string)
    else:
        return 'None'    

def product_spec_extractor_size_spinney(s, p=" *\d*\.*\d* *(?=g|kg|ml|L|ltr|cl|litre|oz|lb|gal|ltrs|gm|mg)[0-9]*[a-zA-Z]*$|\d+ {0,1}[g|kg|l|m]"):
    '''
    Function : Extract Size specified in Product Name
    
    Input : Product Names
    
    Output : Sizes from Product Names - In SIZE Column
    '''
    flags=re.IGNORECASE
    string = re.findall(p, s,flags)
    if string:
        return ''.join(string)
    else:
        return 'None'
     
def product_spec_extractor_quant_spinney(s, p=" {1,2}x *\d*|\d+ *x$|\d+s[^t]|\d+x|\d* *pcs| \d* x|\d+s$|\d+ pack|tea \d* bags|pack of \d* pieces|\d+ *pieces"):
    '''
    Function : Extract Quantity specified in Product Name
    
    Input : Product Names
    
    Output : Quantity from Product Names - In QUANTITY Column
    
    '''
    flags=re.IGNORECASE
    string = re.findall(p, s,flags)
    if string:
        return ''.join(string)
    else:
        return 'None'
    
def product_remove_quant_spinney(s, p=" {1,2}x *\d*|\d+ *x$|\d+s[^t]|\d+x|\d* *pcs| \d* x|\d+s$|\d+ pack|tea \d* bags|pack of \d* [p|P]ieces|\d* *pieces"):
    '''
    Function : Extract Quantity specified in Product Name
    
    Input : Product Names
    
    Output : Quantity from Product Names - In QUANTITY Column
    
    '''
    flags=re.IGNORECASE
    string = re.findall(p, s,flags)
    # print(string)
    if string:
        for i in string:
            if i!=' x' and i!=' X':
                s=s.replace(i,'')
        return s
    else:
        return s

def product_spec_extractor_dim_spinney(s, p=" \d*\.*\d*[cmmsqft]* *[x] *\d*\.*\d*cm *|\d+\.*\d*cm|\d*x\d*x\d*[cmm]*|\d*\.*\d*(?=sqm|sq ft)[a-z ]*"):
    '''
    Function : Extract Dimensional Information from the Product Names such height*weight*depth
    
    Input : PRODUCTNAME
    
    Output : DIMENSION
    
    '''
    
    flags=re.IGNORECASE
    string = re.findall(p, s,flags)
    if string:
        return ''.join(string)
    else:
        return 'None'
    
def num2words1_spinney(s):
    
    '''
    Function : Replace the Size and Stage numbers by words in order to avoid being identified as Size Info
    
    Input : PRODUCTNAME
    
    Output : PRODUCTNAME
    
    '''
    string = re.findall(r'size \d|stage \d',s)
    # print(string)
    if string:
        size=str(string[0][-1])
        num = num2words(size)
        # print(size,num)
        s = re.sub('size \d','size '+str(num),s)
        return s
    else:
        return s

def check_white_label_spinney(x):
    '''
    Function : Identify White Label Products - Those manufactured and sold under the Source Name itself
    
    Input : PRODUCTNAME
    
    Output : COMMENTS Column 
    
    '''
    match=re.match('SPINNEY',x)
    if match:
        return 'WHITE-LABEL-PRODUCT'
    else:
        return 'None'

def add_department_spinney(data):

    data.loc[(data['CATEGORY']=='World foods') , ['DEPARTMENT']] = 'WORLD FOOD'
    
    data.loc[(data['CATEGORY']=='Baby and toddler'), ['DEPARTMENT']] = 'BABY PRODUCTS'
    
    data.loc[(data['CATEGORY']=='Bakery'), ['DEPARTMENT']] = 'BAKERY'
    
    data.loc[(data['CATEGORY']=='Beauty and Cosmetics'), ['DEPARTMENT']] = 'HEALTH AND BEAUTY'
    
    data.loc[(data['CATEGORY']=='Butchery') , ['DEPARTMENT']] = 'MEAT & POULTRY'
    
    data.loc[(data['CATEGORY']=='FoodCupboard') |
            (data['CATEGORY']=='Non-Muslim') 
            , ['DEPARTMENT']] = 'GROCERY'
    
    data.loc[(data['CATEGORY']=='Fruits&Veggies') , ['DEPARTMENT']] = 'FRUITS AND VEGETABLES'
    
    data.loc[(data['CATEGORY']=='Flower') , ['DEPARTMENT']] = 'MISCELLANEOUS'
    
    data.loc[(data['CATEGORY']=='Home&Leisure') |
            (data['CATEGORY']=='Household') |
            (data['CATEGORY']=='Toiletries') 
            , ['DEPARTMENT']] = 'HOUSEHOLD'
    
    data.loc[(data['CATEGORY']=='Petcare') , ['DEPARTMENT']] = 'PET CARE'
    
    data.loc[(data['CATEGORY']=='Seafood') , ['DEPARTMENT']] = 'SEAFOOD'
    
    data.loc[(data['CATEGORY']=='Dairy') , ['DEPARTMENT']] = 'DAIRY AND EGGS'
    
    data.loc[(data['CATEGORY']=='Beverages'), ['DEPARTMENT']] = 'BEVERAGES'
    
    data.loc[(data['CATEGORY']=='Frozen'), ['DEPARTMENT']] = 'FROZEN'
    
    return data

def clean_data_spinney(df):

    df=df.dropna(subset='PRODUCTNAME').reset_index(drop=True)
    
    df['PRODUCTNAME']=df['PRODUCTNAME'].replace('\n',' ', regex=True)

    df['PRODUCTPRICE']=df['PRODUCTPRICE'].replace('\n',' ', regex=True)

    df['PRODUCTNAME'] = df['PRODUCTNAME'].apply(lambda x:(re.sub(r"\,","-",x)))

    df['PRODUCTNAME'] = df['PRODUCTNAME'].apply(lambda x:num2words1_spinney(str(x)))

    df['PRODUCTNAME'] = df['PRODUCTNAME'].apply(lambda x:str(x).rstrip())

    df['PRODUCTNAME']=df['PRODUCTNAME'].apply(lambda x:str(x).replace(u'\xa0',u' '))

    df['PRODUCTNAME']=df['PRODUCTNAME'].apply(lambda x:str(x).replace("\'",''))

    df['PRODUCTNAME1'] = df['PRODUCTNAME'].copy()

    #Clean the Product Price
    df['PRODUCTPRICE']=df['PRODUCTPRICE'].apply(lambda x:(re.findall(r'\d*\.\d*',str(x))[0]))

    #Extract Dimension from Product Name to avoid confusion in Quantity
    df['DIMENSION'] = df['PRODUCTNAME1'].apply(lambda x:product_spec_extractor_dim_spinney(s=x))

    print(len(df['DIMENSION']),len(df['PRODUCTNAME1']))

    df['PRODUCTNAME1'] = df.apply(lambda x: x['PRODUCTNAME1'].replace(x['DIMENSION'], ''), axis=1)

    df['QUANTITY'] = df['PRODUCTNAME1'].apply(lambda x:product_spec_extractor_quant_spinney(s=x))

    #df['PRODUCTNAME2'] = df.apply(lambda x: x['PRODUCTNAME1'].replace(x['QUANTITY'],''), axis=1)
    df['PRODUCTNAME1'] = df['PRODUCTNAME1'].apply(lambda x:product_remove_quant_spinney(x))

    #Extract Size Information from Product Name
    df['SIZE'] = df['PRODUCTNAME1'].apply(lambda x:product_spec_extractor_size_spinney(s=x))

    df['SIZE_MOD'] = df['SIZE'].apply(lambda x:product_spec_extractor_size_mod_spinney(s=x))

    df['UNITS_OF_MEASUREMENT'] = df['SIZE_MOD'].apply(lambda x:units_of_measurement(x))

    #Extract Quantity Information from Product Name
    #Remove Quantity and Size from product Name - Product Name Modified
    df['PRODUCTNAME_MOD'] = df.apply(lambda x: x['PRODUCTNAME1'].replace(x['SIZE_MOD'],''), axis=1)

    df['SIZE_MOD'] = df['SIZE_MOD'].apply(lambda x:re.sub(r'[a-zA-Z]+','',x))

    df['UNITS_OF_MEASUREMENT_FAILED'] = df['UNITS_OF_MEASUREMENT'].copy()

    df['UNITS_OF_MEASUREMENT'] = df['UNITS_OF_MEASUREMENT'].apply(lambda x:re.sub('FAILED +|FAILED','',str(x)))

    #Unify x and s to pcs for readability ( done at last to use it for cleaning in the previous stage)
    df['QUANTITY'] = df['QUANTITY'].apply(lambda x:quant_deal_plusmultiple(x))

    # df['QUANTITY'] = df['QUANTITY'].apply(lambda x: ' '.join(sorted(x.split())))

    df=add_department_spinney(df)
    
    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:' '.join(x.split()))

    df['PRODUCTNAME'] = df['PRODUCTNAME'].apply(lambda x:str(x).strip())

    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:x.upper())

    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:number_to_words(x))

    df['COMMENTS'] = df['PRODUCTNAME_MOD'].apply(lambda x:check_white_label_spinney(x))

    df['PRODUCTNAME_MOD'] = df['PRODUCTNAME_MOD'].apply(lambda x:str(x).replace("\,",' '))

    df['QUANTITY'] = df['QUANTITY'].replace(np.nan, 'None')

    df['QUANTITY'] = df['QUANTITY'].replace('X', 'None')

    df = df.replace("None",'')

    df = df.replace("0.0",'')

    df[['PRODUCTPRICE']] = df[['PRODUCTPRICE']].replace('',0)

    df = df[['PRODUCTNAME','PRODUCTNAME_MOD','PRODUCTPRICE','SIZE','SIZE_MOD','UNITS_OF_MEASUREMENT','UNITS_OF_MEASUREMENT_FAILED',
             'QUANTITY','DIMENSION','CATEGORY','DEPARTMENT','COMMENTS']]

    return df
