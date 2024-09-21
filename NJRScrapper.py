import os
import math
import winsound
from itertools import product
from tqdm.auto import trange
import PyPDF2
import shutil
import shelve
import datetime
import traceback
from datetime import datetime
from sqlalchemy import create_engine
import logging
import requests
import re
import time
import pandas as pd
from bs4 import BeautifulSoup
from twilio.rest import Client


class Scraper:

    state_dict = ''  # Delete
    current_data = ''
    no_of_runs = 1
    event_log = {0: {'Run Type': 'NJR10k', 'Latest Available Data': 'June 2023', 'Run Time': 'N.A',
                     'Run Date': '2023-08-20', 'Days Between Update': 0}}

    def __init__(self, session):
        # Change the directory to store the temporary Selenium files to be processed
        os.chdir('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
        self.session = session
        self.engine = Scraper.connect2postgresql()
        self.run_number, self.last_ran_month, self.last_ran_year = self.latest_event_data()
        self.current_month, self.current_year = self.latest_nj_data()
        self.__counties = []
        self.__towns = []
        self.njrdata = {'City': [], 'County': [], 'Quarter': [], 'Month': [], 'Year': [], 'New Listings': [],
                        'New Listing % Change (YoY)': [], 'Closed Sales': [], 'Closed Sale % Change (YoY)': [],
                        'Days on Markets': [], 'Days on Market % Change (YoY)': [], 'Median Sales Prices': [],
                        'Median Sales Price % Change (YoY)': [], 'Percent of Listing Price Received': [],
                        'Percent of Listing Price Receive % Change (YoY)': [], 'Inventory of Homes for Sales': [],
                        'Inventory of Homes for Sale % Change (YoY)': [], 'Months of Supply': [],
                        'Months of Supplies % Change (YoY)': []}

    """ 
    ______________________________________________________________________________________________________________
                                    Use this section to house the decorator functions
    ______________________________________________________________________________________________________________
    """

    @staticmethod
    def logger_decorator(original_function):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(original_function.__name__)
            logger.setLevel(logging.DEBUG)
            logger.propagate = False
            # Create the FileHandler() and StreamHandler() loggers
            f_handler = logging.FileHandler(
                original_function.__name__ + ' ' + str(datetime.datetime.today().date()) + '.log')
            f_handler.setLevel(logging.DEBUG)
            c_handler = logging.StreamHandler()
            c_handler.setLevel(logging.INFO)
            # Create formatting for the loggers
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                          datefmt='%d-%b-%y %H:%M:%S')
            # Set the formatter for each handler
            f_handler.setFormatter(formatter)
            c_handler.setFormatter(formatter)
            logger.addHandler(f_handler)
            logger.addHandler(c_handler)

            kwargs['logger'] = logger
            kwargs['f_handler'] = f_handler
            kwargs['c_handler'] = c_handler

            result = original_function(*args, **kwargs)

            if result is None:
                pass
            else:
                return result
        return wrapper

    """ 
    ______________________________________________________________________________________________________________
                            Use this section to house the instance, class and static functions
    ______________________________________________________________________________________________________________
    """

    # Function which scrapes the cities and counties from the njrealtor 10k state page
    def area_results(self, soup):
        """
        Function which accepts a BeautifulSoup object to then parse and find the cities
        and counties located in New Jersey
        :param soup: BeautifulSoup object
        :return: None
        """
        area = soup.find('select', id="lmuArea").children
        for obj1 in area:
            municipality = obj1.get_text()
            if municipality in ['Select an area...', 'Entire State', '\n']:
                continue
            else:
                if 'County' in municipality:
                    # There are multiple towns in different counties with the same name. Their county is attached
                    # and need to be separated from the target values
                    if '/' in municipality:
                        # Do not split the city name if it has '/' in it. It signifies that there is more than 1 city
                        # with that name and shows the county it belongs to
                        # newobj = newobj.split('/')
                        self.__towns.append(municipality)
                    else:
                        self.__counties.append(municipality)
                else:
                    self.__towns.append(municipality)

    @staticmethod
    def clean_db(df):
        """

        :param df:
        :return:
        """

        convert_dict = {'Year': str}
        df = df.astype(convert_dict)
        df = df.assign(Dates=df['Month'] + df['Year'])
        df['Dates'] = pd.to_datetime(df['Dates'].tolist(), format="%B%Y", errors='ignore')
        df.insert(1, 'Dates', df.pop('Dates'))
        df['Percent of Listing Price Received'] = df['Percent of Listing Price Received'] / 100
        df = df[df['County'] != 'N.A']

        return df

    @staticmethod
    def connect2postgresql():
        """

        :return:
        """

        # Do I create a function which retrieve my info from UniversalFunction.get_us_pw?
        '''
        database: the name of the database that you want to connect.
        user: the username used to authenticate.
        password: password used to authenticate.
        host: database server address e.g., localhost or an IP address.
        port: the port number that defaults to 5433 if it is not provided.
        '''

        username, base_url, pw = Scraper.get_us_pw('PostgreSQL')

        return create_engine(f"postgresql+psycopg2://{username}:{pw}@{base_url}:5432/nj_realtor_data")

    def create_timeframe(self, outcome, **kwargs):

        timeframe = {}
        months = {
            'January': '01', 'February': '02',
            'March': '03', 'April': '04',
            'May': '05', 'June': '06',
            'July': '07', 'August': '08',
            'September': '09', 'October': '10',
            'November': '11', 'December': '12'
            }

    # if outcome is not None:
        # target_year = outcome['year']
    # else:
        # target_year = self.last_ran_year
        for y in [year for year in range(self.last_ran_year, self.current_year + 1)]:
            if y == self.last_ran_year:
                # May have to play with the month numbers
                month_start = list(months.keys()).index(self.last_ran_month) + 1

                timeframe[y] = list(months.keys())[month_start:]
            # elif y == outcome['year']:
                # month_start = list(months.keys()).index(self.last_ran_month)
                # timeframe[y] = list(months.keys())[month_start:]
            else:
                timeframe[y] = list(months.keys())

        # Log the timeframe before returning it
        return timeframe

    @staticmethod
    def create_url_and_pdfname(base_url, year_var, month_var, town_var):
        """

        :param base_url:
        :param year_var:
        :param month_var:
        :param town_var:
        :return:
        """

        city_list = town_var.split(' ')
        merged_city_name = ''.join(city_list)

        if '/' not in city_list:

            new_url = base_url + year_var + '-' + month_var + '/x/' + merged_city_name
            new_filename = " ".join([' '.join(city_list), Scraper.month2num(month_var), year_var]) + ".pdf"

            return new_url, new_filename

        elif '/' in city_list:
            merged_city_name = '%2F'.join(merged_city_name.split('/'))
            del city_list[city_list.index('/')]
            new_url = base_url + year_var + '-' + month_var + '/x/' + merged_city_name
            new_filename = " ".join([' '.join(city_list), Scraper.month2num(month_var), year_var]) + ".pdf"

            return new_url, new_filename

    # Function which scrapes the current month of data available
    @staticmethod
    def current_data_avail(soup):
        """
        Function which accepts a BeautifulSoup object and scrapes the most recent data available to download
        and assigns the value to the current_data class variable
        :param soup: BeautifulSoup object
        :return: None
        """
        current_results = soup.find('select', id="lmuTime").children
        current_results = list(current_results)
        target = current_results[2].get_text().split(' ')

        return target[0], target[1]

    # Creates the initial dictionary the scraped city data will be stored
    def data_na(self, town, month, year):
        """
        UPDATE AND DELETE THE FY PORTION

        Staticmethod which assigns default values of 0, 0.0 and N/A to variables used in the extraction function
        for real estate pdfs which were found to have corrupted data
        :param town: str variable of the name of the town
        :param month: str variable of the month of the target data
        :param year: str variable of the year of the target data

        :return: None
        """

        category_list = ['New Listings', 'Closed Sales', 'Days on Markets', 'Median Sales Prices',
                         'Percent of Listing Price Received',
                         'Inventory of Homes for Sales', 'Months of Supply']

        self.njrdata['Month'].append(month)
        self.njrdata['Quarter'].append(Scraper.find_quarter(month))
        self.njrdata['Year'].append(year)
        self.njrdata['City'].append(town)
        self.njrdata['County'].append('N.A')
        self.njrdata['Month'].append(month)

        for category in category_list:
            self.njrdata[category].append(0)
            self.njrdata[f'{category} % Change (YoY)'].append(0.0)

    # Function which calculates the difference between the current download date and previous date
    # Use this to calculate the average amount of time it takes between new update periods
    @classmethod
    def daysuntilupdate(cls):
        """
        Classmethod which returns a timedelta object that depicts the amount of days between
        the program's last run and current update
        :return: delta (timedelta object)
        """
        current = cls.no_of_runs
        previous = current - 1
        current_date = datetime.datetime.now()
        previous_date = datetime.datetime.strptime(cls.event_log[previous]['Run Date'], "%a %b %d %H:%M:%S %Y")
        delta = current_date - previous_date

        return delta.days

    def download_pdf(self, pdf_url, pdf_name, params_dict, logger):
        """

        :param pdf_url:
        :param params_dict:
        :param pdf_name:
        :param logger:
        :return:
        """

        with self.session.get(pdf_url, params=params_dict, stream=True) as reader, open(pdf_name, 'wb') as writer:
            for chunk in reader.iter_content(chunk_size=1000000):
                # Casting the bytes into a str type
                # and slicing the first 20 characters to check if 'PDF' is in
                check_pdf = str(chunk)[:20]
                # print(check_pdf)
                if 'PDF' in check_pdf:
                    writer.write(chunk)
                else:
                    logger.warning(f'WARNING! {pdf_name} is possibly a corrupted file')

    # This is an instance method because I'm using a static method inside the function which may not be able
    def event_log_update(self, log_data, logger):
        """

        Instance method which updates the event log with runtime data of the most recent NJR10k download.
        Stores the type of downlaod/update which was run, the length of the download runtime, current date and
        length in time between the previous and current program runs
        :param log_data:
        :param logger: logger function which will return event log to ther logger file
        :return: None
        """

        table_name = 'event_log'
        db = pd.DataFrame(log_data)

        if pd.read_sql_table(table_name, self.engine):
            db.to_sql(table_name, self.engine, if_exists='append', chunksize=1000, index=False)

        logger.info(f'New event log has been saved to the {table_name} in PostgreSQL')


    # Function which extracts the month, current and previous year, new listing, closing sales, DOM, median sales, etc
    # Data will then be stored in a dictionary
    def extract_re_data(self, pdfname, update=None, **kwargs):
        """
        UPDATE

        Function which reads the pdfname name arg and extracts the real estate data from that pdf and stores
        it in the global main_dictionary variable
        :param pdfname: Name of the target pdf
        :param update: Allows for dynamic directory changing if argument is equal to 'Yes'
        :param kwargs: Keyword argument dictionary which houses the logger function variables
        :return: None
        """

        logger = kwargs['logger']

        # For municipalities with the same name and located in multiple counties,
        # the county name will be unpacked in the town var and needs to be extracted to use as an additional
        # redundancy check
        town, county, month, year = Scraper.parse_pdfname(pdfname)

        if update == 'Yes':
            temp_town = town
            if county is not None:
                temp_town = " ".join([town, county])
            os.chdir(f'C:\\Users\\Omar\\Desktop\\Python Temp Folder\\PDF Temp Files\\{year}\\{temp_town}')
        else:
            os.chdir('C:\\Users\\Omar\\Desktop\\Python Temp Folder')

        try:
            with open(pdfname, 'rb') as reader:
                pdfread = PyPDF2.PdfReader(reader)
                page = pdfread.pages[0]
                target = page.extract_text()
                lines = target.split('\n')
                data = '\n'.join(lines[:24])
                info = '\n'.join(lines[24:])

            if county is None:
                assert town in info

            elif county is not None:
                assert town and county in info

            self.good_data(town, data, info)
            logger.info(f'The data for {pdfname} has been extracted')

        except PyPDF2._reader.EmptyFileError as efe:
            logger.exception(f'An Error Has Occured (File Possibly Corrupted):\n{traceback.format_exception(efe)}')
            logger.info(f'The city of {town} for {month} {year} does not have data')
            # If function encounters an empty/corrupted pdf,
            # the data_na function will render all information available for that file equal to zero
            # The generator will then pick up at the next pdf to continue extracting data
            self.data_na(town, month, year)

        except AssertionError as AE:
            logger.exception(f'An AssertionError Has Occurred:\n{traceback.format_exception(AE)}')
            logger.info(f'PDF corrupted. The city of {town} for {month} {year} does not have data')
            self.data_na(town, month, year)

    @logger_decorator
    def fill_missing_data(self, target_directories: list, main_dict, year_dict, **kwargs):
        """
        UPDATE

        :param target_directories:
        :param main_dict:
        :param year_dict:
        :param kwargs:
        :return:
        """

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        name = "Fill Missing Data"
        start_time = datetime.datetime.now()

        for pdf in Scraper.pdf_generator(pdfname=target_directories):
            self.extract_re_data(pdf, ['No Corrupted Files'], main_dict,year_dict, update='Yes')

        end_time = datetime.datetime.now()
        run_time = end_time - start_time

        winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)

        logger.removeHandler(f_handler)
        logger.removeHandler(c_handler)
        logging.shutdown()

    @staticmethod
    def find_closed_sales(pdf_text):
        """
        :param pdf_text:
        :return:
        """

        variable_list = []

        closed_sales_pattern = re.compile(
            r'Closed\sSales\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)')
        closed_sales_search = list(closed_sales_pattern.findall(pdf_text))
        closed_sales_current = int(closed_sales_search[0][1])
        closed_sales_pc = closed_sales_search[0][2].split(' ')
        closed_sales_per_change = ''.join(closed_sales_pc).rstrip('%')
        if '+' in closed_sales_per_change:
            closed_sales_per_change.lstrip('+')
            closed_sales_per_change = round(float(closed_sales_per_change) / 100, 3)
        elif '--' in closed_sales_per_change:
            closed_sales_per_change = 0.0
        else:
            closed_sales_per_change = round(float(closed_sales_per_change) / 100, 3)

        variable_list.extend([closed_sales_current, closed_sales_per_change])

        return variable_list

    @staticmethod
    def find_county(pdftext):
        """

        :return: county name
        """

        county_pattern = re.compile(r'(Atlantic|Bergen|Burlington|Camden|Cape\sMay|Cumberland|Essex|'
                                    r'Gloucester|Hudson|Hunterdon|Mercer|Middlesex|Monmouth|Morris|Ocean|'
                                    r'Passaic|Salem|Somerset|Sussex)\sCounty')

        found_county = county_pattern.search(pdftext)

        return found_county

    @staticmethod
    def find_dom(pdf_text):
        """
        :param pdf_text:
        :return:
        """

        variable_list = []

        dom_pattern = re.compile(
            r'Days\son\sMarket\sUntil\sSale\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)')
        dom_search = list(dom_pattern.findall(pdf_text))
        dom_current = int(dom_search[0][1])
        dom_pc = dom_search[0][2].split(' ')
        dom_per_change = ''.join(dom_pc).rstrip('%')
        if '+' in dom_per_change:
            dom_per_change.lstrip('+')
            dom_per_change = round(float(dom_per_change) / 100, 3)
        elif '--' in dom_per_change:
            dom_per_change = 0.0
        else:
            dom_per_change = round(float(dom_per_change) / 100, 3)

        variable_list.extend([dom_current, dom_per_change])

        return variable_list

    @staticmethod
    def find_inventory(pdf_text):
        """
        :param pdf_text:
        :return:
        """

        variable_list = []

        inventory_pattern = re.compile(
            r'Inventory\sof\sHomes\sfor\sSale\s(--|\d{0,3}?)\s(--|\d{0,3}?)\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)\s(--|\d{0,3}?)\s(--|\d{0,3}?)\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
        inventory_search = list(inventory_pattern.findall(pdf_text))
        inventory_current = inventory_search[0][1]
        if inventory_current != '--':
            inventory_current = int(inventory_current)
        inventory_pc = inventory_search[0][2].split(' ')
        inventory_per_change = ''.join(inventory_pc).rstrip('%')
        if '+' in inventory_per_change:
            inventory_per_change.lstrip('+')
            inventory_per_change = round(float(inventory_per_change) / 100, 3)
        elif '--' in inventory_per_change:
            inventory_per_change = 0.0
        else:
            inventory_per_change = round(float(inventory_per_change) / 100, 3)

        variable_list.extend([inventory_current, inventory_per_change])

        return variable_list

    @staticmethod
    def find_key_metrics(pdf_text):
        """

        :param pdf_text:
        :return:
        """

        key_metrics_basic_pattern = re.compile(
            r'Key\sMetrics\s(\d{4})\s(\d{4})\sPercent\sChange\sThru\s\d{1,2}?-\d{4}\sThru\s\d{1,2}?-\d{4}\sPercent\sChange')
        km_search = list(key_metrics_basic_pattern.findall(pdf_text))

        return km_search[0][1]

    @staticmethod
    def find_median_sales(pdf_text):
        """
        :param pdf_text:
        :return:
        """

        variable_list = []

        median_sales_pattern = re.compile(
            r'Median\sSales\sPrice\*\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
        median_sales_search = list(median_sales_pattern.findall(pdf_text))
        median_sales_current = median_sales_search[0][1]
        median_sales_current = int("".join(median_sales_current.split(',')).lstrip('$'))
        median_sales_pc = median_sales_search[0][2].split(' ')
        median_sales_per_change = ''.join(median_sales_pc).rstrip('%')
        if '+' in median_sales_per_change:
            median_sales_per_change.lstrip('+')
            median_sales_per_change = round(float(median_sales_per_change) / 100, 3)
        elif '--' in median_sales_per_change:
            median_sales_per_change = 0.0
        else:
            median_sales_per_change = round(float(median_sales_per_change) / 100, 3)

        variable_list.extend([median_sales_current, median_sales_per_change])

        return variable_list

    @staticmethod
    def find_month(pdf_text):
        """

        :param pdf_text:
        :return:
        """

        month_pattern = re.compile(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\sYear\sto\sDate\sSingle\sFamily')

        return month_pattern.search(pdf_text).group(1)

    @staticmethod
    def find_new_listings(pdf_text):
        """
        :param pdf_text:
        :return:
        """

        variable_list = []

        new_listings_pattern = re.compile(
            r'New\sListings\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)')
        new_listing_search = list(new_listings_pattern.findall(pdf_text))
        new_listings_current = int(new_listing_search[0][1])
        new_listings_pc = str(new_listing_search[0][2]).split(' ')
        new_listings_per_change = ''.join(new_listings_pc).rstrip('%')
        if '+' in new_listings_per_change:
            new_listings_per_change.lstrip('+')
            new_listings_per_change = round(float(new_listings_per_change) / 100, 3)
        elif '--' in new_listings_per_change:
            new_listings_per_change = 0.0
        else:
            new_listings_per_change = round(float(new_listings_per_change) / 100, 3)

        variable_list.extend([new_listings_current, new_listings_per_change])

        return variable_list

    @staticmethod
    def find_percent_lpr(pdf_text):
        """
        :param pdf_text:
        :return:
        """

        variable_list = []

        percent_lpr_pattern = re.compile(
            r'Percent\sof\sList\sPrice\sReceived\*\s(\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
        percent_lpr_search = list(percent_lpr_pattern.findall(pdf_text))
        # Divide this by 100 and figure out how to format these to show the percent sign
        percent_lpr_current = float(percent_lpr_search[0][1].rstrip('%'))
        percent_lpr_pc = percent_lpr_search[0][2].split(' ')
        percent_lpr_per_change = ''.join(percent_lpr_pc).rstrip('%')
        if '+' in percent_lpr_per_change:
            percent_lpr_per_change.lstrip('+')
            percent_lpr_per_change = round(float(percent_lpr_per_change) / 100, 3)
        elif '--' in percent_lpr_per_change:
            percent_lpr_per_change = 0.0
        else:
            percent_lpr_per_change = round(float(percent_lpr_per_change) / 100, 3)

        variable_list.extend([percent_lpr_current, percent_lpr_per_change])

        return variable_list

    @staticmethod
    def find_supply(pdf_text):
        """
        :param pdf_text:
        :return:
        """

        variable_list = []

        supply_pattern = re.compile(
            r'Months\sSupply\sof\sInventory\s(--|\d{1,2}?.\d{1})\s(--|\d{1,2}?.\d{1})\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)\s(--|\d{1,2}?.\d{1})\s(--|\d{1,2}?.\d{1})\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
        supply_search = list(supply_pattern.findall(pdf_text))
        supply_current = supply_search[0][1]
        if supply_current != '--':
            supply_current = float(supply_current)
        supply_pc = supply_search[0][2].split(' ')
        supply_per_change = ''.join(supply_pc).rstrip('%')
        if '+' in supply_per_change:
            supply_per_change.lstrip('+')
            supply_per_change = round(float(supply_per_change) / 100, 3)
        elif '--' in supply_per_change:
            supply_per_change = 0.0
        else:
            supply_per_change = round(float(supply_per_change) / 100, 3)

        variable_list.extend([supply_current, supply_per_change])

        return variable_list

    @staticmethod
    def find_quarter(month):
        """

        :param month:
        :return:
        """
        if month in ['January', 'February', 'March']:
            return 'Q1'
        elif month in ['April', 'May', 'June']:
            return 'Q2'
        elif month in ['July', 'August', 'September']:
            return 'Q3'
        elif month in ['October', 'November', 'December']:
            return 'Q4'

    # Function which pulls the username and password for a specified website
    @staticmethod
    def get_us_pw(website):
        """
        Staticmethod which fetches the username and passord for the arg "website"
        :param website: Name of the website for which the username and password are needed
        :return: username, pw
        """
        # Saves the current directory in a variable in order to switch back to it once the program ends
        previous_wd = os.getcwd()
        os.chdir('F:\\Jibreel Hameed\\Kryptonite')

        db = pd.read_excel('get_us_pw.xlsx', index_col=0)
        username = db.loc[website, 'Username']
        pw = db.loc[website, 'Password']
        base_url = db.loc[website, 'Base URL']

        os.chdir(previous_wd)

        return username, base_url, pw

    def good_data(self, city, data, information):
        """

        :param data:
        :param city:
        :param information:
        :return:
        """

        category_list = ['New Listings', 'Closed Sales', 'Days on Markets', 'Median Sales Prices',
                         'Percent of Listing Price Received',
                         'Inventory of Homes for Sales', 'Months of Supply']

        function_list = [Scraper.find_new_listings, Scraper.find_closed_sales, Scraper.find_dom,
                         Scraper.find_median_sales, Scraper.find_percent_lpr, Scraper.find_inventory,
                         Scraper.find_supply]

        month = Scraper.find_month(information)
        self.njrdata['Month'].append(month)
        self.njrdata['Quarter'].append(Scraper.find_quarter(month))
        self.njrdata['Year'].append(Scraper.find_key_metrics(information))
        self.njrdata['City'].append(city)
        self.njrdata['County'].append(Scraper.find_county(information))

        for category, function in zip(category_list, function_list):
            scraped_data = function(data)
            self.njrdata[category].append(scraped_data[0])
            self.njrdata[f'{category} % Change (YoY)'].append(scraped_data[1])

    # Used in case the njr10k or the update_njr10k functions are used recursively.
    # This function will find the latest file downloaded and continue from that point
    def last_downloaded_pdf(self):
        """
        THIS NEEDS TO BE REFACTORED
        :return:
        """

        base_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder'

        for root, dirs, filenames in os.walk(base_path):
            # print(filenames)
            # If the length of the filenames variable is less than 2,
            # that means there are no PDF Files in the folder. The 2 existing files are log files
            if len(filenames) <= 2:
                return 'None'
            else:
                try:
                    i = -1
                    while not filenames[i].endswith('.pdf'):
                        # Search the filenames list in reverse order to look for the latest PDF file
                        i -= 1
                    else:
                        target = filenames[i]
                        # Files are stored in alphabetical order
                        # Wyckoff Twp are the last pdfs to be downloaded
                        # If the target pdf name is either equal to Wyckoff Twp Sept for this year or last year
                        # or Wyckoff Twp of the current month and year
                        # All files may be downloaded, need to do a second check
                        check1 = 'Wyckoff Twp September ' + str(int(Scraper.current_data.split(' ')[1]) - 1) + '.pdf'
                        check2 = 'Wyckoff Twp September ' + Scraper.current_data.split(' ')[1] + '.pdf'
                        check3 = 'Wyckoff Twp ' + Scraper.current_data + '.pdf'
                        if target == check1 or target == check2 or target == check3:
                            print(f'Latest file downloaded is: {target}\nAll files may be downloaded...')
                            time.sleep(1)
                            print(f'Moving 2nd phase check...')
                            time.sleep(1)
                        else:
                            print(f'Latest file downloaded is: {target}\nRestarting download process...')
                            info = target.rstrip('.pdf').split(' ')
                            town = info[0:len(info) - 2]
                            if len(town) > 1:
                                town = ' '.join(town)
                            else:
                                town = info[0]

                except IndexError:
                    # The code tried searching outside the list because it didn't find a PDF file
                    print(f'There are currently no downloaded PDFs available. Starting NJR10k download...')
                    return 'None'
                else:
                    check4 = 'Wyckoff Twp ' + Scraper.current_data + '.pdf'
                    if os.path.exists(base_path + '\\' + check4):
                        print('All files have been downloaded. Now reading previous logger file...')
                        return 'Read Logger File'
                    else:
                        full_town_list = self.__towns
                        return full_town_list[full_town_list.index(town):]

    @classmethod
    def latest_event_data(cls):
        """
        - Accept the self.db_engine as a var since the classmethod cant directly access it
        - latest_event = pd.read_sql_table('event_log', engine=db_engine).loc[-1]
        - last_run_num = latest_event['Run Num']
        - last_date = latest_event['Latest Available Data'].split(' ')
        - Also have a try-except block that says if there's no data:
        return 1, 09, 2019
        :return:
        """
        return None, None, None

    def latest_nj_data(self):
        """

        :return:
        """

        url = 'https://www.njrealtor.com/login.php?rd=10&passedURL=/goto.php?10kresearch=1&skipToken=1'
        response = self.session.get(url)

        if response.status_code == 200:
            page_source = response.text
            soup = BeautifulSoup(page_source, 'html.parser')
            self.area_results(soup)

            return self.current_data_avail(soup)

        else:
            response.raise_for_status()

    @staticmethod
    def month2num(month):
        # Return the name of the month if digits are given or digits if the name is given
        month_dict = {
            'January': '01', 'February': '02',
            'March': '03', 'April': '04',
            'May': '05', 'June': '06',
            'July': '07', 'August': '08',
            'September': '09', 'October': '10',
            'November': '11', 'December': '12'
        }
        if month.isalpha():
            return month_dict[month]
        elif month.isdigit():

            for name, value in month_dict.items():
                if value == month:
                    return name

    # Function which logs into njrealtor to automatically download the pdfs from each city to get ready to scrape
    @logger_decorator
    def njr10k(self, timeframe, **kwargs):
        """
        UPDATE

        :param timeframe:
        :param kwargs:
        :return:
        """

        logger = kwargs['logger']
        start_time = datetime.datetime.now()

        name = 'NJR10k'

        base_url = 'http://njar.stats.10kresearch.com/docs/lmu/'
        params = {'src': 'Page'}

        for key, value in timeframe.keys():
            target_list = list(product(key, value, self.__towns))
            for _, data in zip(trange(len(target_list), desc='Downloaded PDFs'), target_list):
                pdf_url, new_filename = Scraper.create_url_and_pdfname(base_url, data[0], data[1], data[2])
                self.download_pdf(pdf_url, new_filename, params, logger)

        end_time = datetime.datetime.now()
        run_time = end_time - start_time

        winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)

        return name, run_time

    def pandas2sql(self, logger):
        """
        UPDATE

        :param logger:
        :return:
        """

        table_name = 'nj_realtor_data'
        db = pd.DataFrame(self.njrdata)
        db = db.apply(Scraper.clean_db)
        db.drop_duplicates(subset=['City', 'Month', 'Year'], keep='first', inplace=True, ignore_index=True)

        if pd.read_sql_table(table_name, self.engine):
            db.to_sql(table_name, self.engine, if_exists='append', chunksize=1000, index=False)

        logger.info(f'New NJ Realtor data has been saved to the {table_name} in PostgreSQL')

    @staticmethod
    def parse_pdfname(pdf_name):
        """

        :param pdf_name:
        :return:
        """

        info = pdf_name.rstrip('.pdf').split(' ')
        town_directory = info[0:len(info) - 2]

        if len(town_directory) > 2:
            if 'County' in town_directory:
                # This means the city name is a duplicate and needs to have the county distinguished
                county = ' '.join(town_directory[-2:])
                town = ' '.join(town_directory[0:(town_directory.index('County') - 1)])
            else:
                town = ' '.join(town_directory)
                county = None
        else:
            town = ' '.join(town_directory)
            county = None

        month = info[-2]
        year = info[-1]

        return town, county, month, year

    # Generator function which will be used in tandem with the extract_re_data function to put data into main dictionary
    @staticmethod
    def pdf_generator(pdfname=None, year=None):
        """
        UPDATE

        :param pdfname:
        :param year
        :return:
        """

        base_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder'

        if pdfname is None:
            return os.listdir(base_path)

        elif type(pdfname) is list:
            filenames = []

            if year is None:
                year = 2019

            for year_var, municipality in zip([y for y in range(year, datetime.now().year + 1)], pdfname):
                search_directory = f'C:\\Users\\Omar\\Desktop\\Python Temp Folder' \
                                   f'\\PDF Temp Files\\{year_var}\\{municipality}'
                try:
                    missing_files = os.listdir(search_directory)
                    filenames.extend(missing_files)
                except FileNotFoundError:
                    # More than likely there are no files downloaded for that year
                    continue

            return filenames

        elif pdfname.endswith('.pdf'):
            filenames = os.listdir(base_path)

            return filenames[filenames.index(pdfname) + 1:]

    def process_checkpoint(self, logger):

        previous_wd = os.getcwd()
        os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')

        with shelve.open('NJ Scrapper Data Dictionary_v2', writeback=True) as saved_data_file:
            saved_data_file['Main Dictionary'] = self.njrdata
            logger.info('Saving the data for Main Dictionary, Full Year and Event Log...')
        os.chdir(previous_wd)

    # A function I can use for debugging purposes.
    # In the event a failure occurs before reaching the extract_re_data function
    # I can read the logger file and start again as a midway point
    def read_logger(self, logger):
        """
        UPDATE

        :param logger:
        :return:
        """

        base_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder'
        corrupt_pattern = re.compile(r'\d+-\w+-\d+\s\d+:\d+:\d+\s-\supdate_njr10k|njr10k\s-\sWARNING\s-\sWARNING!\s(.*.pdf)\sis\spossibly\sa\scorrupted\sfile')
        run_name_pattern = re.compile(r'Run Type:\s(\w+\s\w+)')
        run_time_pattern = re.compile(r"Run Time:\s(.*)")

        possible_corrupted_files = []

        for root, dirs, filenames in os.walk(base_path):

            if len(filenames) < 1:
                return 'None'
            elif len(filenames) > 1:
                i = -1
                # Will find all log files
                while not filenames[i].endswith('.log'):
                    i -= 1
                else:
                    file_pattern = re.compile(r'update_njr10k|njr10k\s\d{4}-\d{2}-\d{2}.log')
                    while not file_pattern.search(filenames[i]):
                        i -= 1
                    else:
                        target = []
                        while len(target) < 1:
                            file = filenames[i]
                            with open('C:\\Users\\Omar\\Desktop\\Python Temp Folder\\' + file, 'r') as reader:
                                target = reader.readlines()
                                i -= 1
                        else:
                            break

        for j in target:
            if corrupt_pattern.search(j):
                if corrupt_pattern.search(j).group(1) is not None:
                    possible_corrupted_files.append(corrupt_pattern.search(j).group(1))
            elif run_name_pattern.search(j):
                name = run_name_pattern.search(j).group(1)
            elif run_time_pattern.search(j):
                run_time = run_time_pattern.search(j).group(1)
            else:
                continue

        return possible_corrupted_files

    @staticmethod
    def text_message(message_body):
        """

        :param message_body:
        :return:
        """
        # Your Account SID from twilio.com/console
        account_sid = 'AC91ccb829e7e47ff05e69d8f96d627f73'
        # Your Auth Token from twilio.com/console
        auth_token = "f686fc30c3c9bc2694f11bb5137bb28c"

        twilioclient = Client(account_sid, auth_token)

        message = twilioclient.messages.create(
            to="+19084683728",
            from_="+19088609446",
            body=message_body)

        print(message.body)

    @staticmethod
    def waiting(sleep_time):
        """

        :param sleep_time:
        :return:
        """
        sleep_time2 = str(sleep_time.days)
        sleep_time3 = int(sleep_time2) * 86400  # 86,400 seconds in a day
        if sleep_time3 > 86400:
            message_body = f'There is currently no new data available. NJRScrapper will check again in {sleep_time.days} days...'
            Scraper.text_message(message_body)
            time.sleep(sleep_time3)
        else:
            message_body = f"There is currently no new data available. Will check again tomorrow..."
            Scraper.text_message(message_body)
            time.sleep(86400)

    def main(self, **kwargs):

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        event_log = {'Run Type': '', 'Latest Available Data': '',
                     'Run Time': '', 'Run Date': '', 'Days Between Update': ''}

        event_log['Run Date'] = time.ctime()
        event_log['Days Between Update'] = Scraper.daysuntilupdate()
        # Step 1a: Check if new data is available
        assert self.current_month != self.last_ran_month

        # Step 1b: Download pdfs
        results = self.last_downloaded_pdf()
        timeframe = self.create_timeframe(results, **kwargs)
        event_log['Run Type'], event_log['Run Time'] = self.njr10k(timeframe)

        # Step 2: Scrape information from pdfs
        pdf_list = Scraper.pdf_generator()
        for _, pdf in zip(trange(len(pdf_list), desc='Extracted PDFs'), pdf_list):
            self.extract_re_data(pdf, **kwargs)

        winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)
        # Step 3: Save the downloaded data. Creates a checkpoint in the event of script failure
        self.process_checkpoint(logger)
        # Step 4: Remove duplicate data (if any) from df and save to sql
        self.pandas2sql(logger)
        # Step 5: Save event_log and data to respective sql dfs
        self.event_log_update(event_log, logger)

        logger.removeHandler(f_handler)
        logger.removeHandler(c_handler)
        logging.shutdown()


if __name__ == '__main__':

    """This is the first part of the NJ Realtor 10k Scrapper. 
    This section of the program will systematically check for the 
    most recent files uploaded to their database and download them 
    for processing and analysis in the second half of the program.
    """

    try:

        url = 'https://www.njrealtor.com/login/?rd=10&passedURL=/goto/10k/'
        url2 = 'https://www.njrealtor.com/ramco-api/web-services/login_POST.php'

        with requests.Session() as s:
            # create a function/module which returns the njr10k info
            username, pw = Scraper.get_us_pw('NJRealtor')

            # payload sent during the HTTP POST
            payload1 = {'rd': '10',
                        'passedURL': '/goto/10k/',
                        'case': '',
                        'LoginEmail': username,
                        'LoginPassword': pw,
                        'LoginButton': 'Login'}

            s.get(url)  # Request to arrive at the log-in page
            s.post(url2, data=payload1)

            obj = Scraper(s)
            obj.main()

    except KeyboardInterrupt:
        print()
        print('Program was manually stopped')
    except AssertionError as ae:
        # Send message that says no new data is available
        # Tell airflow to try again tomorrow
        pass
    else:
        # Send message that says new data as been scraped and stored
        pass






