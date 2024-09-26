import os
import winsound
from itertools import product
from tqdm.auto import trange
import PyPDF2
import shutil
import shelve
import datetime
import traceback
from datetime import datetime, date, timedelta
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
import logging
import requests
import re
import time
import pandas as pd
from bs4 import BeautifulSoup
from twilio.rest import Client


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

    twilioclient.messages.create(
        to="+19084683728",
        from_="+19088609446",
        body=message_body)

    print(f'Program Completed!\n{message_body}')


class Scraper:

    def __init__(self, session, update_list=None):
        # Change the directory to store the temporary Selenium files to be processed
        os.chdir('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
        self.counties = []
        self.towns = []
        self.session = session
        self.engine = Scraper.connect2postgresql()
        self.event_log = {'Number of Runs': [], 'Run Type': [], 'Latest Available Data': [],
                          'Run Time': [], 'Run Date': [], 'Days Between Update': []}
        self.run_number, self.last_ran_month, self.last_ran_year, self.last_run_date = self.latest_event_data()
        self.current_month, self.current_year = self.latest_nj_data()
        self.update_list = update_list
        self.njrdata = {'City': [], 'County': [], 'Quarter': [], 'Month': [], 'Year': [], 'New Listings': [],
                        'New Listings % Change (YoY)': [], 'Closed Sales': [], 'Closed Sales % Change (YoY)': [],
                        'Days on Markets': [], 'Days on Markets % Change (YoY)': [], 'Median Sales Prices': [],
                        'Median Sales Prices % Change (YoY)': [], 'Percent of Listing Price Received': [],
                        'Percent of Listing Price Received % Change (YoY)': [], 'Inventory of Homes for Sales': [],
                        'Inventory of Homes for Sales % Change (YoY)': [], 'Months of Supply': [],
                        'Months of Supply % Change (YoY)': []}

    """ 
    ______________________________________________________________________________________________________________
                                    Use this section to house the decorator functions
    ______________________________________________________________________________________________________________
    """

    @staticmethod
    def logger_decorator(original_function):
        def wrapper(*args, **kwargs):

            logger_dir = 'F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Logs'

            if not os.path.exists(logger_dir):
                os.makedirs(logger_dir)

            logger = logging.getLogger(original_function.__name__)
            filename = original_function.__name__ + ' ' + str(datetime.today().date()) + '.log'
            logger.setLevel(logging.DEBUG)
            logger.propagate = False
            # Create the FileHandler() and StreamHandler() loggers
            f_handler = logging.FileHandler(os.path.join(logger_dir, filename))
            f_handler.setLevel(logging.INFO)
            c_handler = logging.StreamHandler()
            c_handler.setLevel(logging.WARNING)
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

        area_list = Scraper.city_cleaner(soup)

        for municipality in area_list:

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
                        self.towns.append(municipality)
                    else:
                        self.counties.append(municipality)
                else:
                    self.towns.append(municipality)

    @staticmethod
    def city_cleaner(soup):

        municipality_pattern = re.compile(r'AreaList\["lmu"] = \[(.*)];')
        area = soup.find_all('script', {'type': 'text/javascript'})
        area_list = []

        for item in area:
            target_str = item.get_text()
            if 'var AreaList = {};' in target_str:
                municipality_list = municipality_pattern.search(target_str).group(1)

        municipality_list = municipality_list.split(',')

        for city in municipality_list:
            cleaned_city = ''.join([i for i in city if i != '"'])
            area_list.append(cleaned_city)

        return area_list

    @staticmethod
    def clean_db(df):
        """

        :param df:
        :return:
        """

        df = df.assign(Dates=df['Month'] + df['Year'])
        # df['Dates'] = pd.to_datetime(df['Dates'].tolist(), format="%B%Y", errors='ignore')
        df['Dates'] = pd.PeriodIndex(df['Dates'], freq='M').strftime('%Y-%b-%d')
        df = df.astype({'Year': 'string', 'Dates': 'string'})
        df = df.rename(columns={'City': 'Municipality', 'New Listings % Change (YoY)': 'New Listing % Change (YoY)',
                           'Closed Sales % Change (YoY)': 'Closed Sale % Change (YoY)',
                           'Days on Markets % Change (YoY)': 'Days on Market % Change (YoY)',
                           'Median Sales Prices % Change (YoY)': 'Median Sales Price % Change (YoY)',
                           'Percent of Listing Price Received % Change (YoY)': 'Percent of Listing Price Receive % Change (YoY)',
                           'Inventory of Homes for Sales % Change (YoY)': 'Inventory of Homes for Sale % Change (YoY)',
                           'Months of Supply % Change (YoY)': 'Months of Supplies % Change (YoY)'})
        df.insert(0, 'Date', df.pop('Dates'))
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

        user, base_url, psw = Scraper.get_us_pw('PostgreSQL')

        return create_engine(f"postgresql+psycopg2://{user}:{psw}@{base_url}:5432/nj_realtor_data")

    @staticmethod
    def create_date_object(filename):

        metadata = os.stat(filename)
        target = time.strptime(time.ctime(metadata.st_mtime))

        year = target.tm_year
        month = target.tm_mon
        day = target.tm_mday

        return date(year, month, day)

    def create_timeframe(self, outcome, logger):

        timeframe = {}
        months = {
            'January': '01', 'February': '02',
            'March': '03', 'April': '04',
            'May': '05', 'June': '06',
            'July': '07', 'August': '08',
            'September': '09', 'October': '10',
            'November': '11', 'December': '12'
            }

        if outcome.endswith('.pdf'):
            temp_var = outcome.rstrip('.pdf').split(' ')
            municipality = ' '.join(temp_var[:-2])
            start_year = int(temp_var[-1])
            start_month = temp_var[-2]
        else:
            start_year = int(self.last_ran_year)

        for y in [year for year in range(start_year, int(self.current_year) + 1)]:
            if y == int(self.last_ran_year):
                if self.last_ran_month == 'December':
                    continue

                month_start = list(months.keys()).index(self.last_ran_month) + 1

                timeframe[y] = list(months.keys())[month_start:]

            # Only time this block is initiated is if the script failed before completion
            elif outcome.endswith('.pdf'):
                month_start = list(months.keys()).index(start_month)
                timeframe[y] = list(months.keys())[month_start:]

            elif y == int(self.current_year):
                if self.last_ran_month == 'December':
                    # Starts target download months from January
                    month_start = list(months.keys()).index('January')
                else:
                    month_start = list(months.keys()).index(self.last_ran_month) + 1

                month_end = list(months.keys()).index(self.current_month) + 1
                timeframe[y] = list(months.keys())[month_start:month_end]
            else:
                timeframe[y] = list(months.keys())

        logger.info(f'Timeframe of data needed:\n{timeframe}')
        timeframe_years = list(timeframe.keys())

        if outcome.endswith('.pdf'):
            print(f'NJ Realtor Data will be downloaded from {timeframe[timeframe_years[0]][0]} '
                  f'{timeframe_years[0]} to {timeframe[timeframe_years[-1]][-1]} {timeframe_years[-1]} '
                  f'starting from {outcome}')
            return municipality, timeframe

        else:
            print(f'NJ Realtor Data will be downloaded from {timeframe[timeframe_years[0]][0]} '
                  f'{timeframe_years[0]} to {timeframe[timeframe_years[-1]][-1]} {timeframe_years[-1]}')
            return None, timeframe

    @staticmethod
    def create_timestamp(text):

        new_time = text.split(',')

        if len(new_time) == 2:
            t = timedelta(seconds=float(new_time[0].split('=')[1]), microseconds=float(new_time[1].split('=')[1]))
        elif len(new_time) == 3:
            t = timedelta(days=float(new_time[0].split('=')[1]), seconds=float(new_time[1].split('=')[1]),
                          microseconds=float(new_time[2].split('=')[1]))

        return str(t)

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

            new_url = base_url + str(year_var) + '-' + Scraper.month2num(month_var) + '/x/' + merged_city_name
            new_filename = " ".join([' '.join(city_list), month_var, str(year_var)]) + ".pdf"

            return new_url, new_filename

        elif '/' in city_list:
            merged_city_name = '%2F'.join(merged_city_name.split('/'))
            del city_list[city_list.index('/')]
            new_url = base_url + str(year_var) + '-' + Scraper.month2num(month_var) + '/x/' + merged_city_name
            new_filename = " ".join([' '.join(city_list), month_var, str(year_var)]) + ".pdf"

            return new_url, new_filename

    # Function which scrapes the current month of data available
    def current_data_avail(self, soup):
        """
        Function which accepts a BeautifulSoup object and scrapes the most recent data available to download
        and assigns the value to the current_data class variable
        :param soup: BeautifulSoup object
        :return: None
        """
        current_results = soup.find('select', id="lmuTime").children
        current_results = list(current_results)
        self.event_log['Latest Available Data'].append(current_results[2].get_text())
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

        for category in category_list:
            self.njrdata[category].append(0)
            self.njrdata[f'{category} % Change (YoY)'].append(0.0)

    # Function which calculates the difference between the current download date and previous date
    # Use this to calculate the average amount of time it takes between new update periods
    def daysuntilupdate(self):
        """
        Method which returns a timedelta object that depicts the amount of days between
        the program's last run and current update

        :return: delta (timedelta object)
        """
        current = self.run_number
        previous = current - 1
        current_date = datetime.now()
        previous_date = datetime.strptime(self.last_run_date, "%a %b %d %H:%M:%S %Y")
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
    def event_log_update(self, logger):
        """

        Instance method which updates the event log with runtime data of the most recent NJR10k download.
        Stores the type of downlaod/update which was run, the length of the download runtime, current date and
        length in time between the previous and current program runs
        :param logger: logger function which will return event log to ther logger file
        :return: None
        """

        table_name = 'event_log'
        db = pd.DataFrame(self.event_log)

        if not pd.read_sql_table(table_name, self.engine).empty:
            db.to_sql(table_name, self.engine, if_exists='append', index=False)

        print(f'New event log has been saved to the {table_name} in PostgreSQL')
        logger.info(f'New event log has been saved to the {table_name} in PostgreSQL')

    def extract_re_data(self, pdfname, logger, update=None):
        """
        UPDATE

        Function which reads the pdfname name arg and extracts the real estate data from that pdf and stores
        it in the global main_dictionary variable
        :param pdfname: Name of the target pdf
        :param update: Allows for dynamic directory changing if argument is equal to 'Yes'
        :param logger: Keyword argument dictionary which houses the logger function variables
        :return: None
        """

        os.chdir('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
        # For municipalities with the same name and located in multiple counties,
        # the county name will be unpacked in the town var and needs to be extracted to use as an additional
        # redundancy check
        town, county, month, year = Scraper.parse_pdfname(pdfname)

        # Ensure or create directory for destination of pdf after scraping
        destination = os.path.join('C:\\Users\\Omar\\Desktop\\Python Temp Folder\\PDF Temp Files', year, town)

        if not os.path.exists(destination):
            os.makedirs(destination)

        if update == 'Yes':
            temp_town = town
            if county is not None:
                temp_town = " ".join([town, county])
            os.chdir(f'C:\\Users\\Omar\\Desktop\\Python Temp Folder\\PDF Temp Files\\{year}\\{temp_town}')

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

            if update is None:
                shutil.move(os.path.abspath(pdfname), destination)

        except PyPDF2._reader.EmptyFileError:
            logger.warning(f'The city of {town} for {month} {year} does not have data. File possibly corrupted')
            # If function encounters an empty/corrupted pdf,
            # the data_na function will render all information available for that file equal to zero
            # The generator will then pick up at the next pdf to continue extracting data
            self.data_na(town, month, year)
            if update is None:
                shutil.move(os.path.abspath(pdfname), destination)

        except AssertionError:
            logger.warning(f'The municipality and/or county found in the pdf '
                           f'does not match {town} {month} {year}. PDF possibly corrupted.')
            self.data_na(town, month, year)
            if update is None:
                shutil.move(os.path.abspath(pdfname), destination)

    @staticmethod
    def find_closed_sales(pdf_text):
        """
        :param pdf_text:
        :return:
        """

        variable_list = []

        closed_sales_pattern = re.compile(
            r'Closed\sSales\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)'
            r'\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)')
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

        county_pattern = re.compile(r'(Atlantic|Bergen|Burlington|Camden|Cape May|Cumberland|Essex|'
                                    r'Gloucester|Hudson|Hunterdon|Mercer|Middlesex|Monmouth|Morris|Ocean|'
                                    r'Passaic|Salem|Somerset|Sussex|Union|Warren) County')

        found_county = county_pattern.search(pdftext).group()

        return found_county

    @staticmethod
    def find_dom(pdf_text):
        """
        :param pdf_text:
        :return:
        """

        variable_list = []

        dom_pattern = re.compile(
            r'Days\son\sMarket\sUntil\sSale\s(\d{0,3}?)\s(\d{0,3}?)\s'
            r'(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)')
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
            r'Inventory\sof\sHomes\sfor\sSale\s(--|\d{0,3}?)\s(--|\d{0,3}?)\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)'
            r'\s(--|\d{0,3}?)\s(--|\d{0,3}?)\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
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
            r'Key\sMetrics\s(\d{4})\s(\d{4})\sPercent\sChange\sThru'
            r'\s\d{1,2}?-\d{4}\sThru\s\d{1,2}?-\d{4}\sPercent\sChange')
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
            r'Median\sSales\sPrice\*\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})'
            r'\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})'
            r'\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
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
            r'(January|February|March|April|May|June|July|August|September|October|November|December)'
            r'\sYear\sto\sDate\sSingle\sFamily')

        return month_pattern.search(pdf_text).group(1)

    @staticmethod
    def find_new_listings(pdf_text):
        """
        :param pdf_text:
        :return:
        """

        variable_list = []

        new_listings_pattern = re.compile(
            r'New\sListings\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)'
            r'\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)')
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
            r'Percent\sof\sList\sPrice\sReceived\*\s(\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(0.0%|--|[+-]'
            r'\s\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
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
            r'Months\sSupply\sof\sInventory\s(--|\d{1,2}?.\d{1})\s(--|\d{1,2}?.\d{1})\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)'
            r'\s(--|\d{1,2}?.\d{1})\s(--|\d{1,2}?.\d{1})\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
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
        self.njrdata['Year'].append(Scraper.find_key_metrics(data))
        self.njrdata['City'].append(city)
        self.njrdata['County'].append(Scraper.find_county(information))

        for category, function in zip(category_list, function_list):
            scraped_data = function(data)
            self.njrdata[category].append(scraped_data[0])
            self.njrdata[f'{category} % Change (YoY)'].append(scraped_data[1])

    def last_downloaded_pdf(self):
        """
        Used in case the njr10k function is used recursively.
        This function will find the latest file downloaded and continue from that point
        :return:
        """

        pdf_download_pattern = re.compile(r'The data for\s(.*\.pdf) has been downloaded')
        downloads_finished = re.compile(r'All PDFs have been downloaded|All files downloaded')
        logger_file = Scraper.read_logger()

        if logger_file != 'No logger file available':
            if downloads_finished.search(logger_file):
                self.scrape_event_log(logger_file)
                print('Previously downloaded pdfs have been found. All municipalities have been downloaded')
                return 'All files downloaded'
            else:
                try:
                    last_download = re.findall(pdf_download_pattern, logger_file)[-1]
                    print(f'Previously downloaded pdfs have been found. Continuing download from {last_download}')
                    return last_download

                except IndexError:
                    # No pdfs have been downloaded
                    print('No logger file available')
                    return 'No logger file available'
        else:
            print('No logger file available')
            return 'No logger file available'

    def latest_event_data(self):
        """
        - Accept the self.db_engine as a var since the classmethod cant directly access it
        - latest_event = pd.read_sql_table('event_log', engine=db_engine).loc[-1]
        - last_run_num = latest_event['Run Num']
        - last_date = latest_event['Latest Available Data'].split(' ')
        - Also have a try-except block that says if there's no data:
        return 1, 09, 2019
        :return:
        """

        table_name = 'event_log'

        try:
            latest_event = pd.read_sql_table(table_name, self.engine).iloc[-1]
            last_ran_data = latest_event['Latest Available Data'].split(' ')
            last_month = last_ran_data[0]
            last_year = last_ran_data[1]

            return latest_event['Number of Runs'] + 1, last_month, last_year, latest_event['Run Date']

        except IndexError:
            # The table exists but there's no data in it yet
            return 1, 'September', 2019, None

        except sqlalchemy.exc.ProgrammingError:
            # The table doesn't exist. SQLAlchemy raises error
            return 1, 'September', 2019, None

    def latest_nj_data(self):
        """

        :return:
        """

        base_url = 'https://njar.stats.10kresearch.com/reports'
        response = self.session.get(base_url)

        if response.status_code == 200:
            page_source = response.text
            soup = BeautifulSoup(page_source, 'html.parser')
            self.area_results(soup)

            return self.current_data_avail(soup)

        else:
            response.raise_for_status()

    def load_checkpoint(self, logger):

        previous_wd = os.getcwd()
        os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')

        with shelve.open('NJ Scrapper Data', writeback=True) as saved_data_file:
            self.njrdata = saved_data_file['Main Dictionary']

            print('Previous data has been loaded')
            logger.info('Previous data has been loaded')

        os.chdir(previous_wd)

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

    def njr10k(self, timeframe, logger, municipality=None):
        """
        UPDATE
        Function which automatically downloads the pdfs from each city to get ready to scrape

        :param timeframe:
        :param logger:
        :param municipality
        :return:
        """

        start_time = datetime.now()

        base_url = 'http://njar.stats.10kresearch.com/docs/lmu/'
        params = {'src': 'Page'}

        for year, months_list in timeframe.items():
            # If the year equals the first index in the timeframe
            # keys AND municipality is not None, there was a previous
            # script interruption and is continuing from where it stopped
            if year == list(timeframe.keys())[0] and municipality is not None:
                town_index = self.towns.index(municipality)
                target_list = list(product(year, months_list, self.towns[town_index + 1:]))
            else:
                target_list = list(product([year], months_list, self.towns))

            for _, data in zip(trange(len(target_list), desc='Downloaded PDFs'), target_list):
                pdf_url, new_filename = Scraper.create_url_and_pdfname(base_url, data[0], data[1], data[2])
                self.download_pdf(pdf_url, new_filename, params, logger)
                logger.info(f'The data for {new_filename} has been downloaded')

        logger.info('All files downloaded')

        end_time = datetime.now()

        winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)

        self.event_log['Run Type'].append('NJR10k')
        self.event_log['Run Time'].append(str(end_time - start_time))

    def pandas2sql(self, logger):
        """
        UPDATE

        :param logger:
        :return:
        """

        table_name = 'nj_realtor_basic'
        db = pd.DataFrame(self.njrdata)
        db = Scraper.clean_db(db)
        db.drop_duplicates(subset=['Municipality', 'Month', 'Year'], keep='first', inplace=True, ignore_index=True)

        if not pd.read_sql_table(table_name, self.engine).empty:
            db.to_sql(table_name, self.engine, if_exists='append', chunksize=1000, index=False)

        print(f'New NJ Realtor data has been saved to the {table_name} in PostgreSQL')
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
            return [i for i in os.listdir(base_path) if i != 'PDF Temp Files']

        elif type(pdfname) is list:
            # This pdfname won't be a list of pdf names but a list of municipalities
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

        with shelve.open('NJ Scrapper Data', writeback=True) as saved_data_file:
            saved_data_file['Main Dictionary'] = self.njrdata

            print('Scraped data has been saved')
            logger.info('Scraped data has been saved')

        os.chdir(previous_wd)

    @staticmethod
    def read_logger():
        """
        UPDATE
        A function I can use for debugging purposes.
        In the event a failure occurs before reaching the extract_re_data function
        I can read the logger file and start again as a midway point

        :return:
        """

        previous_wd = os.getcwd()
        logger_dir = 'F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Logs'
        os.chdir(logger_dir)

        try:
            # The index should be -1. Only using -2 for the first run until code is fixed
            logger_file = sorted(os.listdir(logger_dir), key=Scraper.sortby_date_created)[-1]
            file_date = Scraper.create_date_object(logger_file)
            today = date.today()

            if abs(today - file_date).days > 5:
                # This file will be considered too old and therefore, no file is available
                raise IndexError

            with open(logger_file, 'r') as file:
                os.chdir(previous_wd)
                target_file = file.read()

                if len(target_file) > 1:
                    # If these patterns are found
                    return target_file
                else:
                    raise IndexError

        except IndexError:
            os.chdir(previous_wd)
            return 'No logger file available'

    def scrape_data(self, logger):

        pdf_list = Scraper.pdf_generator(pdfname=self.update_list)
        for _, pdf in zip(trange(len(pdf_list), desc='Extracted PDFs'), pdf_list):
            if self.update_list is not None:
                self.extract_re_data(pdf, logger, update='Yes')
            else:
                self.extract_re_data(pdf, logger)

        logger.info('All PDFs have been scraped')

        winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)
        # Step 3: Save the downloaded data. Creates a checkpoint in the event of script failure
        self.process_checkpoint(logger)

    def scrape_event_log(self, text):

        event_log_pattern = re.compile(r"('Number of Runs': \d{1,4}), ('Run Type': .*), "
                                       r"('Latest Available Data': .*), ('Run Time': .*), ('Run Date': .*), "
                                       r"('Days Between Update': \d{1,3})")
        time_pattern = re.compile(r'datetime\.timedelta\((.*)\)')

        for index in range(1, 7):
            found_targets = event_log_pattern.search(text).group(index).split(':')
            category = ''.join([i for i in found_targets[0].strip() if i != "'"])

            if category == 'Number of Runs' or category == 'Days Between Update':
                value = int(found_targets[1].strip())
            elif category == 'Run Time':
                if time_pattern.search(text):
                    value = Scraper.create_timestamp(time_pattern.search(text).group(1))
                else:
                    value = ''.join([i for i in found_targets[1].strip() if i != "'"])
            elif category == 'Latest Available Data':
                if len(self.event_log['Latest Available Data']) > 0:
                    continue
                value = ''.join([i for i in found_targets[1].strip() if i != "'"])
            else:
                value = ''.join([i for i in found_targets[1].strip() if i != "'"])

            self.event_log[category].append(value)

        print(f'Event log found in logger file and saved for this run')

    @staticmethod
    def sortby_date_created(file):
        """

        :param file:
        :return:
        """
        file_stats = os.stat(file)
        return time.strptime(time.ctime(file_stats.st_mtime))

    @logger_decorator
    def main(self, **kwargs):

        logger = kwargs['logger']
        f_handler = kwargs['f_handler']
        c_handler = kwargs['c_handler']

        target_pdfs_path = f'C:\\Users\\Omar\\Desktop\\Python Temp Folder\\PDF Temp Files\\{self.current_year}'
        shelve_dir = f'F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data'

        # Step 1a: Check if new data is available
        assert self.current_month != self.last_ran_month, 'No new real estate data available'
        print(f'New data available for {self.current_month + " " + self.current_year}')

        # Step 1b: Check for a recent logger file and if any download pdfs
        # are in the target folder
        results = self.last_downloaded_pdf()

        if results == 'No logger file available' or results.endswith('.pdf'):

            # Ensure the target destination for downloaded pdfs is available
            # If not then create it
            if not os.path.exists(target_pdfs_path):
                os.makedirs(target_pdfs_path)

            self.event_log['Number of Runs'].append(self.run_number)
            self.event_log['Run Date'].append(time.ctime())
            self.event_log['Days Between Update'].append(self.daysuntilupdate())

            # If this instance variable is None, all new data is downloaded
            if self.update_list is None:
                print('Creating pdf downloading timeframe')
                municipality_name, timeframe = self.create_timeframe(results, logger)
                self.njr10k(timeframe, logger, municipality_name)
            else:
                # The pdfs are already downloaded and in their respective directories
                self.event_log['Run Type'].append("Fill Missing Data")

            logger.info(f'New Event Log: {self.event_log}')

        # Step 2: Scrape information from pdfs
        if results == 'All files downloaded':
            # Create function to load the proper shelve file
            shelve_file = os.path.join(shelve_dir, 'NJ Scrapper Data.dat')
            if os.path.exists(shelve_file):
                self.load_checkpoint(logger)
            else:
                self.scrape_data(logger)
        else:
            self.scrape_data(logger)

        # Step 4: Remove duplicate data (if any) from df and save to sql
        self.pandas2sql(logger)
        # Step 5: Save event_log and data to respective sql dfs
        self.event_log_update(logger)

        logger.removeHandler(f_handler)
        logger.removeHandler(c_handler)
        logging.shutdown()

        message_body = f"NJ Realtor Data for {self.event_log['Latest Available Data']} has been downloaded and" \
                       f"saved in PostgreSQL"

        return message_body


if __name__ == '__main__':

    """This is the first part of the NJ Realtor 10k Scrapper. 
    This section of the program will systematically check for the 
    most recent files uploaded to their database and download them 
    for processing and analysis in the second half of the program.
    """

    try:

        url = 'https://www.njrealtor.com/login/?rd=10&passedURL=/goto/10k/'
        url1 = 'https://www.njrealtor.com/ramco-api/web-services/login_POST.php'

        with requests.Session() as s:
            # create a function/module which returns the njr10k info
            username, _, pw = Scraper.get_us_pw('NJRealtor')

            # payload sent during the HTTP POST
            payload1 = {'rd': '10',
                        'passedURL': '/goto/10k/',
                        'case': '',
                        'LoginEmail': username,
                        'LoginPassword': pw,
                        'LoginButton': 'Login'}

            s.get(url)  # Request to arrive at the log-in page
            s.post(url1, data=payload1)

            obj = Scraper(s)
            message = obj.main()

    except KeyboardInterrupt:
        print()
        print('Program was manually stopped')
    except AssertionError as ae:
        # Send message that says no new data is available
        # Tell airflow to try again tomorrow
        pass
    else:
        text_message(message)
