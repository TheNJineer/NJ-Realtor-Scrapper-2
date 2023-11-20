import os
import winsound
import openpyxl
import PyPDF2
import shutil
import shelve
import datetime
from datetime import date
import enlighten
import logging
import zipfile
from send2trash import send2trash
from os import strerror
import requests
from sys import path
import re
import time
import pandas as pd
from tabulate import tabulate
from bs4 import BeautifulSoup
import pprint
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
# Allows us to interact with the Enter key and see search results
from selenium.webdriver.common.keys import Keys
# Allows Selenium to search for page elements By their attributes
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
# Next two imports set the program up for explicit waits so the document doesn't move to ther next step until the element is found
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# Allows for Selenium to click a button
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementNotVisibleException
from selenium.common.exceptions import NoSuchElementException


class Scraper:

    current_data_avail = ''
    no_of_runs = 0
    event_log = {}

    def __init__(self):
        #Change the directory to store the temporary Selenium files to be processed
        os.chdir('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
        #Regex patterns to find matches for the towns and counties in NJ. May not be needed anymore while using BeautifulSoup
        self.__counties_pattern = re.compile(r'<option value="(\w+)(\s\w+)?\sCounty">\w+(\s\w+)?\sCounty</option>')
        self.__towns_pattern = re.compile(r'<option value="(\w+\s\w+(\s\w+)?(\s\w+)?)">\w+\s\w+(\s\w+)?(\s\w+)?</option>')
        #Empty list to stored the found cities and counties
        self.__counties = []
        self.__towns = []
        #The years and months for the real estate data I'm looking for
        self.__years = ['2019', '2020', '2021', '2022', '2023']
        self.__months = {'01': 'January',
                         '02': 'February',
                         '03': 'March',
                         '04': 'April',
                         '05': 'May',
                         '06': 'June',
                         '07': 'July',
                         '08': 'August',
                         '09': 'September',
                         '10': 'October',
                         '11': 'November',
                         '12': 'December'
                         }
        #self.__last_run = '' This may not be needed anymore since the event_log class variable created to keep track of run times

    # Function which scrapes the cities and counties from the njrealtor 10k state page
    def area_results(self, soup):
        area = soup.find('select', id="lmuArea").children
        for obj in area:
            newobj = obj.get_text()
            if newobj in ['Select an area...', 'Entire State', '\n']:
                continue
            else:
                if 'County' in newobj:
                    # I believe there are multiple towns in different counties with the same name. Their county is attached
                    # and need to be seperated from the target values
                    if '/' in newobj:
                        # Do not split the city name if it has '/' in it. It signifies that there is more than 1 city with that name and shows the county it belongs to
                        #newobj = newobj.split('/')
                        city = newobj
                        self.__towns.append(city)
                    else:
                        self.__counties.append(newobj)
                else:
                    self.__towns.append(newobj)

    def check_results(self):

        event_log_list = list(Scraper.event_log.keys())
        last_run_num = event_log_list[-1]
        last_data_avail = Scraper.event_log[last_run_num]['Latest Avaiable Data']

        if Scraper.current_data_avail == last_data_avail:

            return True

        elif Scraper.current_data_avail != last_data_avail:
            start = last_data_avail.split()
            finish = Scraper.current_data_avail.split()

            return start, finish

    def corrupted_files(self, list):

        logger = logging.getLogger("Corrupted Files")
        logger.setLevel(logging.DEBUG)
        # Create the FileHandler() and StreamHandler() loggers
        f_handler = logging.FileHandler('Corrupted Files ' + str(datetime.datetime.today().date()) + '.log')
        f_handler.setLevel(logging.DEBUG)
        c_handler = logging.StreamHandler()
        c_handler.setLevel(logging.INFO)
        # Create formatting for the loggers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')
        # Set the formatter for each handler
        f_handler.setFormatter(formatter)
        c_handler.setFormatter(formatter)
        logger.addHandler(f_handler)
        logger.addHandler(c_handler)

        start_time = datetime.datetime.now()

        dict = {}
        name = 'Corrupted Files'

        possible_corrupted_files = []
        # Do I want to delete the corrupted files before redownloading them?
        # Extract the town, month and year from the corrupted file name and put it in a dictionary for further processing
        # Real estate data pulled for the year 2019 seems to not be available for some areas so we'll skip this file. It is a known error
        for n, i in enumerate(list):
            info = i.rstrip('.pdf').split(' ')
            town = info[0:len(info) - 2]
            if len(town) > 1:
                if 'County' in town:
                    # This means the city name is a duplicate and needs to have the county distinguished
                    # For example: ['Franklin', 'Twp', 'Gloucester', 'County'] --------> ['Franklin', 'Twp', '/', 'Gloucester', 'County']
                    town.insert(town.index('County') - 1, '/')
                    town = ' '.join(town)
                else:
                    town = ' '.join(town)
            else:
                town = info[0]

            month = info[-2]
            year = info[-1]

            if year == '2019':
                # Skip all corrupted files from 2019. That data is not available
                possible_corrupted_files.append(i)
                continue
            else:
                for m, i in enumerate(self.__towns):
                    if town in i:
                        town = self.__towns[m]
                        dict[n] = [town, month, year]

        # UnboundLocalError produced without this checkpoint.
        if len(dict) < 1:
            return possible_corrupted_files

        base_url = 'http://njar.stats.10kresearch.com/docs/lmu/'

        with requests.Session() as session:
            username, pw = self.get_us_pw('NJRealtor')

            payload1 = {'rd': '10',
                        'passedURL': '/goto/10k/',
                        'case': '',
                        'LoginEmail': username,
                        'LoginPassword': pw,
                        'LoginButton': 'Login'}

            params = {'src': 'Page'}
            # months = list(self.__months.keys())

            # months_tracker = manager.counter(total=len(months), desc=f'Year:', unit='Months')

            url = 'https://www.njrealtor.com/login/?rd=10&passedURL=/goto/10k/'
            url2 = 'https://www.njrealtor.com/ramco-api/web-services/login_POST.php'

            response = session.get(url)
            r_post = session.post(url2, data=payload1)

            for k, v in dict.items():
                city0 = v[0].split(' ')
                city = ''.join(city0)
                # Redundant checker if a 2019 file has slipped through the first check
                if v[2] == '2019':
                    continue
                else:
                    y = v[2]
                for k, v in self.__months.items():
                    if month in v:
                        m = k

            try:
                if '/' not in city:
                    url3 = base_url + y + '-' + m + '/x/' + city
                    new_filename = " ".join([' '.join(city0), self.__months[m], y]) + ".pdf"

                elif '/' in city:
                    city = '%2F'.join(city.split('/'))
                    del city0[city0.index('/')]
                    url3 = base_url + y + '-' + m + '/x/' + city
                    new_filename = " ".join([' '.join(city0), self.__months[m], y]) + ".pdf"

                with session.get(url3, params=params, stream=True) as reader, open(new_filename, 'wb') as writer:
                    for chunk in reader.iter_content(chunk_size=1000000):
                        # Casting the bytes into a str type and slicing the first 20 characters to check if 'PDF' is in
                        check_pdf = str(chunk)[:20]
                        #print(check_pdf)
                        if 'PDF' in check_pdf:
                            writer.write(chunk)
                        else:
                            logger.warning(f'WARNING! {new_filename} is possibly a corrupted file')
                            possible_corrupted_files.append(new_filename)

            except IOError:
                """An OS Error has occurred """
                logger.exception(f'IOError has Occurred')

            except requests.exceptions.HTTPError as rht:
                """An HTTP error occurred."""
                logger.exception(f'An HTTP has Occurred: {rht}')

            except requests.exceptions.Timeout as ret:
                """The request timed out.
                Catching this error will catch both
                :exc:`~requests.exceptions.ConnectTimeout` and
                :exc:`~requests.exceptions.ReadTimeout` errors.
                """
                logger.exception(f'The Request Has Timed Out: {ret}')

            except requests.exceptions.InvalidURL as rei:
                """The URL provided was somehow invalid."""
                logger.exception(f'The URL Provided Was Invalid: {rei}')

            except requests.exceptions.RetryError as rer:
                """Custom retries logic failed"""
                logger.exception(f'Custom Retries Logic Failed: {rer}')

            except requests.exceptions.StreamConsumedError as res:
                """The content for this response was already consumed."""
                logger.exception(f'The Content For This Response Was Already Consumed: {res}')

            except requests.exceptions.ContentDecodingError as rec:
                """Failed to decode response content."""
                logger.exception(f'Failed to Decode Response Content: {rec}')

            except requests.exceptions.ChunkedEncodingError as rece:
                """The server declared chunked encoding but sent an invalid chunk."""
                logger.exception(f'Invalid Chunk Encoding: {rece}')

            except:
                logger.exception('An Error Has Occured: Unhandled')

            else:

                end_time = datetime.datetime.now()
                run_time = end_time - start_time

                if Scraper.no_of_runs == 0:
                    Scraper.event_log[Scraper.no_of_runs] = {'Run Type': name,
                                                             'Latest Available Data': self.current_data_avail,
                                                             'Run Time' : run_time,
                                                             'Run Date': time.ctime(),
                                                             'Days Between Update': 0}
                    logger.info(f'New Event Log Created: \n{Scraper.event_log}')
                elif Scraper.no_of_runs > 0:
                    Scraper.event_log[Scraper.no_of_runs] = {'Run Type' : name,
                                                             'Latest Available Data': self.current_data_avail,
                                                             'Run Time' : run_time,
                                                             'Run Date': time.ctime(),
                                                             'Days Between Update': self.daysuntilupdate(
                                                                 Scraper.no_of_runs)}
                    logger.info(f'New Event Log Created: \n{Scraper.event_log}')
                Scraper.no_of_runs += 1
                # This is a very lengthy program so I'd like play a sound that signifies the process is done
                winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)

        return possible_corrupted_files

    def create_categories(dict):
        # Seperate each dictionary from the quarterly_yearly function into their tracked categories
        new_dict = {}
        for k, v in dict.items():
            new_dict.setdefault(k, {})
            new_dict[k].setdefault('Q1', [])
            new_dict[k].setdefault('Q2', [])
            new_dict[k].setdefault('Q3', [])
            new_dict[k].setdefault('Q4', [])
            for df in v:
                temp = df.columns.values.tolist()
                if len(temp) > 3:
                    fy_new_listings = df[[column for column in df.columns if 'New Listings' in column if k in column]]
                    fy_new_listings['Mean of FY ' + k + ' New Listings'] = fy_new_listings.mean(axis=1)
                    fy_new_listings['Median of FY ' + k + ' New Listings'] = fy_new_listings.median(axis=1)
                    fy_new_listings['Std Dev of FY ' + k + ' New Listings'] = fy_new_listings.std(axis=1)
                    fy_new_listings['Min of FY ' + k + ' New Listings'] = fy_new_listings.min(axis=1)
                    fy_new_listings['Max of FY ' + k + ' New Listings'] = fy_new_listings.max(axis=1)
                    fy_closed_sales = df[[column for column in df.columns if 'Closed Sales' in column if k in column]]
                    fy_closed_sales['Mean of FY ' + k + ' Closed Sales'] = fy_closed_sales.mean(axis=1)
                    fy_closed_sales['Median of FY ' + k + ' Closed Sales'] = fy_closed_sales.median(axis=1)
                    fy_closed_sales['Std Dev of FY ' + k + ' Closed Sales'] = fy_closed_sales.std(axis=1)
                    fy_closed_sales['Min of FY ' + k + ' Closed Sales'] = fy_closed_sales.min(axis=1)
                    fy_closed_sales['Max of FY ' + k + ' Closed Sales'] = fy_closed_sales.max(axis=1)
                    fy_dom = df[
                        [column for column in df.columns if 'Days on Market Until Sale' in column if k in column]]
                    fy_dom['Mean of FY ' + k + ' Days on Market'] = fy_dom.mean(axis=1)
                    fy_dom['Median of FY ' + k + ' Days on Market'] = fy_dom.median(axis=1)
                    fy_dom['Std Dev of FY ' + k + ' Days on Market'] = fy_dom.std(axis=1)
                    fy_dom['Min of FY ' + k + ' Days on Market'] = fy_dom.min(axis=1)
                    fy_dom['Max of FY ' + k + ' Days on Market'] = fy_dom.max(axis=1)
                    fy_median_sales = df[
                        [column for column in df.columns if 'Median Sales Prices' in column if k in column]]
                    fy_median_sales['Mean of FY ' + k + ' Median Sales Price'] = fy_median_sales.mean(axis=1)
                    fy_median_sales['Median of FY ' + k + ' Median Sales Price'] = fy_median_sales.median(axis=1)
                    fy_median_sales['Std Dev of FY ' + k + ' Median Sales Price'] = fy_median_sales.std(axis=1)
                    fy_median_sales['Min of FY ' + k + ' Median Sales Price'] = fy_median_sales.min(axis=1)
                    fy_median_sales['Max of FY ' + k + ' Median Sales Price'] = fy_median_sales.max(axis=1)
                    fy_polpr = df[
                        [column for column in df.columns if 'Percent of List Price Received' in column if k in column]]
                    fy_polpr['Mean of FY ' + k + ' Percent of List Price Received'] = fy_polpr.mean(axis=1)
                    fy_polpr['Median of FY ' + k + ' Percent of List Price Received'] = fy_polpr.median(axis=1)
                    fy_polpr['Std Dev of FY ' + k + ' Percent of List Price Received'] = fy_polpr.std(axis=1)
                    fy_polpr['Min of FY ' + k + ' Percent of List Price Received'] = fy_polpr.min(axis=1)
                    fy_polpr['Max of FY ' + k + ' Percent of List Price Received'] = fy_polpr.max(axis=1)
                    fy_inventory = df[
                        [column for column in df.columns if 'Inventory of Homes for Sale' in column if k in column]]
                    fy_inventory['Mean of FY ' + k + ' Inventory of Homes'] = fy_inventory.mean(axis=1)
                    fy_inventory['Median of FY ' + k + ' Inventory of Homes'] = fy_inventory.median(axis=1)
                    fy_inventory['Std Dev of FY ' + k + ' Inventory of Homes'] = fy_inventory.std(axis=1)
                    fy_inventory['Min of FY ' + k + ' Inventory of Homes'] = fy_inventory.min(axis=1)
                    fy_inventory['Max of FY ' + k + ' Inventory of Homes'] = fy_inventory.max(axis=1)
                    fy_supply = df[
                        [column for column in df.columns if 'Months Supply of Inventory' in column if k in column]]
                    fy_supply['Mean of FY ' + k + ' Monthly Supply'] = fy_supply.mean(axis=1)
                    fy_supply['Median of FY ' + k + ' Monthly Supply'] = fy_supply.median(axis=1)
                    fy_supply['Std Dev of FY ' + k + ' Monthly Supply'] = fy_supply.std(axis=1)
                    fy_supply['Min of FY ' + k + ' Monthly Supply'] = fy_supply.min(axis=1)
                    fy_supply['Max of FY ' + k + ' Monthly Supply'] = fy_supply.max(axis=1)
                    new_dict[k]['FY'] = [fy_new_listings, fy_closed_sales, fy_dom, fy_median_sales, fy_polpr,
                                         fy_inventory, fy_supply]
                elif 'Jan' in temp[0]:
                    fyq1_new_listings = df[[column for column in df.columns if 'New Listings' in column if k in column]]
                    fyq1_new_listings['Mean of Q1 ' + k + ' New Listings'] = fyq1_new_listings.mean(axis=1)
                    fyq1_new_listings['Median of Q1 ' + k + ' New Listings'] = fyq1_new_listings.median(axis=1)
                    fyq1_new_listings['Std Dev of Q1 ' + k + ' New Listings'] = fyq1_new_listings.std(axis=1)
                    fyq1_new_listings['Min of Q1 ' + k + ' New Listings'] = fyq1_new_listings.min(axis=1)
                    fyq1_new_listings['Max of Q1 ' + k + ' New Listings'] = fyq1_new_listings.max(axis=1)
                    fyq1_new_listings = fyq1_new_listings[
                        [column for column in fyq1_new_listings.columns if ['Jan', 'Feb', 'Mar'] not in column if k in column]]
                    fyq1_closed_sales = df[[column for column in df.columns if 'Closed Sales' in column if k in column]]
                    fyq1_closed_sales['Mean of Q1 ' + k + ' Closed Sales'] = fyq1_closed_sales.mean(axis=1)
                    fyq1_closed_sales['Median of Q1 ' + k + ' Closed Sales'] = fyq1_closed_sales.median(axis=1)
                    fyq1_closed_sales['Std Dev of Q1 ' + k + ' Closed Sales'] = fyq1_closed_sales.std(axis=1)
                    fyq1_closed_sales['Min of Q1 ' + k + ' Closed Sales'] = fyq1_closed_sales.min(axis=1)
                    fyq1_closed_sales['Max of Q1 ' + k + ' Closed Sales'] = fyq1_closed_sales.max(axis=1)
                    fyq1_closed_sales = fyq1_closed_sales[
                        [column for column in fyq1_closed_sales.columns if ['Jan', 'Feb', 'Mar'] not in column if
                         k in column]]
                    fyq1_dom = df[
                        [column for column in df.columns if 'Days on Market Until Sale' in column if k in column]]
                    fyq1_dom['Mean of Q1 ' + k + ' Days on the Market'] = fyq1_dom.mean(axis=1)
                    fyq1_dom['Median of Q1 ' + k + ' Days on the Market'] = fyq1_dom.median(axis=1)
                    fyq1_dom['Std Dev of Q1 ' + k + ' Days on the Market'] = fyq1_dom.std(axis=1)
                    fyq1_dom['Min of Q1 ' + k + ' Days on the Market'] = fyq1_dom.min(axis=1)
                    fyq1_dom['Max of Q1 ' + k + ' Days on the Market'] = fyq1_dom.max(axis=1)
                    fyq1_dom = fyq1_dom[
                        [column for column in fyq1_dom.columns if ['Jan', 'Feb', 'Mar'] not in column if
                         k in column]]
                    fyq1_median_sales = df[
                        [column for column in df.columns if 'Median Sales Price' in column if k in column]]
                    fyq1_median_sales['Mean of Q1 ' + k + ' Median Sales'] = fyq1_median_sales.mean(axis=1)
                    fyq1_median_sales['Median of ' + k + ' Q1 Median Sales'] = fyq1_median_sales.median(axis=1)
                    fyq1_median_sales['Std Dev of Q1 ' + k + ' Median Sales'] = fyq1_median_sales.std(axis=1)
                    fyq1_median_sales['Min of Q1 ' + k + ' Median Sales'] = fyq1_median_sales.min(axis=1)
                    fyq1_median_sales['Max of Q1 ' + k + ' Median Sales'] = fyq1_median_sales.max(axis=1)
                    fyq1_median_sales = fyq1_median_sales[
                        [column for column in fyq1_median_sales.columns if ['Jan', 'Feb', 'Mar'] not in column if
                         k in column]]
                    fyq1_polpr = df[
                        [column for column in df.columns if 'Percent of List Price Received' in column if k in column]]
                    fyq1_polpr['Mean of Q1 ' + k + ' Percentage of Listing Price Received'] = fyq1_polpr.mean(axis=1)
                    fyq1_polpr['Median of Q1 ' + k + ' Percentage of Listing Price Received'] = fyq1_polpr.median(axis=1)
                    fyq1_polpr['Std Dev of Q1 ' + k + ' Percentage of Listing Price Received'] = fyq1_polpr.std(axis=1)
                    fyq1_polpr['Min of Q1 ' + k + ' Percentage of Listing Price Receive'] = fyq1_polpr.min(axis=1)
                    fyq1_polpr['Max of Q1 ' + k + ' Percentage of Listing Price Received'] = fyq1_polpr.max(axis=1)
                    fyq1_polpr = fyq1_polpr[
                        [column for column in fyq1_polpr.columns if ['Jan', 'Feb', 'Mar'] not in column if
                         k in column]]
                    fyq1_inventory = df[
                        [column for column in df.columns if 'Inventory of Homes for Sale' in column if k in column]]
                    fyq1_inventory['Mean of Q1 ' + k + ' Inventory of Homes'] = fyq1_inventory.mean(axis=1)
                    fyq1_inventory['Median of Q1 ' + k + ' Inventory of Homes'] = fyq1_inventory.median(axis=1)
                    fyq1_inventory['Std Dev of Q1 ' + k + ' Inventory of Homes'] = fyq1_inventory.std(axis=1)
                    fyq1_inventory['Min of Q1 ' + k + ' Inventory of Homes'] = fyq1_inventory.min(axis=1)
                    fyq1_inventory['Max of Q1 ' + k + ' Inventory of Homes'] = fyq1_inventory.max(axis=1)
                    fyq1_inventory = fyq1_inventory[
                        [column for column in fyq1_inventory.columns if ['Jan', 'Feb', 'Mar'] not in column if
                         k in column]]
                    fyq1_supply = df[
                        [column for column in df.columns if 'Months Supply of Inventory' in column if k in column]]
                    fyq1_supply['Mean of Q1 ' + k + ' Monthly Supply'] = fyq1_supply.mean(axis=1)
                    fyq1_supply['Median of Q1 ' + k + ' Monthly Supply'] = fyq1_supply.median(axis=1)
                    fyq1_supply['Std Dev of Q1 ' + k + ' Monthly Supply'] = fyq1_supply.std(axis=1)
                    fyq1_supply['Min of Q1 ' + k + ' Monthly Supply'] = fyq1_supply.min(axis=1)
                    fyq1_supply['Max of Q1 ' + k + ' Monthly Supply'] = fyq1_supply.max(axis=1)
                    fyq1_supply = fyq1_supply[
                        [column for column in fyq1_supply.columns if ['Jan', 'Feb', 'Mar'] not in column if
                         k in column]]
                    new_dict[k]['Q1'] = [fyq1_new_listings, fyq1_closed_sales, fyq1_dom, fyq1_median_sales, fyq1_polpr,
                                         fyq1_inventory, fyq1_supply]
                elif 'Apr' in temp[0]:
                    fyq2_new_listings = df[[column for column in df.columns if 'New Listings' in column if k in column]]
                    fyq2_new_listings['Mean of Q2 ' + k + ' New Listings'] = fyq2_new_listings.mean(axis=1)
                    fyq2_new_listings['Median of Q2 ' + k + ' New Listings'] = fyq2_new_listings.median(axis=1)
                    fyq2_new_listings['Std Dev of Q2 ' + k + ' New Listings'] = fyq2_new_listings.std(axis=1)
                    fyq2_new_listings['Min of Q2 ' + k + ' New Listings'] = fyq2_new_listings.min(axis=1)
                    fyq2_new_listings['Max of Q2 ' + k + ' New Listings'] = fyq2_new_listings.max(axis=1)
                    fyq2_new_listings = fyq2_new_listings[
                        [column for column in fyq2_new_listings.columns if ['Apr', 'May', 'Jun'] not in column if
                         k in column]]
                    fyq2_closed_sales = df[[column for column in df.columns if 'Closed Sales' in column if k in column]]
                    fyq2_closed_sales['Mean of Q2 ' + k + ' Closed Sales'] = fyq2_closed_sales.mean(axis=1)
                    fyq2_closed_sales['Median of Q2 ' + k + ' Closed Sales'] = fyq2_closed_sales.median(axis=1)
                    fyq2_closed_sales['Std Dev of Q2 ' + k + ' Closed Sales'] = fyq2_closed_sales.std(axis=1)
                    fyq2_closed_sales['Min of Q2 ' + k + ' Closed Sales'] = fyq2_closed_sales.min(axis=1)
                    fyq2_closed_sales['Max of Q2 ' + k + ' Closed Sales'] = fyq2_closed_sales.max(axis=1)
                    fyq2_closed_sales = fyq2_closed_sales[
                        [column for column in fyq2_closed_sales.columns if ['Apr', 'May', 'Jun'] not in column if
                         k in column]]
                    fyq2_dom = df[
                        [column for column in df.columns if 'Days on Market Until Sale' in column if k in column]]
                    fyq2_dom['Mean of Q2 ' + k + ' Days on the Market'] = fyq2_dom.mean(axis=1)
                    fyq2_dom['Median of Q2 ' + k + ' Days on the Market'] = fyq2_dom.median(axis=1)
                    fyq2_dom['Std Dev of Q2 ' + k + ' Days on the Market'] = fyq2_dom.std(axis=1)
                    fyq2_dom['Min of Q2 ' + k + ' Days on the Market'] = fyq2_dom.min(axis=1)
                    fyq2_dom['Max of Q2 ' + k + ' Days on the Market'] = fyq2_dom.max(axis=1)
                    fyq2_dom = fyq2_dom[
                        [column for column in fyq2_dom.columns if ['Apr', 'May', 'Jun'] not in column if
                         k in column]]
                    fyq2_median_sales = df[
                        [column for column in df.columns if 'Median Sales Price' in column if k in column]]
                    fyq2_median_sales['Mean of Q2 ' + k + ' Median Sales'] = fyq2_median_sales.mean(axis=1)
                    fyq2_median_sales['Median of Q2 ' + k + ' Median Sales'] = fyq2_median_sales.median(axis=1)
                    fyq2_median_sales['Std Dev of Q2 ' + k + ' Median Sales'] = fyq2_median_sales.std(axis=1)
                    fyq2_median_sales['Min of Q2 ' + k + ' Median Sales'] = fyq2_median_sales.min(axis=1)
                    fyq2_median_sales['Max of Q2 ' + k + ' Median Sales'] = fyq2_median_sales.max(axis=1)
                    fyq2_median_sales = fyq2_median_sales[
                        [column for column in fyq2_median_sales.columns if ['Apr', 'May', 'Jun'] not in column if
                         k in column]]
                    fyq2_polpr = df[
                        [column for column in df.columns if 'Percent of List Price Received' in column if k in column]]
                    fyq2_polpr['Mean of Q2 ' + k + ' Percentage of Listing Price Received'] = fyq2_polpr.mean(axis=1)
                    fyq2_polpr['Median of Q2 ' + k + ' Percentage of Listing Price Received'] = fyq2_polpr.median(axis=1)
                    fyq2_polpr['Std Dev of Q2 ' + k + ' Percentage of Listing Price Received'] = fyq2_polpr.std(axis=1)
                    fyq2_polpr['Min of Q2 ' + k + ' Percentage of Listing Price Received'] = fyq2_polpr.min(axis=1)
                    fyq2_polpr['Max of Q2 ' + k + ' Percentage of Listing Price Received'] = fyq2_polpr.max(axis=1)
                    fyq2_polpr = fyq2_polpr[
                        [column for column in fyq2_polpr.columns if ['Apr', 'May', 'Jun'] not in column if
                         k in column]]
                    fyq2_inventory = df[
                        [column for column in df.columns if 'Inventory of Homes for Sale' in column if k in column]]
                    fyq2_inventory['Mean of Q2 ' + k + ' Inventory of Homes'] = fyq2_inventory.mean(axis=1)
                    fyq2_inventory['Median of Q2 ' + k + ' Inventory of Homes'] = fyq2_inventory.median(axis=1)
                    fyq2_inventory['Std Dev of Q2 ' + k + ' Inventory of Homes'] = fyq2_inventory.std(axis=1)
                    fyq2_inventory['Min of Q2 ' + k + ' Inventory of Homes'] = fyq2_inventory.min(axis=1)
                    fyq2_inventory['Max of Q2 ' + k + ' Inventory of Homes'] = fyq2_inventory.max(axis=1)
                    fyq2_inventory = fyq2_inventory[
                        [column for column in fyq2_inventory.columns if ['Apr', 'May', 'Jun'] not in column if
                         k in column]]
                    fyq2_supply = df[
                        [column for column in df.columns if 'Months Supply of Inventory' in column if k in column]]
                    fyq2_supply['Mean of Q2 ' + k + ' Monthly Supply'] = fyq2_supply.mean(axis=1)
                    fyq2_supply['Median of Q2 ' + k + ' Monthly Supply'] = fyq2_supply.median(axis=1)
                    fyq2_supply['Std Dev of Q2 ' + k + ' Monthly Supply'] = fyq2_supply.std(axis=1)
                    fyq2_supply['Min of Q2 ' + k + ' Monthly Supply'] = fyq2_supply.min(axis=1)
                    fyq2_supply['Max of Q2 ' + k + ' Monthly Supply'] = fyq2_supply.max(axis=1)
                    fyq2_supply = fyq2_supply[
                        [column for column in fyq2_supply.columns if ['Apr', 'May', 'Jun'] not in column if
                         k in column]]
                    new_dict[k]['Q2'] = [fyq2_new_listings, fyq2_closed_sales, fyq2_dom, fyq2_median_sales, fyq2_polpr,
                                         fyq2_inventory, fyq2_supply]
                elif 'Jul' in temp[0]:
                    fyq3_new_listings = df[[column for column in df.columns if 'New Listings' in column if k in column]]
                    fyq3_new_listings['Mean of Q3 ' + k + ' New Listings'] = fyq3_new_listings.mean(axis=1)
                    fyq3_new_listings['Median of Q3 ' + k + ' New Listings'] = fyq3_new_listings.median(axis=1)
                    fyq3_new_listings['Std Dev of Q3 ' + k + ' New Listings'] = fyq3_new_listings.std(axis=1)
                    fyq3_new_listings['Min of Q3 ' + k + ' New Listings'] = fyq3_new_listings.min(axis=1)
                    fyq3_new_listings['Max of Q3 ' + k + ' New Listings'] = fyq3_new_listings.max(axis=1)
                    fyq3_new_listings = fyq3_new_listings[
                        [column for column in fyq3_new_listings.columns if ['Jul', 'Aug', 'Sep'] not in column if
                         k in column]]
                    fyq3_closed_sales = df[[column for column in df.columns if 'Closed Sales' in column if k in column]]
                    fyq3_closed_sales['Mean of Q3 ' + k + ' Closed Sales'] = fyq3_closed_sales.mean(axis=1)
                    fyq3_closed_sales['Median of Q3 ' + k + ' Closed Sales'] = fyq3_closed_sales.median(axis=1)
                    fyq3_closed_sales['Std Dev of Q3 ' + k + ' Closed Sales'] = fyq3_closed_sales.std(axis=1)
                    fyq3_closed_sales['Min of Q3 ' + k + ' Closed Sales'] = fyq3_closed_sales.min(axis=1)
                    fyq3_closed_sales['Max of Q3 ' + k + ' Closed Sales'] = fyq3_closed_sales.max(axis=1)
                    fyq3_closed_sales = fyq3_closed_sales[
                        [column for column in fyq3_closed_sales.columns if ['Jul', 'Aug', 'Sep'] not in column if
                         k in column]]
                    fyq3_dom = df[
                        [column for column in df.columns if 'Days on Market Until Sale' in column if k in column]]
                    fyq3_dom['Mean of Q3 ' + k + ' Days on the Market'] = fyq3_dom.mean(axis=1)
                    fyq3_dom['Median of Q3 ' + k + ' Days on the Market'] = fyq3_dom.median(axis=1)
                    fyq3_dom['Std Dev of Q3 ' + k + ' Days on the Market'] = fyq3_dom.std(axis=1)
                    fyq3_dom['Min of Q3 ' + k + ' Days on the Market'] = fyq3_dom.min(axis=1)
                    fyq3_dom['Max of Q3 ' + k + ' Days on the Market'] = fyq3_dom.max(axis=1)
                    fyq3_dom = fyq3_dom[
                        [column for column in fyq3_dom.columns if ['Jul', 'Aug', 'Sep'] not in column if
                         k in column]]
                    fyq3_median_sales = df[
                        [column for column in df.columns if 'Median Sales Price' in column if k in column]]
                    fyq3_median_sales['Mean of Q3 ' + k + ' Median Sales'] = fyq3_median_sales.mean(axis=1)
                    fyq3_median_sales['Median of Q3 ' + k + ' Median Sale'] = fyq3_median_sales.median(axis=1)
                    fyq3_median_sales['Std Dev of Q3 ' + k + ' Median Sales'] = fyq3_median_sales.std(axis=1)
                    fyq3_median_sales['Min of Q3 ' + k + ' Median Sales'] = fyq3_median_sales.min(axis=1)
                    fyq3_median_sales['Max of Q3 ' + k + ' Median Sales'] = fyq3_median_sales.max(axis=1)
                    fyq3_median_sales = fyq3_median_sales[
                        [column for column in fyq3_median_sales.columns if ['Jul', 'Aug', 'Sep'] not in column if
                         k in column]]
                    fyq3_polpr = df[
                        [column for column in df.columns if 'Percent of List Price Received' in column if k in column]]
                    fyq3_polpr['Mean of Q3 ' + k + ' Percentage of Listing Price Received'] = fyq3_polpr.mean(axis=1)
                    fyq3_polpr['Median of Q3 ' + k + ' Percentage of Listing Price Received'] = fyq3_polpr.median(axis=1)
                    fyq3_polpr['Std Dev of Q3 ' + k + ' Percentage of Listing Price Received'] = fyq3_polpr.std(axis=1)
                    fyq3_polpr['Min of Q3 ' + k + ' Percentage of Listing Price Received'] = fyq3_polpr.min(axis=1)
                    fyq3_polpr['Max of Q3 ' + k + ' Percentage of Listing Price Received'] = fyq3_polpr.max(axis=1)
                    fyq3_polpr = fyq3_polpr[
                        [column for column in fyq3_polpr.columns if ['Jul', 'Aug', 'Sep'] not in column if
                         k in column]]
                    fyq3_inventory = df[
                        [column for column in df.columns if 'Inventory of Homes for Sale' in column if k in column]]
                    fyq3_inventory['Mean of Q3 ' + k + ' Inventory of Homes'] = fyq3_inventory.mean(axis=1)
                    fyq3_inventory['Median of Q3 ' + k + ' Inventory of Homes'] = fyq3_inventory.median(axis=1)
                    fyq3_inventory['Std Dev of Q3 ' + k + ' Inventory of Homes'] = fyq3_inventory.std(axis=1)
                    fyq3_inventory['Min of Q3 ' + k + ' Inventory of Homes'] = fyq3_inventory.min(axis=1)
                    fyq3_inventory['Max of Q3 ' + k + ' Inventory of Homes'] = fyq3_inventory.max(axis=1)
                    fyq3_inventory = fyq3_inventory[
                        [column for column in fyq3_inventory.columns if ['Jul', 'Aug', 'Sep'] not in column if
                         k in column]]
                    fyq3_supply = df[
                        [column for column in df.columns if 'Months Supply of Inventory' in column if k in column]]
                    fyq3_supply['Mean of Q3 ' + k + ' Monthly Supply'] = fyq3_supply.mean(axis=1)
                    fyq3_supply['Median of Q3 ' + k + ' Monthly Supply'] = fyq3_supply.median(axis=1)
                    fyq3_supply['Std Dev of Q3 ' + k + ' Monthly Supply'] = fyq3_supply.std(axis=1)
                    fyq3_supply['Min of Q3 ' + k + ' Monthly Supply'] = fyq3_supply.min(axis=1)
                    fyq3_supply['Max of Q3 ' + k + ' Monthly Supply'] = fyq3_supply.max(axis=1)
                    fyq3_supply = fyq3_supply[
                        [column for column in fyq3_supply.columns if ['Jul', 'Aug', 'Sep'] not in column if
                         k in column]]
                    new_dict[k]['Q3'] = [fyq3_new_listings, fyq3_closed_sales, fyq3_dom, fyq3_median_sales, fyq3_polpr,
                                         fyq3_inventory, fyq3_supply]
                elif 'Oct' in temp[0]:
                    fyq4_new_listings = df[[column for column in df.columns if 'New Listings' in column if k in column]]
                    fyq4_new_listings['Mean of Q4 ' + k + ' New Listings'] = fyq4_new_listings.mean(axis=1)
                    fyq4_new_listings['Median of Q4 ' + k + ' New Listings'] = fyq4_new_listings.median(axis=1)
                    fyq4_new_listings['Std Dev of Q4 ' + k + ' New Listings'] = fyq4_new_listings.std(axis=1)
                    fyq4_new_listings['Min of Q4 ' + k + ' New Listings'] = fyq4_new_listings.min(axis=1)
                    fyq4_new_listings['Max of Q4 ' + k + ' New Listings'] = fyq4_new_listings.max(axis=1)
                    fyq4_new_listings = fyq4_new_listings[
                        [column for column in fyq4_new_listings.columns if ['Oct', 'Nov', 'Dec'] not in column if
                         k in column]]
                    fyq4_closed_sales = df[[column for column in df.columns if 'Closed Sales' in column if k in column]]
                    fyq4_closed_sales['Mean of Q4 ' + k + ' Closed Sales'] = fyq4_closed_sales.mean(axis=1)
                    fyq4_closed_sales['Median of Q4 ' + k + ' Closed Sales'] = fyq4_closed_sales.median(axis=1)
                    fyq4_closed_sales['Std Dev of Q4 ' + k + ' Closed Sales'] = fyq4_closed_sales.std(axis=1)
                    fyq4_closed_sales['Min of Q4 ' + k + ' Closed Sales'] = fyq4_closed_sales.min(axis=1)
                    fyq4_closed_sales['Max of Q4 ' + k + ' Closed Sales'] = fyq4_closed_sales.max(axis=1)
                    fyq4_closed_sales = fyq4_closed_sales[
                        [column for column in fyq4_closed_sales.columns if ['Oct', 'Nov', 'Dec'] not in column if
                         k in column]]
                    fyq4_dom = df[
                        [column for column in df.columns if 'Days on Market Until Sale' in column if k in column]]
                    fyq4_dom['Mean of Q4 ' + k + ' Days on the Market'] = fyq4_dom.mean(axis=1)
                    fyq4_dom['Median of Q4 ' + k + ' Days on the Market'] = fyq4_dom.median(axis=1)
                    fyq4_dom['Std Dev of Q4 ' + k + ' Days on the Market'] = fyq4_dom.std(axis=1)
                    fyq4_dom['Min of Q4 ' + k + ' Days on the Market'] = fyq4_dom.min(axis=1)
                    fyq4_dom['Max of Q4 ' + k + ' Days on the Market'] = fyq4_dom.max(axis=1)
                    fyq4_dom = fyq4_dom[
                        [column for column in fyq4_dom.columns if ['Oct', 'Nov', 'Dec'] not in column if
                         k in column]]
                    fyq4_median_sales = df[
                        [column for column in df.columns if 'Median Sales Price' in column if k in column]]
                    fyq4_median_sales['Mean of Q4 ' + k + ' Median Sales'] = fyq4_median_sales.mean(axis=1)
                    fyq4_median_sales['Median of Q4 ' + k + ' Median Sales'] = fyq4_median_sales.median(axis=1)
                    fyq4_median_sales['Std Dev of Q4 ' + k + ' Median Sales'] = fyq4_median_sales.std(axis=1)
                    fyq4_median_sales['Min of Q4 ' + k + ' Median Sales'] = fyq4_median_sales.min(axis=1)
                    fyq4_median_sales['Max of Q4 ' + k + ' Median Sales'] = fyq4_median_sales.max(axis=1)
                    fyq4_median_sales = fyq4_median_sales[
                        [column for column in fyq4_median_sales.columns if ['Oct', 'Nov', 'Dec'] not in column if
                         k in column]]
                    fyq4_polpr = df[
                        [column for column in df.columns if 'Percent of List Price Received' in column if k in column]]
                    fyq4_polpr['Mean of Q4 ' + k + ' Percentage of Listing Price Received'] = fyq4_polpr.mean(axis=1)
                    fyq4_polpr['Median of Q4 ' + k + ' Percentage of Listing Price Received'] = fyq4_polpr.median(axis=1)
                    fyq4_polpr['Std Dev of Q4 ' + k + ' Percentage of Listing Price Received'] = fyq4_polpr.std(axis=1)
                    fyq4_polpr['Min of Q4 ' + k + ' Percentage of Listing Price Received'] = fyq4_polpr.min(axis=1)
                    fyq4_polpr['Max of Q4 ' + k + ' Percentage of Listing Price Received'] = fyq4_polpr.max(axis=1)
                    fyq4_polpr = fyq4_polpr[
                        [column for column in fyq4_polpr.columns if ['Oct', 'Nov', 'Dec'] not in column if
                         k in column]]
                    fyq4_inventory = df[
                        [column for column in df.columns if 'Inventory of Homes for Sale' in column if k in column]]
                    fyq4_inventory['Mean of Q4 ' + k + ' Inventory of Homes'] = fyq4_inventory.mean(axis=1)
                    fyq4_inventory['Median of Q4 ' + k + ' Inventory of Homes'] = fyq4_inventory.median(axis=1)
                    fyq4_inventory['Std Dev of Q4 ' + k + ' Inventory of Homes'] = fyq4_inventory.std(axis=1)
                    fyq4_inventory['Min of Q4 ' + k + ' Inventory of Homes'] = fyq4_inventory.min(axis=1)
                    fyq4_inventory['Max of Q4 ' + k + ' Inventory of Homes'] = fyq4_inventory.max(axis=1)
                    fyq4_inventory = fyq4_inventory[
                        [column for column in fyq4_inventory.columns if ['Oct', 'Nov', 'Dec'] not in column if
                         k in column]]
                    fyq4_supply = df[
                        [column for column in df.columns if 'Months Supply of Inventory' in column if k in column]]
                    fyq4_supply['Mean of Q4 ' + k + ' Monthly Supply'] = fyq4_supply.mean(axis=1)
                    fyq4_supply['Median of Q4 ' + k + ' Monthly Supply'] = fyq4_supply.median(axis=1)
                    fyq4_supply['Std Dev of Q4 ' + k + ' Monthly Supply'] = fyq4_supply.std(axis=1)
                    fyq4_supply['Min of Q4 ' + k + ' Monthly Supply'] = fyq4_supply.min(axis=1)
                    fyq4_supply['Max of Q4 ' + k + ' Monthly Supply'] = fyq4_supply.max(axis=1)
                    fyq4_supply = fyq4_supply[
                        [column for column in fyq4_supply.columns if ['Oct', 'Nov', 'Dec'] not in column if
                         k in column]]
                    new_dict[k]['Q4'] = [fyq4_new_listings, fyq4_closed_sales, fyq4_dom, fyq4_median_sales, fyq4_polpr,
                                         fyq4_inventory, fyq4_supply]



        return self.quarterly_merge(new_dict)

    def CreateZip(self):

        logger = logging.getLogger("Create Zip")
        logger.setLevel(logging.DEBUG)
        # Create the FileHandler() and StreamHandler() loggers
        f_handler = logging.FileHandler('CreateZip ' + str(datetime.datetime.today().date()) + '.log')
        f_handler.setLevel(logging.DEBUG)
        c_handler = logging.StreamHandler()
        c_handler.setLevel(logging.INFO)
        # Create formatting for the loggers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
        # Set the formatter for each handler
        f_handler.setFormatter(formatter)
        c_handler.setFormatter(formatter)
        logger.addHandler(f_handler)
        logger.addHandler(c_handler)

        os.chdir('C:\\Users\\Omar\\Desktop\\Python Temp Folder\\PDF Temp Files')
        zip_folder = 'F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\NJR10k Zips'
        folder = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder\\PDF Temp Files'
        newZip = zipfile.Zipfile('NJR10k ' + str(datetime.datetime.now()) + '.zip', 'w')
        for root, dirs, filenames in os.walk(folder):
            if len(filesnames) > 0:
                # Insert logger here stating 'Zipping {Year_Number} Folder'
                newZip.write(root)
                for filename in filenames:
                    # # Insert logger here stating 'Zipping {Filename}'
                    newZip.write(filename)

        newZip.close()
        # Move created zipfile to storage folder
        shutil.move(newZip, zip_folder)
        # Check if the PDF Temp Files folder in the Python Temp Folder still exists. If not create it again
        if os.path.exists(folder):
            pass
        else:
            os.mkdir('PDF Temp Files')

    # Function which scrapes the current month of data available
    def current_data_avail(self, soup):
        results = soup.find('select', id="lmuTime").children
        results = list(results)
        month_year = results[2].get_text()
        target = month_year.split(' ')
        year = target[1]
        month = target[0]
        if year not in self.__years:
            self.__years.append(year)
            main_dictionary[year] = {}

        Scraper.current_data_avail = month_year

    # Creates the initial dictionary the scraped city data will be stored
    def data_na(self, town, month, year):

        month = month[0:3]
        current_year = year
        city = town
        county = 'N.A'
        previous_year = str(int(year) - 1)
        new_listings_current = 0
        new_listings_previous = 0
        new_listings_per_change = 0.0
        closed_sales_current = 0
        closed_sales_previous = 0
        closed_sales_per_change = 0.0
        DOM_current = 0
        DOM_previous = 0
        DOM_per_change = 0.0
        median_sales_current = 0
        median_sales_previous = 0
        median_sales_per_change = 0.0
        percent_lpr_current = 0.0
        percent_lpr_previous = 0.0
        percent_lpr_per_change = 0.0
        inventory_current = 0
        inventory_previous = 0
        inventory_per_change = 0.0
        supply_current = 0.0
        supply_previous = 0.0
        supply_per_change = 0.0

        if main_dictionary[current_year] == {}:
            # self.create_dictionary(month, current_year)
            main_dictionary[current_year].setdefault('City', [])
            main_dictionary[current_year].setdefault('County', [])
            main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('New Listing % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Closed Sale % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault(
                'Percent of Listing Price Received ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault(
                'Percent of Listing Price Received ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault(
                'Percent of Listing Price Receive % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sales ' + month + ' ' + previous_year,
                                                     [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sales ' + month + ' ' + current_year,
                                                     [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Months of Supplies % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year]['City'].append(city)
            main_dictionary[current_year]['County'].append(county)
            main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(
                new_listings_previous)
            main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(new_listings_current)
            main_dictionary[current_year]['New Listing % Change ' + month + ' (YoY)'].append(
                new_listings_per_change)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(
                closed_sales_previous)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(closed_sales_current)
            main_dictionary[current_year]['Closed Sale % Change ' + month + ' (YoY)'].append(
                closed_sales_per_change)
            main_dictionary[current_year]['Days on Markets ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Days on Markets ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
            main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + previous_year].append(
                median_sales_previous)
            main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + current_year].append(
                median_sales_current)
            main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(
                median_sales_per_change)
            main_dictionary[current_year][
                'Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
            main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + current_year].append(
                percent_lpr_current)
            main_dictionary[current_year]['Percent of Listing Price Receive % Change ' + month + ' (YoY)'].append(
                percent_lpr_per_change)
            main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + previous_year].append(
                inventory_previous)
            main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + current_year].append(
                inventory_current)
            main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(
                inventory_per_change)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Months of Supplies % Change ' + month + ' (YoY)'].append(DOM_per_change)

        # For each city inserted into the dictionary, a new data column needs to be created for each month
        # I'm tracking 21 data columns. So for a every city created, 21 new data columns are needed
        # Only 21 data columns need to be created if the city already exists in the dictionary. City would be appended 12x if not for this elif block
        elif main_dictionary[current_year] != {} and city in main_dictionary[current_year]['City']:

            main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('New Listing % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Closed Sale % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault(
                'Percent of Listing Price Received ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault(
                'Percent of Listing Price Received ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault(
                'Percent of Listing Price Receive % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sales ' + month + ' ' + previous_year,
                                                     [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sales ' + month + ' ' + current_year,
                                                     [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Months of Supplies % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(
                new_listings_previous)
            main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(new_listings_current)
            main_dictionary[current_year]['New Listing % Change ' + month + ' (YoY)'].append(
                new_listings_per_change)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(
                closed_sales_previous)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(closed_sales_current)
            main_dictionary[current_year]['Closed Sale % Change ' + month + ' (YoY)'].append(
                closed_sales_per_change)
            main_dictionary[current_year]['Days on Markets ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Days on Markets ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
            main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + previous_year].append(
                median_sales_previous)
            main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + current_year].append(
                median_sales_current)
            main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(
                median_sales_per_change)
            main_dictionary[current_year][
                'Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
            main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + current_year].append(
                percent_lpr_current)
            main_dictionary[current_year]['Percent of Listing Price Receive % Change ' + month + ' (YoY)'].append(
                percent_lpr_per_change)
            main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + previous_year].append(
                inventory_previous)
            main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + current_year].append(
                inventory_current)
            main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(
                inventory_per_change)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Months of Supplies % Change ' + month + ' (YoY)'].append(DOM_per_change)

        # For each city inserted into the dictionary, a new data column needs to be created for each month
        # I'm tracking 21 data columns. So for a every city created, 21 new data columns are needed
        # The city and county need to be appended to the existing list & 21 new data columns created
        elif main_dictionary[current_year] != {} and city not in main_dictionary[current_year]['City']:
            main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('New Listing % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Closed Sale % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault(
                'Percent of Listing Price Received ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault(
                'Percent of Listing Price Received ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault(
                'Percent of Listing Price Receive % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sales ' + month + ' ' + previous_year,
                                                     [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sales ' + month + ' ' + current_year,
                                                     [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Months of Supplies % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year]['City'].append(city)
            main_dictionary[current_year]['County'].append(county)
            main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(
                new_listings_previous)
            main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(new_listings_current)
            main_dictionary[current_year]['New Listing % Change ' + month + ' (YoY)'].append(
                new_listings_per_change)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(
                closed_sales_previous)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(closed_sales_current)
            main_dictionary[current_year]['Closed Sale % Change ' + month + ' (YoY)'].append(
                closed_sales_per_change)
            main_dictionary[current_year]['Days on Markets ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Days on Markets ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
            main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + previous_year].append(
                median_sales_previous)
            main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + current_year].append(
                median_sales_current)
            main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(
                median_sales_per_change)
            main_dictionary[current_year][
                'Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
            main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + current_year].append(
                percent_lpr_current)
            main_dictionary[current_year]['Percent of Listing Price Receive % Change ' + month + ' (YoY)'].append(
                percent_lpr_per_change)
            main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + previous_year].append(
                inventory_previous)
            main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + current_year].append(
                inventory_current)
            main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(
                inventory_per_change)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Months of Supplies % Change ' + month + ' (YoY)'].append(DOM_per_change)

    def data2pandas(self, dict):
        # Use each key in the main_dictionary to represent the new key in the pandas_dict
        # I dont know how to create dynamic names so this will have to do
        pandas_dict = {}
        for k, v in dict.items():
            df = pd.DataFrame(v)
            pandas_dict[k] = df.set_index('City')

        return pandas_dict

    # Function which calculates the difference between the current download date and previous date
    # Use this to calculate the average amount of time it takes between new update periods
    def daysuntilupdate(self, no_of_runs):
        current = Scraper.no_of_runs
        previous = current - 1
        current_date = datetime.datetime.now()
        previous_date = datetime.datetime.strptime(Scraper.event_log[previous]['Run Date'], "%a %b %d %H:%M:%S %Y")
        delta = current_date - previous_date

        return delta.days

    # Function which extracts the month, current and previous year, new listing, closing sales, DOM, median sales, etc
    # Data will then be stored in a dictionary
    def extract_re_data(self, pdfname, possible_corrupted_list):

        os.chdir('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
        #logging.basicConfig(level=logging.ERROR)
        logger = logging.getLogger("Extract_Data")
        logger.propagate = False
        logger.setLevel(logging.DEBUG)
        # Create the FileHandler() and StreamHandler() loggers
        f_handler = logging.FileHandler('Extract_Data ' + str(datetime.datetime.today().date()) + '.log')
        f_handler.setLevel(logging.DEBUG)
        c_handler = logging.StreamHandler()
        c_handler.setLevel(logging.INFO)
        # Create formatting for the loggers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')
        # Set the formatter for each handler
        f_handler.setFormatter(formatter)
        c_handler.setFormatter(formatter)
        logger.addHandler(f_handler)
        logger.addHandler(c_handler)

        pdfname = pdfname
        logger.info(f'The data for {pdfname} is now being extracted')
        # Information will be used in data_na function
        info = pdfname.rstrip('.pdf').split(' ')
        town = info[0:len(info) - 2]
        town = ' '.join(town)
        month = info[-2]
        year = info[-1]

        if pdfname in possible_corrupted_list:
            self.data_na(town, month, year)
        else:
            try:
                with open(pdfname, 'rb') as reader:
                    pdfread = PyPDF2.PdfReader(reader)
                    page = pdfread.pages[0]
                    target = page.extract_text()

                for i in self.__towns:
                    if i in target:
                        city = i
                        break

                for c in self.__counties:
                    if c in target:
                        county = c
                        break

                month_pattern = re.compile(
                    r'(January|February|March|April|May|June|July|August|September|October|November|December)\sYear\sto\sDate\sSingle\sFamily')
                month = month_pattern.search(target)
                month = month.group()[0:3]
                key_metrics_basic_pattern = re.compile(
                    r'Key\sMetrics\s(\d{4})\s(\d{4})\sPercent\sChange\sThru\s\d{1,2}?-\d{4}\sThru\s\d{1,2}?-\d{4}\sPercent\sChange')
                km_search = list(key_metrics_basic_pattern.findall(target))
                current_year = km_search[0][1]
                previous_year = str(int(current_year) - 1)
                new_listings_pattern = re.compile(
                    r'New\sListings\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)')
                new_listing_search = list(new_listings_pattern.findall(target))
                new_listings_current = int(new_listing_search[0][1])
                new_listings_previous = int(new_listing_search[0][0])
                new_listings_pc = str(new_listing_search[0][2]).split(' ')
                new_listings_per_change = ''.join(new_listings_pc).rstrip('%')
                if '+' in new_listings_per_change:
                    new_listings_per_change.lstrip('+')
                    new_listings_per_change = round(float(new_listings_per_change) / 100, 3)
                elif '--' in new_listings_per_change:
                    new_listings_per_change = 0.0
                else:
                    new_listings_per_change = round(float(new_listings_per_change) / 100, 3)
                closed_sales_pattern = re.compile(
                    r'Closed\sSales\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)')
                closed_sales_search = list(closed_sales_pattern.findall(target))
                closed_sales_current = int(closed_sales_search[0][1])
                closed_sales_previous = int(closed_sales_search[0][0])
                closed_sales_pc = closed_sales_search[0][2].split(' ')
                closed_sales_per_change = ''.join(closed_sales_pc).rstrip('%')
                if '+' in closed_sales_per_change:
                    closed_sales_per_change.lstrip('+')
                    closed_sales_per_change = round(float(closed_sales_per_change) / 100, 3)
                elif '--' in closed_sales_per_change:
                    closed_sales_per_change = 0.0
                else:
                    closed_sales_per_change = round(float(closed_sales_per_change) / 100, 3)
                DOM_pattern = re.compile(
                    r'Days\son\sMarket\sUntil\sSale\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)')
                DOM_search = list(DOM_pattern.findall(target))
                DOM_current = int(DOM_search[0][1])
                DOM_previous = int(DOM_search[0][0])
                DOM_pc = DOM_search[0][2].split(' ')
                DOM_per_change = ''.join(DOM_pc).rstrip('%')
                if '+' in DOM_per_change:
                    DOM_per_change.lstrip('+')
                    DOM_per_change = round(float(DOM_per_change) / 100, 3)
                elif '--' in DOM_per_change:
                    DOM_per_change = 0.0
                else:
                    DOM_per_change = round(float(DOM_per_change) / 100, 3)
                median_sales_pattern = re.compile(
                    r'Median\sSales\sPrice\*\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(--|[+-]\s\d{1,3}?.\d{1}%)\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(--|[+-]\s\d{1,3}?.\d{1}%)')
                median_sales_search = list(median_sales_pattern.findall(target))
                median_sales_current = median_sales_search[0][1]
                median_sales_current = int("".join(median_sales_current.split(',')).lstrip('$'))
                median_sales_previous = median_sales_search[0][0]
                median_sales_previous = int("".join(median_sales_previous.split(',')).lstrip('$'))
                median_sales_pc = median_sales_search[0][2].split(' ')
                median_sales_per_change = ''.join(median_sales_pc).rstrip('%')
                if '+' in median_sales_per_change:
                    median_sales_per_change.lstrip('+')
                    median_sales_per_change = round(float(median_sales_per_change) / 100, 3)
                elif '--' in median_sales_per_change:
                    median_sales_per_change = 0.0
                else:
                    median_sales_per_change = round(float(median_sales_per_change) / 100, 3)
                percent_lpr_pattern = re.compile(
                    r'Percent\sof\sList\sPrice\sReceived\*\s(\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(--|[+-]\s\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(--|[+-]\s\d{1,3}?.\d{1}%)')
                percent_lpr_search = list(percent_lpr_pattern.findall(target))
                # Divide this by 100 and figure out how to format these to show the percent sign
                percent_lpr_current = float(percent_lpr_search[0][1].rstrip('%'))
                percent_lpr_previous = float(percent_lpr_search[0][0].rstrip('%'))
                percent_lpr_pc = percent_lpr_search[0][2].split(' ')
                percent_lpr_per_change = ''.join(percent_lpr_pc).rstrip('%')
                if '+' in percent_lpr_per_change:
                    percent_lpr_per_change.lstrip('+')
                    percent_lpr_per_change = round(float(percent_lpr_per_change) / 100, 3)
                elif '--' in percent_lpr_per_change:
                    percent_lpr_per_change = 0.0
                else:
                    percent_lpr_per_change = round(float(percent_lpr_per_change) / 100, 3)
                inventory_pattern = re.compile(
                    r'Inventory\sof\sHomes\sfor\sSale\s(--|\d{0,3}?)\s(--|\d{0,3}?)\s(--|[+-]\s\d{1,3}?.\d{1}%)\s(--|\d{0,3}?)\s(--|\d{0,3}?)\s(--|[+-]\s\d{1,3}?.\d{1}%)')
                inventory_search = list(inventory_pattern.findall(target))
                inventory_current = int(inventory_search[0][1])
                inventory_previous = int(inventory_search[0][0])
                inventory_pc = inventory_search[0][2].split(' ')
                inventory_per_change = ''.join(inventory_pc).rstrip('%')
                if '+' in inventory_per_change:
                    inventory_per_change.lstrip('+')
                    inventory_per_change = round(float(inventory_per_change) / 100, 3)
                elif '--' in inventory_per_change:
                    inventory_per_change = 0.0
                else:
                    inventory_per_change = round(float(inventory_per_change) / 100, 3)
                supply_pattern = re.compile(
                    r'Months\sSupply\sof\sInventory\s(--|\d{1,2}?.\d{1})\s(--|\d{1,2}?.\d{1})\s(--|[+-]\s\d{1,3}?.\d{1}%)\s(--|\d{1,2}?.\d{1})\s(--|\d{1,2}?.\d{1})\s(--|[+-]\s\d{1,3}?.\d{1}%)')
                supply_search = list(supply_pattern.findall(target))
                supply_current = float(supply_search[0][1])
                supply_previous = float(supply_search[0][0])
                supply_pc = supply_search[0][2].split(' ')
                supply_per_change = ''.join(supply_pc).rstrip('%')
                if '+' in supply_per_change:
                    supply_per_change.lstrip('+')
                    supply_per_change = round(float(supply_per_change) / 100, 3)
                elif '--' in supply_per_change:
                    supply_per_change = 0.0
                else:
                    supply_per_change = round(float(supply_per_change) / 100, 3)

                if main_dictionary[current_year] == {}:
                    # self.create_dictionary(month, current_year)
                    main_dictionary[current_year].setdefault('City', [])
                    main_dictionary[current_year].setdefault('County', [])
                    main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('New Listing % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Closed Sale % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault(
                        'Percent of Listing Price Received ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault(
                        'Percent of Listing Price Received ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault(
                        'Percent of Listing Price Receive % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault(
                        'Inventory of Homes for Sales ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault(
                        'Inventory of Homes for Sales ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)',
                                                             [])
                    main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Months of Supplies % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year]['City'].append(city)
                    main_dictionary[current_year]['County'].append(county)
                    main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(
                        new_listings_previous)
                    main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(
                        new_listings_current)
                    main_dictionary[current_year]['New Listing % Change ' + month + ' (YoY)'].append(
                        new_listings_per_change)
                    main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(
                        closed_sales_previous)
                    main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(
                        closed_sales_current)
                    main_dictionary[current_year]['Closed Sale % Change ' + month + ' (YoY)'].append(
                        closed_sales_per_change)
                    main_dictionary[current_year]['Days on Markets ' + month + ' ' + previous_year].append(DOM_previous)
                    main_dictionary[current_year]['Days on Markets ' + month + ' ' + current_year].append(DOM_current)
                    main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
                    main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + previous_year].append(
                        median_sales_previous)
                    main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + current_year].append(
                        median_sales_current)
                    main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(
                        median_sales_per_change)
                    main_dictionary[current_year][
                        'Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
                    main_dictionary[current_year][
                        'Percent of Listing Price Received ' + month + ' ' + current_year].append(percent_lpr_current)
                    main_dictionary[current_year][
                        'Percent of Listing Price Receive % Change ' + month + ' (YoY)'].append(percent_lpr_per_change)
                    main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + previous_year].append(
                        inventory_previous)
                    main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + current_year].append(
                        inventory_current)
                    main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(
                        inventory_per_change)
                    main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(
                        DOM_previous)
                    main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
                    main_dictionary[current_year]['Months of Supplies % Change ' + month + ' (YoY)'].append(
                        DOM_per_change)

                # For each city inserted into the dictionary, a new data column needs to be created for each month
                # I'm tracking 21 data columns. So for a every city created, 21 new data columns are needed
                # Only 21 data columns need to be created if the city already exists in the dictionary. City would be appended 12x if not for this elif block
                elif main_dictionary[current_year] != {} and city in main_dictionary[current_year]['City']:

                    main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('New Listing % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Closed Sale % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault(
                        'Percent of Listing Price Received ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault(
                        'Percent of Listing Price Received ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault(
                        'Percent of Listing Price Receive % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault(
                        'Inventory of Homes for Sales ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault(
                        'Inventory of Homes for Sales ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)',
                                                             [])
                    main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Months of Supplies % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(
                        new_listings_previous)
                    main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(
                        new_listings_current)
                    main_dictionary[current_year]['New Listing % Change ' + month + ' (YoY)'].append(
                        new_listings_per_change)
                    main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(
                        closed_sales_previous)
                    main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(
                        closed_sales_current)
                    main_dictionary[current_year]['Closed Sale % Change ' + month + ' (YoY)'].append(
                        closed_sales_per_change)
                    main_dictionary[current_year]['Days on Markets ' + month + ' ' + previous_year].append(DOM_previous)
                    main_dictionary[current_year]['Days on Markets ' + month + ' ' + current_year].append(DOM_current)
                    main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
                    main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + previous_year].append(
                        median_sales_previous)
                    main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + current_year].append(
                        median_sales_current)
                    main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(
                        median_sales_per_change)
                    main_dictionary[current_year][
                        'Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
                    main_dictionary[current_year][
                        'Percent of Listing Price Received ' + month + ' ' + current_year].append(percent_lpr_current)
                    main_dictionary[current_year][
                        'Percent of Listing Price Receive % Change ' + month + ' (YoY)'].append(percent_lpr_per_change)
                    main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + previous_year].append(
                        inventory_previous)
                    main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + current_year].append(
                        inventory_current)
                    main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(
                        inventory_per_change)
                    main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(
                        DOM_previous)
                    main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
                    main_dictionary[current_year]['Months of Supplies % Change ' + month + ' (YoY)'].append(
                        DOM_per_change)

                # For each city inserted into the dictionary, a new data column needs to be created for each month
                # I'm tracking 21 data columns. So for a every city created, 21 new data columns are needed
                # The city and county need to be appended to the existing list & 21 new data columns created
                elif main_dictionary[current_year] != {} and city not in main_dictionary[current_year]['City']:
                    main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('New Listing % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Closed Sale % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Days on Markets ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Median Sales Prices ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault(
                        'Percent of Listing Price Received ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault(
                        'Percent of Listing Price Received ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault(
                        'Percent of Listing Price Receive % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year].setdefault(
                        'Inventory of Homes for Sales ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault(
                        'Inventory of Homes for Sales ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)',
                                                             [])
                    main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year, [])
                    main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year, [])
                    main_dictionary[current_year].setdefault('Months of Supplies % Change ' + month + ' (YoY)', [])
                    main_dictionary[current_year]['City'].append(city)
                    main_dictionary[current_year]['County'].append(county)
                    main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(
                        new_listings_previous)
                    main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(
                        new_listings_current)
                    main_dictionary[current_year]['New Listing % Change ' + month + ' (YoY)'].append(
                        new_listings_per_change)
                    main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(
                        closed_sales_previous)
                    main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(
                        closed_sales_current)
                    main_dictionary[current_year]['Closed Sale % Change ' + month + ' (YoY)'].append(
                        closed_sales_per_change)
                    main_dictionary[current_year]['Days on Markets ' + month + ' ' + previous_year].append(DOM_previous)
                    main_dictionary[current_year]['Days on Markets ' + month + ' ' + current_year].append(DOM_current)
                    main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
                    main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + previous_year].append(
                        median_sales_previous)
                    main_dictionary[current_year]['Median Sales Prices ' + month + ' ' + current_year].append(
                        median_sales_current)
                    main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(
                        median_sales_per_change)
                    main_dictionary[current_year][
                        'Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
                    main_dictionary[current_year][
                        'Percent of Listing Price Received ' + month + ' ' + current_year].append(percent_lpr_current)
                    main_dictionary[current_year][
                        'Percent of Listing Price Receive % Change ' + month + ' (YoY)'].append(percent_lpr_per_change)
                    main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + previous_year].append(
                        inventory_previous)
                    main_dictionary[current_year]['Inventory of Homes for Sales ' + month + ' ' + current_year].append(
                        inventory_current)
                    main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(
                        inventory_per_change)
                    main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(
                        DOM_previous)
                    main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
                    main_dictionary[current_year]['Months of Supplies % Change ' + month + ' (YoY)'].append(
                        DOM_per_change)

            except re.error as ree:
                logger.exception(f'A Regex Error Has Occurred: {ree}')

            except PyPDF2._reader.EmptyFileError as efe:
                logger.exception(f'An Error Has Occured (File Possibly Corrupted): {efe}')
                logger.info(f'The city of {town} for {month} {year} will have no data')
                # If function encounters an empty/ corrupted pdf, the data_na function will render all information available for that file equal to zero
                # The generator will then pick up at the next pdf to continue extracting data
                # self.data_na(town, month, year)
                # for pdf in self.pdf_generator(pdfname):
                #     self.extract_re_data(pdf)

            except:
                logger.exception(f'A Regex Error Has Occurred')

        #pprint.pprint(main_dictionary)


    # Function which pulls the username and password for a specified website
    def get_us_pw(self, website):
        #Saves the current directory in a variable in order to switch back to it once the program ends
        previous_wd = os.getcwd()
        os.chdir('F:\\Jibreel Hameed\\Kryptonite')
        wb = openpyxl.load_workbook('get_us_pw.xlsx')
        sheet = wb.active
        for i,n in enumerate(sheet['A0' : 'A20']):
            for cell in n:
                if website == cell.value:
                    username = sheet['C' + str(i+1)].value
                    pw = sheet['D' + str(i+1)].value


        os.chdir(previous_wd)

        return username, pw

    # Used in case the njr10k or the update_njr10k functions are used recursively. This function will find the latest file downloaded and continue from that point
    def latest_file(self):

        base_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder'

        for root, dirs, filenames in os.walk(base_path):
            # print(filenames)
            # If the length of the filenames variable is less than 2, that means there are no PDF Files in the folder. The 2 existing files are log files
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
                        print(f'Latest file downloaded is: {target}\n Restarting download process...')
                        info = target.rstrip('.pdf').split(' ')
                        town = info[0:len(info) - 2]
                        if len(town) > 1:
                            town = ' '.join(town)
                        else:
                            town = info[0]

                except IndexError:
                    # The code tried searching outside the list because it didnt find a PDF file
                    print(f'There are Currently No PDFs Available')
                    return 'None'
                else:
                    if target == 'Wyckoff Twp ' + Scraper.current_data_avail.split(' ')[0] + ' ' + Scraper.current_data_avail.split(' ')[1]:
                        return 'All Files Downloaded'
                    else:
                        full_town_list = self.__towns
                        return full_town_list[full_town_list.index(town):]

    # Return to this function when I figure out what I want to do
    # def merge_years(self, dict):
    #     """ The dict arguement will be a nested dictionary.
    #                             dict = {'2019' : [],
    #                                     '2020' : [],
    #                                     '2021' : [],
    #                                     '2022' : [],
    #                                     '2023' : []}"""
    #     cat_length = len(dict['2020'])
    #     years = dict.keys()
    #     for i in cat_length:
    #         for year in dict.keys():
    #             for value in dict


    # Function which logs into njrealtor to automatically download the pdfs from each city to get ready to scrape
    def njr10k(self):

        logger = logging.getLogger("NJR10k")
        logger.setLevel(logging.DEBUG)
        # Create the FileHandler() and StreamHandler() loggers
        f_handler = logging.FileHandler('NJR10k ' + str(datetime.datetime.today().date()) + '.log')
        f_handler.setLevel(logging.DEBUG)
        c_handler = logging.StreamHandler()
        c_handler.setLevel(logging.INFO)
        # Create formatting for the loggers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')
        # Set the formatter for each handler
        f_handler.setFormatter(formatter)
        c_handler.setFormatter(formatter)
        logger.addHandler(f_handler)
        logger.addHandler(c_handler)

        start_time = datetime.datetime.now()

        name = 'NJR10k'

        possible_corrupted_files = []

        # manager = enlighten.Manager()
        # city_tracker = manager.counter(total = len(self.__towns), desc = f'City', unit = 'Cities')
        # year_tracker = manager.counter(total = len(self.__years), desc = f'Year', unit = 'Years')


        base_url = 'http://njar.stats.10kresearch.com/docs/lmu/'

        # Initiates a Requests Session which maintains the cookies and session info until closed
        # This is needed in order to successfully log into njrealtor and access the 10k pdfs
        with requests.Session() as session:
            # create a function/module which returns the njr10k info
            username, pw = self.get_us_pw('NJRealtor')

            #payload sent during the HTTP POST
            payload1 = {'rd': '10',
                        'passedURL': '/goto/10k/',
                        'case': '',
                        'LoginEmail': username,
                        'LoginPassword': pw,
                        'LoginButton': 'Login'}

            # website parameters needs to access the correct pdf addresses
            params = {'src': 'Page'}
            months = list(self.__months.keys())

            # months_tracker = manager.counter(total=len(months), desc=f'Year:', unit='Months')

            url = 'https://www.njrealtor.com/login/?rd=10&passedURL=/goto/10k/'
            url2 = 'https://www.njrealtor.com/ramco-api/web-services/login_POST.php'

            response = session.get(url)
            r_post = session.post(url2, data=payload1)

            # If this is a recursive run, towns_list will be a sliced list starting from the last run city
            # Latest_file searches for the last
            towns_list = self.latest_file()

            if towns_list == 'None':
                towns_list = self.__towns
            elif towns_list == 'All Files Downloaded':
                return 'All Files Downloaded'

            try:
                for i in towns_list:
                    # time.sleep(0.1)
                    # city_tracker.update()
                    # Takes the name of the city from the list and splits the string at the space, then joins the strings in the newly created list
                    # This is needed to use in the url3 variable to access the correct 10k pdfs
                    city0 = i.split(' ')
                    city = ''.join(city0)
                    for y in self.__years:
                        # time.sleep(0.1)
                        # year_tracker.update()
                        if y == '2019':
                            # If 2019 is the year, there is no data available from January to Sept. And some cities dont have data available for Sept
                            months1 = months[8:13]
                            for m in months1:
                                # time.sleep(0.1)
                                # months_tracker.update()
                                if '/' not in city:
                                    url3 = base_url + y + '-' + m + '/x/' + city
                                    new_filename = " ".join([' '.join(city0), self.__months[m], y]) + ".pdf"

                                elif '/' in city:
                                    city = '%2F'.join(city.split('/'))
                                    del city0[city0.index('/')]
                                    url3 = base_url + y + '-' + m + '/x/' + city
                                    new_filename = " ".join([' '.join(city0), self.__months[m], y]) + ".pdf"

                                with session.get(url3, params=params, stream = True) as reader, open(new_filename, 'wb') as writer:
                                    for chunk in reader.iter_content(chunk_size=1000000):
                                        # Casting the bytes into a str type and slicing the first 20 characters to check if 'PDF' is in
                                        check_pdf = str(chunk)[:20]
                                        #print(check_pdf)
                                        if 'PDF' in check_pdf:
                                            writer.write(chunk)
                                        else:
                                            logger.warning(f'WARNING! {new_filename} is possibly a corrupted file')
                                            possible_corrupted_files.append(new_filename)

                        elif y == self.__years[-1]:
                            # If year is the latest year, months1 will equal a sliced list of the first month represented by 01 to the latests month represent by 2 digits
                            for k, v in self.__months.items():
                                # If v equals the the month of the most current data
                                if v == Scraper.current_data_avail.split(' ')[0]:
                                    months1 = months[:months.index(k) + 1]
                            for m in months1:
                                # time.sleep(0.1)
                                # months_tracker.update()
                                if '/' not in city:
                                    url3 = base_url + y + '-' + m + '/x/' + city
                                    new_filename = " ".join([' '.join(city0), self.__months[m], y]) + ".pdf"

                                elif '/' in city:
                                    city = '%2F'.join(city.split('/'))
                                    del city0[city0.index('/')]
                                    url3 = base_url + y + '-' + m + '/x/' + city
                                    new_filename = " ".join([' '.join(city0), self.__months[m], y]) + ".pdf"

                                with session.get(url3, params=params, stream = True) as reader, open(new_filename, 'wb') as writer:
                                    for chunk in reader.iter_content(chunk_size=1000000):
                                        # Casting the bytes into a str type and slicing the first 20 characters to check if 'PDF' is in
                                        check_pdf = str(chunk)[:20]
                                        #print(check_pdf)
                                        if 'PDF' in check_pdf:
                                            writer.write(chunk)
                                        else:
                                            logger.warning(f'WARNING! {new_filename} is possibly a corrupted file')
                                            possible_corrupted_files.append(new_filename)

                        elif y != '2019':
                            for m in months:
                                # time.sleep(0.1)
                                # months_tracker.update()
                                if '/' not in city:
                                    url3 = base_url + y + '-' + m + '/x/' + city
                                    new_filename = " ".join([' '.join(city0), self.__months[m], y]) + ".pdf"

                                elif '/' in city:
                                    city = '%2F'.join(city.split('/'))
                                    del city0[city0.index('/')]
                                    url3 = base_url + y + '-' + m + '/x/' + city
                                    new_filename = " ".join([' '.join(city0), self.__months[m], y]) + ".pdf"

                                with session.get(url3, params=params, stream = True) as reader, open(new_filename, 'wb') as writer:
                                    for chunk in reader.iter_content(chunk_size=1000000):
                                        # Casting the bytes into a str type and slicing the first 20 characters to check if 'PDF' is in
                                        check_pdf = str(chunk)[:20]
                                        #print(check_pdf)
                                        if 'PDF' in check_pdf:
                                            writer.write(chunk)
                                        else:
                                            logger.warning(f'WARNING! {new_filename} is possibly a corrupted file')
                                            possible_corrupted_files.append(new_filename)

            except IOError:
                """An OS Error has occurred """
                logger.exception(f'IOError has Occurred')
                self.njr10k()

            except requests.exceptions.HTTPError as rht:
                """An HTTP error occurred."""
                logger.exception(f'An HTTP has Occurred: {rht}')

            except requests.exceptions.Timeout as ret:
                """The request timed out.
    
                Catching this error will catch both
                :exc:`~requests.exceptions.ConnectTimeout` and
                :exc:`~requests.exceptions.ReadTimeout` errors.
                """
                logger.exception(f'The Request Has Timed Out: {ret}')

            except requests.exceptions.InvalidURL as rei:
                """The URL provided was somehow invalid."""
                logger.exception(f'The URL Provided Was Invalid: {rei}')

            except requests.exceptions.RetryError as rer:
                    """Custom retries logic failed"""
                    logger.exception(f'Custom Retries Logic Failed: {rer}')

            except requests.exceptions.StreamConsumedError as res:
                """The content for this response was already consumed."""
                logger.exception(f'The Content For This Response Was Already Consumed: {res}')

            except requests.exceptions.ContentDecodingError as rec:
                """Failed to decode response content."""
                logger.exception(f'Failed to Decode Response Content: {rec}')

            except requests.exceptions.ChunkedEncodingError as rece:
                """The server declared chunked encoding but sent an invalid chunk."""
                logger.exception(f'Invalid Chunk Encoding: {rece}')

            except:
                logger.exception('An Error Has Occurred: Unhandled')

            else:

                end_time = datetime.datetime.now()
                run_time = end_time - start_time

                if Scraper.no_of_runs == 0:
                    Scraper.event_log[Scraper.no_of_runs] = {'Run Type' : name,
                                                             'Latest Available Data' : self.current_data_avail,
                                                             'Run Time' : str(run_time),
                                                             'Run Date' : time.ctime(),
                                                             'Days Between Update' : 0 }
                    logger.info(f'New Event Log Created: \n{Scraper.event_log}')
                elif Scraper.no_of_runs > 0:
                    Scraper.event_log[Scraper.no_of_runs] = {'Run Type' : name,
                                                             'Latest Available Data': self.current_data_avail,
                                                             'Run Time' : str(run_time),
                                                             'Run Date': time.ctime(),
                                                             'Days Between Update': self.daysuntilupdate(Scraper.no_of_runs)}
                    logger.info(f'New Event Log Created: \n{Scraper.event_log}')
                Scraper.no_of_runs += 1
                #This is a very lengthy program so I'd like play a sound that signifies the process is done
                winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)

        return possible_corrupted_files

    # Function uses Selenium to webscrape the cities and counties from the njrealtor 10k website
    def njrdata(self):

        logger = logging.getLogger("NJRData")
        logger.setLevel(logging.DEBUG)
        # Create the FileHandler() and StreamHandler() loggers
        f_handler = logging.FileHandler('NJRData ' + str(datetime.datetime.today().date()) + '.log')
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

        options = Options()
        # Change this directory to the new one: ('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
        s = {"savefile.default_directory": 'C:\\Users\\Omar\\Desktop\\Selenium Temp Folder'}
        # options.add_argument('window-postion=2000,0')
        # options.add_experimental_option("detach", True)
        options.add_experimental_option("prefs", s)
        options.add_argument("--headless=new")
        #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver = webdriver.Chrome(service=Service(ChromeDriverManager(version='114.0.5735.90').install()), options = options)
        url = 'https://www.njrealtor.com/login.php?rd=10&passedURL=/goto.php?10kresearch=1&skipToken=1'
        driver.get(url)

        username, pw = self.get_us_pw('NJRealtor')

        try:
            # Login in using my email and password
            email = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//input[@id='LoginEmail']"))
            )
            email.send_keys(username)
            pw1 = driver.find_element(By.XPATH, "//input[@id='LoginPassword']")
            pw1.send_keys(pw)
            login = driver.find_element(By.XPATH, "//input[@id='LoginButton']")
            login.click()

            # Recognize the page element to know its time to webscrape all the cities and counties
            brand = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//img[@class='brand']"))
            )
            results = driver.page_source
            soup = BeautifulSoup(results, 'html.parser')
            self.area_results(soup)
            self.current_data_avail(soup)

        except TimeoutException as te:
            logger.exception(f'Timeout Error Occurred: {te}')

        except NoSuchElementException as nse:
            logger.exception(f'So Such Element Was Found: {nse}')

        except ElementNotVisibleException as env:
            logger.exception(f'The Element Is Not Visible: {env}')

        except:
            logger.exception('An Error Has Occured: Unhandled')

        else:
            logger.info(f'BeautifulSoup Has Run for NJ Realtor Successfully')

    # Function which will organize the PDF Temp Files Folder by year, month and city
    def OrganizeFiles(self, list):

        base_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder\\PDF Temp Files'
        target_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder'

        for root, dirs, filenames in os.walk(target_path):
            # If the first index of the list is 'No Corrupted Files', the send2trash function isnt necessary
            if list[0] == 'No Corrupted Files':
                for filename in filenames:
                    target = filename.rstrip('.pdf').split(' ')
                    year = target[-1]
                    month = target[-2]
                    city = ' '.join(target[:len(target) - 2])

                    first_path = base_path + '\\' + year
                    second_path = first_path + '\\' + month
                    third_path = second_path + '\\' + city
                    # If the first target path exists, check to see if the month and city paths exists as well
                    if os.path.exists(first_path):

                        if os.path.exists(second_path):

                            if os.path.exists(third_path):
                                shutil.move(filename, third_path)

                            elif os.path.exists(third_path) == False:
                                os.makedirs(year + '\\' + month + '\\' + city)
                                shutil.move(filename, third_path)

                        elif os.path.exists(second_path) == False:
                            os.makedirs(year + '\\' + month + '\\' + city)
                            shutil.move(filename, third_path)

                    elif os.path.exists(first_path) == False:
                        os.makedirs(year + '\\' + month + '\\' + city)
                        shutil.move(filename, third_path)
            else:
                for filename in filenames:
                    # If the filename is in the List, it is labeled corrupted and sent to the Trash Bin
                    if filename in list:
                        send2trash(filename)
                    else:
                        target = filename.rstrip('.pdf').split(' ')
                        year = target[-1]
                        month = target[-2]
                        city = ' '.join(target[:len(target) - 2])

                        first_path = base_path + '\\' + year
                        second_path = first_path + '\\' + month
                        third_path = second_path + '\\' + city
                        # If the first target path exists, check to see if the month and city paths exists as well
                        if os.path.exists(first_path):

                            if os.path.exists(second_path):

                                if os.path.exists(third_path):
                                    shutil.move(filename, third_path)

                                elif os.path.exists(third_path) == False:
                                    os.makedirs(year + '\\' + month + '\\' + city)
                                    shutil.move(filename, third_path)

                            elif os.path.exists(second_path) == False:
                                os.makedirs(year + '\\' + month + '\\' + city)
                                shutil.move(filename, third_path)

                        elif os.path.exists(first_path) == False:
                            os.makedirs(year + '\\' + month + '\\' + city)
                            shutil.move(filename, third_path)

    def pandas2excel(self, dict):
        """ The dict arguement will be a nested dictionary.
          dict = {'2019' : [new_listings_allqtrs, new_listings_fy, closed_sales_allqtrs, closed_sales_fy, dom_allqtrs, dom_fy, median_sales_allqtrs, etc ],
                  '2020' : [new_listings_allqtrs, new_listings_fy, closed_sales_allqtrs, closed_sales_fy, dom_allqtrs, dom_fy, median_sales_allqtrs, etc ],
                  '2021' : [new_listings_allqtrs, new_listings_fy, closed_sales_allqtrs, closed_sales_fy, dom_allqtrs, dom_fy, median_sales_allqtrs, etc ],
                  '2022' : [new_listings_allqtrs, new_listings_fy, closed_sales_allqtrs, closed_sales_fy, dom_allqtrs, dom_fy, median_sales_allqtrs, etc ],
                  '2023' : [new_listings_allqtrs, new_listings_fy, closed_sales_allqtrs, closed_sales_fy, dom_allqtrs, dom_fy, median_sales_allqtrs, etc ]}"""
        # Store the dfs in one Excel file under different sheets for later processing
        with pd.ExcelWriter(str(datetime.today().month) + ' ' + str(datetime.today().year) + ' NJ 10k Real Estate Data') as writer:
            for k, v in dict.items():
                v[0].to_excel(writer, sheet_name= k + ' New Listings By Qtr')
                v[1].to_excel(writer, sheet_name= k + ' New Listings FY')
                v[2].to_excel(writer, sheet_name= k + ' Closed Sales By Qtr')
                v[3].to_excel(writer, sheet_name= k + ' Closed Sales FY')
                v[4].to_excel(writer, sheet_name= k + ' Median Sales By Qtr')
                v[5].to_excel(writer, sheet_name= k + ' Median Sales FY')
                v[6].to_excel(writer, sheet_name= k + ' Percent of LP Received By Qtr')
                v[7].to_excel(writer, sheet_name= k + ' Percent of LP Received FY')
                v[8].to_excel(writer, sheet_name= k + ' Inventory By Qtr')
                v[9].to_excel(writer, sheet_name= k + ' Inventory FY')
                v[10].to_excel(writer, sheet_name= k + ' Monthly Supply By Qtr')
                v[11].to_excel(writer, sheet_name= k + ' Monthly Supply FY')

    # Generator function which will be used in tandem with the extractre_data function to put data into main dictionary
    def pdf_generator(self, pdfname=None):

        base_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder'
        # Variable pdfname will either be a string arguement or None
        pdfname = pdfname
        for root, dirs, filenames in os.walk(base_path):
            if pdfname is None:
                # If pdfname is None, the generator starts at the beginning of the list
                for filename in filenames:
                    if filename.endswith('.pdf'):
                        yield filename
                    else:
                        continue
            elif pdfname.endswith('.pdf'):
                # If pdfname is a string and ends with '.pdf', an error occurred in the extract_re_data function
                # The program will truncate the filesnames list starting with the first file name after the error
                filenames = filenames[filenames.index(pdfname) + 1:]
                for filename in filenames:
                    if filename.endswith('.pdf'):
                        yield filename
                    else:
                        continue

    def quarterly_yearly(self, dict):
        # Create new dfs where the full years data is broken up into 4 quarters
        new_dict = {}
        for k, v in dict.items():
            new_dict.setdefault(k, [])
            q1_df = v[[column for mon in ['Jan', 'Feb', 'Mar'] for column in dict[k].keys() if mon in column if k in column]]
            q2_df = v[[column for mon in ['Apr', 'May', 'Jun'] for column in dict[k].keys() if mon in column if k in column]]
            q3_df = v[[column for mon in ['Jul', 'Aug', 'Sep'] for column in dict[k].keys() if mon in column if k in column]]
            q4_df = v[[column for mon in ['Oct', 'Nov', 'Dec'] for column in dict[k].keys() if mon in column if k in column]]
            fy_df = v[[column for column in dict[k].keys() if k in column]]
            # This new dictionary will hold a list of the quarterly dfs and the full year df. 5 data frames in total
            new_dict[k] = [q1_df, q2_df, q3_df, q4_df, fy_df]

        return new_dict

    # Merges all of the individual quarterly dfs into one df for each category
    def quarterly_merge(self, dict):
        # Join all respective categories at the FY version of that dataframe
        # Isolate the Year key in the dictionary
        list = ['Q1', 'Q2', 'Q3', 'Q4', 'FY']
        year_dict = {}
        for year in dict.keys():
            year_dict.setdefault(year, [])
            # Reduce the indent here
            for i in range(len(fy_list)):
                q1_df = dict[year][list[0]][i]
                q2_df = dict[year][list[1]][i]
                q3_df = dict[year][list[2]][i]
                q4_df = dict[year][list[3]][i]
                fy_df = dict[year][list[-1]][i]
        # make sure all the indexes are set to 'City'
        # Indent this so its under the last for loop
                if i == 0:
                    new_listings_allqtrs = q1_df.join([q2_df, q3_df, q4_df])
                    new_listings_fy = fy_df
                    year_dict[year].append(new_listings_allqtrs)
                    year_dict[year].append(new_listings_fy)
                if i == 1:
                    closed_sales_allqtrs = q1_df.join([q2_df, q3_df, q4_df])
                    closed_sales_fy = fy_df
                    year_dict[year].append(closed_sales_allqtrs)
                    year_dict[year].append(closed_sales_fy)
                if i == 2:
                    dom_allqtrs = q1_df.join([q2_df, q3_df, q4_df])
                    dom_fy = fy_df
                    year_dict[year].append(dom_allqtrs)
                    year_dict[year].append(dom_fy)
                if i == 3:
                    median_sales_allqtrs = q1_df.join([q2_df, q3_df, q4_df])
                    median_sales_fy = fy_df
                    year_dict[year].append(median_sales_allqtrs)
                    year_dict[year].append(median_sales_fy)
                if i == 4:
                    polpr_allqtrs = q1_df.join([q2_df, q3_df, q4_df])
                    polpr_fy = fy_df
                    year_dict[year].append(polpr_allqtrs)
                    year_dict[year].append(polpr_fy)
                if i == 5:
                    inventory_allqtrs = q1_df.join([q2_df, q3_df, q4_df])
                    inventory_fy = fy_df
                    year_dict[year].append(inventory_allqtrs)
                    year_dict[year].append(inventory_fy)
                if i == 6:
                    supply_allqtrs = q1_df.join([q2_df, q3_df, q4_df])
                    supply_fy = fy_df
                    year_dict[year].append(supply_allqtrs)
                    year_dict[year].append(supply_fy)

        return year_dict

    # A function I can use for debuggin purposes. In the event a failure occurs before reaching the extract_re_data function
    # I can read the logger file and start again as a midway point
    def read_logger(self):

        base_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder'
        corrupt_pattern = re.compile(r'\d+-\w+-\d+\s\d+:\d+:\d+\s-\sNJR10k\s-\sWARNING\s-\sWARNING!\s(.*.pdf)\sis\spossibly\sa\scorrupted\sfile')
        ev_pattern = re.compile(r"{(\d{1,4}?):\s\{'Run Type':\s'(\w+)',\s'Latest Available Data':\s'(\w+\s\d{4})',\s'Run Time':\s(.*),\s'Run Date':\s(.*),\s'Days Between Update':\s(\d{1,4}?)}}")
        possible_corrupted_files = []

        for root, dirs, filenames in os.walk(base_path):

            if len(filenames) < 1:
                return 'None'
            else:
                i = -1
                while not filenames[i].endswith('.log'):
                    i -= 1
                else:
                    file_pattern = re.compile(r'NJR10k\s\d{4}-\d{2}-\d{2}.log')

            while not file_pattern.search(filenames[i]):
                i -= 1
            else:
                file = filenames[i]
                break

        with open('C:\\Users\\Omar\\Desktop\\Python Temp Folder\\' + file, 'r') as reader:
            target = reader.readlines()
            for i in target:
                if corrupt_pattern.search(i):
                    possible_corrupted_files.append(corrupt_pattern.search(i).group(1))
                elif ev_pattern.search(i):
                    Scraper.event_log.setdefault(int(ev_pattern.search(i).group(1)), {})
                    Scraper.event_log[int(ev_pattern.search(i).group(1))]['Run Type'] = ev_pattern.search(i).group(2)
                    Scraper.event_log[int(ev_pattern.search(i).group(1))]['Latest Available Data'] = ev_pattern.search(i).group(3)
                    Scraper.event_log[int(ev_pattern.search(i).group(1))]['Run Time'] = ev_pattern.search(i).group(4)
                    Scraper.event_log[int(ev_pattern.search(i).group(1))]['Run Date'] = ev_pattern.search(i).group(5)
                    Scraper.event_log[int(ev_pattern.search(i).group(1))]['Days Between Update'] = int(ev_pattern.search(i).group(6))
                    Scraper.no_of_runs += 1
                else:
                    continue

        return possible_corrupted_files

    # This function can only be run after njr10k runs for the first time
    # This function will compare the last key-value pair of the class event_log variable and the results of the current_data_avail function
    # If the values are not the same, run the program
    def update_njr10k(self, start, finish):

        logger = logging.getLogger("Update NJR10k")
        logger.setLevel(logging.DEBUG)
        # Create the FileHandler() and StreamHandler() loggers
        f_handler = logging.FileHandler('Update NJR10k ' + str(datetime.datetime.today().date()) + '.log')
        f_handler.setLevel(logging.DEBUG)
        c_handler = logging.StreamHandler()
        c_handler.setLevel(logging.INFO)
        # Create formatting for the loggers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
        # Set the formatter for each handler
        f_handler.setFormatter(formatter)
        c_handler.setFormatter(formatter)
        logger.addHandler(f_handler)
        logger.addHandler(c_handler)

        start_time = datetime.datetime.now()

        name = 'Update NJR10k'

        possible_corrupted_files = []

        # manager = enlighten.Manager()
        # city_tracker = manager.counter(total=len(self.__towns), desc=f'City', unit='Cities')
        # year_tracker = manager.counter(total=len(self.__years), desc=f'Year', unit='Years')

        base_url = 'http://njar.stats.10kresearch.com/docs/lmu/'

        # Initiates a Requests Session which maintains the cookies and session info until closed
        # This is needed in order to successfully log into njrealtor and access the 10k pdfs
        with requests.Session() as session:
            # create a function/module which returns the njr10k info
            username, pw = self.get_us_pw('NJRealtor')

            # payload sent during the HTTP POST
            payload1 = {'rd': '10',
                        'passedURL': '/goto/10k/',
                        'case': '',
                        'LoginEmail': username,
                        'LoginPassword': pw,
                        'LoginButton': 'Login'}

            # website parameters needs to access the correct pdf addresses
            params = {'src': 'Page'}
            months = list(self.__months.keys())

            # months_tracker = manager.counter(total=len(months), desc=f'Year:', unit='Months')

            url = 'https://www.njrealtor.com/login/?rd=10&passedURL=/goto/10k/'
            url2 = 'https://www.njrealtor.com/ramco-api/web-services/login_POST.php'

            response = session.get(url)
            r_post = session.post(url2, data=payload1)

            start_month = start[0]
            start_year = start[1]
            current_month = finish[0]
            current_year = finish[1]

            # Assures that I'll have the correct year range when I slice the self.__years list
            assert int(start_year) <= int(current_year), "Invalid Operation: Start Year is greater than Current Year"

            # If the start_year and current year variables are the same, form a one-object list to iterate through consisting of the current_year
            # Else, create a new year list which is the full range from the start year to current year by slicing self.__year
            if start_year == current_year:
                years = [current_year]
            else:
                years = self.__years[self.__years.index(start_year) : self.__years.index(current_year) + 1]

            for k, v in self.__months.items():
                if start_month == v:
                    start_month1 = k
                if current_month == v:
                    current_month1 = k

            # If this is a recursive run, towns_list will be a sliced list starting from the last run city
            towns_list = self.latest_file()

            if towns_list == 'None':
                towns_list = self.__towns
            elif towns_list == 'All Files Downloaded':
                return 'All Files Downloaded'

            try:
                for i in towns_list:
                    # time.sleep(0.1)
                    # city_tracker.update()
                    # Takes the name of the city from the list and splits the string at the space, then joins the strings in the newly created list
                    # This is needed to use in the url3 variable to access the correct 10k pdfs
                    city0 = i.split(' ')
                    city = ''.join(i.split(' '))
                    for y in years:
                        # time.sleep(0.1)
                        # year_tracker.update()
                        months1 = months[start_month1 : current_month1 + 1]
                        for m in months1:
                            # time.sleep(0.1)
                            # months_tracker.update()
                            if '/' not in city:
                                url3 = base_url + y + '-' + m + '/x/' + city
                                new_filename = " ".join([' '.join(city0), self.__months[m], y]) + ".pdf"

                            elif '/' in city:
                                city = '%2F'.join(city.split('/'))
                                del city0[city0.index('/')]
                                url3 = base_url + y + '-' + m + '/x/' + city
                                new_filename = " ".join([' '.join(city0), self.__months[m], y]) + ".pdf"

                            with session.get(url3, params=params, stream=True) as reader, open(new_filename,'wb') as writer:
                                for chunk in reader.iter_content(chunk_size=1000000):
                                    # Casting the bytes into a str type and slicing the first 20 characters to check if 'PDF' is in
                                    check_pdf = str(chunk)[:20]
                                    #print(check_pdf)
                                    if 'PDF' in check_pdf:
                                        writer.write(chunk)
                                    else:
                                        logger.warning(f'WARNING! {new_filename} is possibly a corrupted file')
                                        possible_corrupted_files.append(new_filename)

            except AssertionError:
                # The program should not be allowed to continue should this error occur
                logger.exception('An Error Has Occured: AssertionError')


            except IOError:
                """An OS Error has occurred """
                logger.exception(f'IOError has Occurred')
                self.update_njr10k(start, finish)

            except requests.exceptions.HTTPError as rht:
                """An HTTP error occurred."""
                logger.exception(f'An HTTP has Occurred: {rht}')

            except requests.exceptions.Timeout as ret:
                """The request timed out.
                Catching this error will catch both
                :exc:`~requests.exceptions.ConnectTimeout` and
                :exc:`~requests.exceptions.ReadTimeout` errors.
                """
                logger.exception(f'The Request Has Timed Out: {ret}')

            except requests.exceptions.InvalidURL as rei:
                """The URL provided was somehow invalid."""
                logger.exception(f'The URL Provided Was Invalid: {rei}')

            except requests.exceptions.RetryError as rer:
                """Custom retries logic failed"""
                logger.exception(f'Custom Retries Logic Failed: {rer}')

            except requests.exceptions.StreamConsumedError as res:
                """The content for this response was already consumed."""
                logger.exception(f'The Content For This Response Was Already Consumed: {res}')

            except requests.exceptions.ContentDecodingError as rec:
                """Failed to decode response content."""
                logger.exception(f'Failed to Decode Response Content: {rec}')

            except requests.exceptions.ChunkedEncodingError as rece:
                """The server declared chunked encoding but sent an invalid chunk."""
                logger.exception(f'Invalid Chunk Encoding: {rece}')
            except:
                logger.exception('An Error Has Occured: Unhandled')

            else:

                end_time = datetime.datetime.now()
                run_time = end_time - start_time

                if Scraper.no_of_runs == 0:
                    Scraper.event_log[Scraper.no_of_runs] = {'Run Type' : name,
                                                             'Latest Available Data': self.current_data_avail,
                                                             'Run Time' : str(run_time),
                                                             'Run Date': time.ctime(),
                                                             'Days Between Update': 0}
                    logger.info(f'New Event Log Created: \n{Scraper.event_log}')
                elif Scraper.no_of_runs > 0:
                    Scraper.event_log[Scraper.no_of_runs] = {'Run Type' : name,
                                                             'Latest Available Data': self.current_data_avail,
                                                             'Run Time' : str(run_time),
                                                             'Run Date': time.ctime(),
                                                             'Days Between Update': self.daysuntilupdate(
                                                                 Scraper.no_of_runs)}
                    logger.info(f'New Event Log Created: \n{Scraper.event_log}')
                Scraper.no_of_runs += 1
                # This is a very lengthy program so I'd like play a sound that signifies the process is done
                winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)

        return possible_corrupted_files



if __name__ == '__main__':

    main_dictionary = {
        '2018': {},
        '2019': {},
        '2020': {},
        '2021': {},
        '2022': {},
        '2023': {}
    }

    """This is the first part of the NJ Realtor 10k Scrapper. This section of the program will systematically check for the 
    most recent files uploaded to their database and download them for processing and analysis in the second half of the program.
    """

    if os.path.exists('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data\\NJ Scrapper Data Dictionary.dat'):
        os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')
        with shelve.open('NJ Scrapper Data Dictionary') as saved_data_file:
            obj = saved_data_file['Scrapper Object']

    else:
        obj = Scraper()

    # load the shelf file for the saved data
    # If this code has never been run before, the full NJR10k will need to be run all the way back from 2018
    if obj.no_of_runs == 0:
        obj.njrdata()
        #print(obj._Scraper__towns)
        # print(Scraper.current_data_avail)
        # print(obj._Scraper__towns)
        # results = obj.njr10k()
        results = obj.read_logger()
        # The NJR10k function will return a list if there and pdfs found to be possibly corrupted
        # If length of the list is created than 0, the program will trigger the next function to download corrupted data
        if results == 'All Files Downloaded':
            # Read latest logger file to get a list of the corrupted files
            # results1 = obj.corrupted_files(obj.read_logger())
            pass
        elif len(results) > 0:
            results1 = obj.corrupted_files(results)
        else:
            results1 = ['No Corrupted Files']

        runs = 0
        for pdf in obj.pdf_generator():
            obj.extract_re_data(pdf, results1)
            # This is temporary code to make sure the information is being formatted into the dictionary as expected
            runs += 1
            if runs % 100 == 0:
                print(tabulate(main_dictionary['2018'], headers='keys', tablefmt='plain'))
                print()
                print(tabulate(main_dictionary['2019'], headers='keys', tablefmt='plain'))
                print()
                print(tabulate(main_dictionary['2020'], headers='keys', tablefmt='plain'))
                print()
                print(tabulate(main_dictionary['2021'], headers='keys', tablefmt='plain'))
                print()
                print(tabulate(main_dictionary['2022'], headers='keys', tablefmt='plain'))
                print()
                print(tabulate(main_dictionary['2023'], headers='keys', tablefmt='plain'))

        old_dir = os.getcwd()
        # Use the Shelve module to save data for later use
        os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')
        with shelve.open('NJ Scrapper Data Dictionary') as saved_data_file:
            saved_data_file['Main Dictionary'] = main_dictionary
            saved_data_file['Corrupted Files'] = results1
            saved_data_file['Scrapper Object'] = obj

        os.chdir(old_dir)


        # Organize all the files into their respective folders according to the year of the data
        obj.OrganizeFiles(results1)

        now = datetime.datetime.strptime(time.ctime(), "%a %b %d %H:%M:%S %Y")
        end_of_year = datetime.datetime.strptime(Scraper.current_data_avail.split(' ')[1] + "/12/31", "%Y/%m/%d")

        # If todays date is the last day of the year or greater run zip functino. If not, stay sleep
        if now >= end_of_year:
            obj.CreateZip()

        final_dict = obj.create_categories(obj.quarterly_yearly(main_dictionary))

        obj.pandas2excel(final_dict)

    # If this code has been run before, the Updated NJR10k will need to be run from last pulled data
    elif obj.no_of_runs > 0:
        obj.njrdata()
        if obj.check_results() == True:
            pass
            #time.sleep() put decided sleep time here to rerun the program
        else:
            start, finish = obj.check_results()
            results = obj.update_njr10k(start,finish)
            if results == 'All Files Downloaded':
                # Read latest logger file to get a list of the corrupted files
                # results1 = obj.corrupted_files(obj.read_logger())
                pass
            elif len(results) > 0:
                results1 = obj.corrupted_files(results)
            else:
                results1 = ['No Corrupted Files']

        # Use the Shelve module to save data for later use
        os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')
        with shelve.open('NJ Scrapper Data Dictionary') as saved_data_file:
            main_dictionary = saved_data_file['Main Dictionary']
            saved_data_file['Corrupted Files'] = results1

        for pdf in obj.pdf_generator():
            obj.extract_re_data(pdf)

        os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')
        with shelve.open('NJ Scrapper Data Dictionary') as saved_data_file:
            saved_data_file['Main Dictionary'] = main_dictionary
            saved_data_file['Scrapper Object'] = obj
            saved_data_file.sync()

        obj.OrganizeFiles(results1)

        now = datetime.datetime.strptime(time.ctime(), "%a %b %d %H:%M:%S %Y")
        end_of_year = datetime.datetime.strptime(Scraper.current_data_avail.split(' ')[1] + "/12/31", "%Y/%m/%d")

        # If todays date is the last day of the year or greater run zip functino. If not, stay sleep
        if now >= end_of_year:
            obj.CreateZip()

        final_dict = obj.create_categories(obj.quarterly_yearly(main_dictionary))

        obj.pandas2excel(final_dict)








    #
    # for i in obj.pdf_generator():
    #     obj.extract_re_data(i)
    #
    # pprint.pprint(main_dictionary)

    # username, pw = obj.get_us_pw('NJRealtor')
    # print(type(username))
    # print(type(pw))


