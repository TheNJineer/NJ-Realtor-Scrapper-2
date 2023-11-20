import os
import winsound
import openpyxl
import PyPDF2
import shutil
import shelve
import datetime
from datetime import date
# import enlighten
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
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
# from webdriver_manager.chrome import ChromeDriverManager
# Allows us to interact with the Enter key and see search results
from selenium.webdriver.common.keys import Keys
# Allows Selenium to search for page elements By their attributes
from selenium.webdriver.common.by import By
# from webdriver_manager.chrome import ChromeDriverManager
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
    no_of_runs = 1
    event_log = {0: {'Run Type': 'NJR10k', 'Latest Available Data': 'June 2023', 'Run Time': 'N.A',
                     'Run Date': '2023-08-20', 'Days Between Update': 0}}

    def __init__(self):
        # Change the directory to store the temporary Selenium files to be processed
        os.chdir('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
        self.njrdata()
        self.event_log()
        # Empty list to store the found cities and counties
        self.__counties = []
        self.__towns = []
        # The years and months for the real estate data I'm looking for
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

    # Function which scrapes the cities and counties from the njrealtor 10k state page
    def area_results(self, soup):
        area = soup.find('select', id="lmuArea").children
        for obj1 in area:
            newobj = obj1.get_text()
            if newobj in ['Select an area...', 'Entire State', '\n']:
                continue
            else:
                if 'County' in newobj:
                    # There are multiple towns in different counties with the same name. Their county is attached
                    # and need to be separated from the target values
                    if '/' in newobj:
                        # Do not split the city name if it has '/' in it. It signifies that there is more than 1 city
                        # with that name and shows the county it belongs to
                        # newobj = newobj.split('/')
                        self.__towns.append(newobj)
                    else:
                        self.__counties.append(newobj)
                else:
                    self.__towns.append(newobj)

    def check_results(self):

        event_log_list = list(Scraper.event_log.keys())
        last_run_num = event_log_list[-1]
        last_data_avail = Scraper.event_log[last_run_num]['Latest Available Data']

        if Scraper.current_data_avail == last_data_avail:

            return 'No Results'

        elif Scraper.current_data_avail != last_data_avail:
            start = last_data_avail.split()
            finish = Scraper.current_data_avail.split()

            return start, finish

    def corrupted_files(self, corrupt_list):

        logger = logging.getLogger("Corrupted Files")
        logger.setLevel(logging.DEBUG)
        # Create the FileHandler() and StreamHandler() loggers
        f_handler = logging.FileHandler('Corrupted Files ' + str(datetime.datetime.today().date()) + '.log')
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

        corrupt_dict = {}

        possible_corrupted_files = []
        # Do I want to delete the corrupted files before re-downloading them?
        # Extract the town, month and year from the file name and put it in a dictionary for further processing
        # Real estate data pulled for the year 2019 seems to not be available for some areas so we'll skip these files.
        for n, i in enumerate(corrupt_list):
            info = i.rstrip('.pdf').split(' ')
            town = info[0:len(info) - 2]
            if len(town) > 1:
                if 'County' in town:
                    # This means the city name is a duplicate and needs to have the county distinguished
                    # For example: ['Franklin', 'Twp', 'Gloucester', 'County']
                    # --------> ['Franklin', 'Twp', '/', 'Gloucester', 'County']
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
                for idx, i1 in enumerate(self.__towns):
                    if town in i1:
                        town = self.__towns[idx]
                        corrupt_dict[n] = [town, month, year]

        # UnboundLocalError produced without this checkpoint.
        if len(corrupt_dict) < 1:
            if len(possible_corrupted_files) > 0:
                print('All corrupted files have been captured. The Extract RE Data function will now initiate...')
                time.sleep(0.5)
                return possible_corrupted_files
            else:
                print('There are no corrupted files. The Extract RE Data function will now initiate...')
                time.sleep(0.5)
                return ['No Corrupted Files']

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

            session.get(url)  # Requesting initial log-in page
            session.post(url2, data=payload1)  # response object for initially logging into website

            try:
                for k, v in corrupt_dict.items():
                    city0 = v[0].split(' ')
                    city = ''.join(city0)
                    # Redundant checker if a 2019 file has slipped through the first check
                    if v[2] == '2019':
                        continue
                    else:
                        y = v[2]
                    for k1, v1 in self.__months.items():
                        if v[1] in v1:
                            m = k1
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
                            # Casting the bytes into a str type
                            # and slicing the first 20 characters to check if 'PDF' is in
                            check_pdf = str(chunk)[:20]
                            # print(check_pdf)
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

            except Exception as e:
                logger.exception(f'An Error Has Unhandled Occurred: {e} ')

            else:

                logger.removeHandler(f_handler)
                logger.removeHandler(c_handler)
                logging.shutdown()
                print('All corrupted files have been captured. The Extract RE Data function will now initiate...')
                time.sleep(0.5)

        return possible_corrupted_files

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
            os.makedirs('C:\\Users\\Omar\\Desktop\\Python Temp Folder\\PDF Temp Files')

        logger.removeHandler(f_handler)
        logger.removeHandler(c_handler)
        logging.shutdown()

    # Function which scrapes the current month of data available
    def current_data_avail(self, soup):
        current_results = soup.find('select', id="lmuTime").children
        current_results = list(current_results)
        month_year = current_results[2].get_text()
        target = month_year.split(' ')
        year = target[1]
        # month = target[0]
        if year not in self.__years:
            self.__years.append(year)
            main_dictionary[year] = {}
            full_year[year] = {}

        Scraper.current_data_avail = month_year

    # Creates the initial dictionary the scraped city data will be stored
    def data_na(self, town, month, year):

        current_year = year
        city = town
        county = 'N.A'
        quarter = self.quarter(month)
        new_listings_current = 0
        new_listings_fy = 0
        new_listings_per_change = 0.0
        new_listings_per_change_fy = 0.0
        closed_sales_current = 0
        closed_sales_fy = 0
        closed_sales_per_change = 0.0
        closed_sales_per_change_fy = 0.0
        DOM_current = 0
        dom_fy = 0
        DOM_per_change = 0.0
        dom_per_change_fy = 0.0
        median_sales_fy = 0
        median_sales_current = 0
        median_sales_per_change = 0.0
        median_sales_per_change_fy = 0.0
        percent_lpr_current = 0.0
        percent_lpr_fy = 0.0
        percent_lpr_per_change = 0.0
        percent_lpr_per_change_fy = 0.0
        inventory_current = 0
        inventory_fy = 0
        inventory_per_change = 0.0
        inventory_per_change_fy = 0.0
        supply_current = 0.0
        supply_fy = 0
        supply_per_change = 0.0
        supply_per_change_fy = 0.0

        category_list = ['City', 'County', 'Quarter', 'Month', 'Year', 'New Listings',
                         'New Listing % Change (YoY)', 'Closed Sales',
                         'Closed Sale % Change (YoY)', 'Days on Markets', 'Days on Market % Change (YoY)',
                         'Median Sales Prices',
                         'Median Sales Price % Change (YoY)', 'Percent of Listing Price Received',
                         'Percent of Listing Price Receive % Change (YoY)', 'Inventory of Homes for Sales',
                         'Inventory of Homes for Sale % Change (YoY)', 'Months of Supply',
                         'Months of Supplies % Change (YoY)']

        variable_list = [city, county, quarter, month, current_year, new_listings_current, new_listings_per_change,
                         closed_sales_current,
                         closed_sales_per_change, DOM_current, DOM_per_change, median_sales_current,
                         median_sales_per_change, percent_lpr_current, percent_lpr_per_change,
                         inventory_current, inventory_per_change, supply_current, supply_per_change]

        fy_variable_list = [city, county, month, current_year, new_listings_fy, new_listings_per_change_fy,
                            closed_sales_fy, closed_sales_per_change_fy, dom_fy, dom_per_change_fy, median_sales_fy,
                            median_sales_per_change_fy, percent_lpr_fy, percent_lpr_per_change_fy,
                            inventory_fy, inventory_per_change_fy, supply_fy, supply_per_change_fy]

        if main_dictionary[current_year] == {}:
            for idx, n in enumerate(category_list):
                main_dictionary[current_year].setdefault(n, [])
                main_dictionary[current_year][n].append(variable_list[idx])
        else:
            for idx, n in enumerate(category_list):
                main_dictionary[current_year][n].append(variable_list[idx])
        if month == 'December':
            category_list1 = category_list[:]
            del category_list1[2]
            if full_year[current_year] == {}:
                for idx, n in enumerate(category_list1):
                    full_year[current_year].setdefault(n, [])
                    full_year[current_year][n].append(fy_variable_list[idx])
            else:
                for idx, n in enumerate(category_list1):
                    full_year[current_year][n].append(fy_variable_list[idx])

    # Function which calculates the difference between the current download date and previous date
    # Use this to calculate the average amount of time it takes between new update periods
    def daysuntilupdate(self, no_of_runs):
        current = Scraper.no_of_runs
        previous = current - 1
        current_date = datetime.datetime.now()
        previous_date = datetime.datetime.strptime(Scraper.event_log[previous]['Run Date'], "%a %b %d %H:%M:%S %Y")
        delta = current_date - previous_date

        return delta.days

    # Functon which checks if there's a duplicate vector for the current input year
    def duplicate_vector_check(self, pdfname, current_vector, year):

        i = len(main_dictionary[year]['City'])
        number = i - 1
        if number > 0:
            for r in range(number, 0, -1):
                previous_vector = []
                for k in main_dictionary[year].keys():
                    previous_vector.append(main_dictionary[year][k][r])

                assert current_vector != previous_vector, f'Duplicate Error: {pdfname} & {previous_vector[0]} ' \
                                                          f'{previous_vector[3]} {previous_vector[4]}.pdf'
        else:
            previous_vector = []
            for k in main_dictionary[year].keys():
                previous_vector.append(main_dictionary[year][k][number])

            assert current_vector != previous_vector, f'Duplicate Error: {pdfname} & {previous_vector[0]} ' \
                                                      f'{previous_vector[3]} {previous_vector[4]}.pdf'

    def event_log(self):

        if os.path.exists('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data\\NJ Scrapper Data Dictionary.dat'):
            os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')
            with shelve.open('NJ Scrapper Data Dictionary', writeback=True) as saved_data_file:
                if saved_data_file['Event Log']:
                    Scraper.event_log = saved_data_file['Event Log']
                    runs_list = [i for i in Scraper.event_log.keys()]
                    Scraper.no_of_runs = runs_list[-1] + 1

            os.chdir('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
        else:

            key_names = ['Run Type', 'Latest Available Data', 'Run Time', 'Run Date', 'Days Between Update']
            Scraper.event_log.setdefault(Scraper.no_of_runs, {})

            for kn in key_names:
                Scraper.event_log[Scraper.no_of_runs].setdefault(kn, '')

    def event_log_update(self, run_time, logger):

        Scraper.event_log[Scraper.no_of_runs]['Run Type'] = name
        Scraper.event_log[Scraper.no_of_runs]['Latest Available Data'] = self.current_data_avail
        Scraper.event_log[Scraper.no_of_runs]['Run Time'] = str(run_time)
        Scraper.event_log[Scraper.no_of_runs]['Run Date'] = time.ctime()
        Scraper.event_log[Scraper.no_of_runs]['Days Between Update'] = self.daysuntilupdate(
            Scraper.no_of_runs)

        logger.info(f'New Event Log Created: \n{Scraper.event_log}')

    # Function which extracts the month, current and previous year, new listing, closing sales, DOM, median sales, etc
    # Data will then be stored in a dictionary
    def extract_re_data(self, pdfname, possible_corrupted_list):

        os.chdir('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
        # logging.basicConfig(level=logging.ERROR)
        logger = logging.getLogger("Extract_Data")
        logger.propagate = False
        logger.setLevel(logging.DEBUG)
        # Create the FileHandler() and StreamHandler() loggers
        f_handler = logging.FileHandler('Extract_Data ' + str(datetime.datetime.today().date()) + '.log')
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

        pdfname = pdfname
        # Information will be used in data_na function
        info = pdfname.rstrip('.pdf').split(' ')
        town = info[0:len(info) - 2]
        if len(town) > 2:
            if 'County' in town:
                # This means the city name is a duplicate and needs to have the county distinguished
                # For example: ['Franklin', 'Twp', 'Gloucester', 'County']
                # --------> ['Franklin', 'Twp', '/', 'Gloucester', 'County']
                town.insert(town.index('County') - 1, '/')
                town = ' '.join(town)
            else:
                town = ' '.join(town)
        else:
            town = ' '.join(town)

        month1 = info[-2]
        year1 = info[-1]

        if pdfname in possible_corrupted_list:
            logger.info(f'PDF corrupted. The city of {town} for {month1} {year1} does not have data')
            self.data_na(town, month1, year1)
            logger.removeHandler(f_handler)
            logger.removeHandler(c_handler)
            logging.shutdown()
        else:
            try:
                with open(pdfname, 'rb') as reader:
                    pdfread = PyPDF2.PdfReader(reader)
                    page = pdfread.pages[0]
                    target = page.extract_text()
                    lines = target.split('\n')
                    lines = lines[24:]
                    check_city = lines[4]
                    check_county = lines[5]

                if town == check_city:
                    city = town
                    if '(' in check_county:
                        check_county = check_county.split('(')[0].strip()
                        if check_county in self.__counties:
                            county = check_county
                    else:
                        check_county = check_county.strip()
                        if check_county in self.__counties:
                            county = check_county
                        else:
                            raise Exception("County can't be found. PDF possibly corrupted" )

                    outcome = self.good_data(pdfname, target, city, county, month1, year1)
                    if outcome is None:
                        logger.info(f'The data for {pdfname} has been extracted')
                    elif outcome[1] == 'ree':
                        raise re.error(f'{outcome[0]}')
                    elif outcome[1] == 'AE':
                        logger.info(f'PDF corrupted. The city of {town} for {month1} {year1} does not have data')
                        self.data_na(town, month1, year1)
                        raise AssertionError(f'{outcome[0]}')
                    elif outcome[1] == 'E':
                        raise Exception(f'{outcome[0]}')

                elif town != check_city:
                    # Reoccurring alternate index locations of the city and county
                    check_city = lines[2]
                    check_county = lines[3]
                    for i in self.__towns:
                        if i == check_city and check_city == town:
                            city = town
                            # break
                            if '(' in check_county:
                                check_county = check_county.split('(')[0].strip()
                                if check_county in self.__counties:
                                    county = check_county
                                else:
                                    for c in self.__counties:
                                        if c in lines:
                                            county = c
                                        # break

                                outcome = self.good_data(pdfname, target, city, county, month1, year1)
                                if outcome is None:
                                    logger.info(f'The data for {pdfname} has been extracted')
                                elif outcome[1] == 'ree':
                                    raise re.error(f'{outcome[0]}')
                                elif outcome[1] == 'AE':
                                    logger.info(f'PDF corrupted. The city of {town} for {month1} {year1} does not have data')
                                    self.data_na(town, month1, year1)
                                    raise AssertionError(f'{outcome[0]}')
                                elif outcome[1] == 'E':
                                    raise Exception(f'{outcome[0]}')

                            elif '(' not in check_county:
                                check_county = check_county.strip()
                                if check_county in self.__counties:
                                    county = check_county
                                else:
                                    for c in self.__counties:
                                        if c in lines:
                                            county = c
                                        break

                                outcome = self.good_data(pdfname, target, city, county, month1, year1)
                                if outcome is None:
                                    logger.info(f'The data for {pdfname} has been extracted')
                                elif outcome[1] == 'ree':
                                    raise re.error(f'{outcome[0]}')
                                elif outcome[1] == 'AE':
                                    logger.info(f'PDF corrupted. The city of {town} for {month1} {year1} does not have data')
                                    self.data_na(town, month1, year1)
                                    raise AssertionError(f'{outcome[0]}')
                                elif outcome[1] == 'E':
                                    raise Exception(f'{outcome[0]}')

                            else:
                                raise Exception("County can't be found. PDF possibly corrupted")

                        if i == check_city and check_city != town:
                            logger.info(f'PDF corrupted. The city of {town} for {month1} {year1} does not have data')
                            self.data_na(town, month1, year1)

            except PyPDF2._reader.EmptyFileError as efe:
                logger.exception(f'An Error Has Occured (File Possibly Corrupted):\n{efe}')
                logger.info(f'The city of {town} for {month1} {year1} does not have data')
                # If function encounters an empty/corrupted pdf,
                # the data_na function will render all information available for that file equal to zero
                # The generator will then pick up at the next pdf to continue extracting data
                self.data_na(town, month1, year1)

            except re.error as ree:
                logger.exception(f'A Regex Error Has Occurred:\n{ree}')

            except AssertionError as AE:
                logger.exception(f'An AssertionError Has Occurred:\n{AE}')

            except Exception as E:
                logger.exception(f'An Unhandled Error Has Occurred:\n{E}')

            finally:
                logger.removeHandler(f_handler)
                logger.removeHandler(c_handler)
                logging.shutdown()

    # Function which pulls the username and password for a specified website
    def get_us_pw(self, website):
        # Saves the current directory in a variable in order to switch back to it once the program ends
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

    def good_data(self, pdfname, target, city, county, month1, year1):

        try:
            month_pattern = re.compile(
                r'(January|February|March|April|May|June|July|August|September|October|November|December)\sYear\sto\sDate\sSingle\sFamily')
            month = month_pattern.search(target)
            month = month.group(1)
            quarter = self.quarter(month)
            key_metrics_basic_pattern = re.compile(
                r'Key\sMetrics\s(\d{4})\s(\d{4})\sPercent\sChange\sThru\s\d{1,2}?-\d{4}\sThru\s\d{1,2}?-\d{4}\sPercent\sChange')
            km_search = list(key_metrics_basic_pattern.findall(target))
            current_year = km_search[0][1]
            new_listings_pattern = re.compile(
                r'New\sListings\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)')
            new_listing_search = list(new_listings_pattern.findall(target))
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
            closed_sales_pattern = re.compile(
                r'Closed\sSales\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)')
            closed_sales_search = list(closed_sales_pattern.findall(target))
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
            DOM_pattern = re.compile(
                r'Days\son\sMarket\sUntil\sSale\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(0.0%|--|[+-]\s\d{0,3}?.\d{0,1}?%)')
            DOM_search = list(DOM_pattern.findall(target))
            DOM_current = int(DOM_search[0][1])
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
                r'Median\sSales\sPrice\*\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
            median_sales_search = list(median_sales_pattern.findall(target))
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
            percent_lpr_pattern = re.compile(
                r'Percent\sof\sList\sPrice\sReceived\*\s(\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
            percent_lpr_search = list(percent_lpr_pattern.findall(target))
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
            inventory_pattern = re.compile(
                r'Inventory\sof\sHomes\sfor\sSale\s(--|\d{0,3}?)\s(--|\d{0,3}?)\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)\s(--|\d{0,3}?)\s(--|\d{0,3}?)\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
            inventory_search = list(inventory_pattern.findall(target))
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
            supply_pattern = re.compile(
                r'Months\sSupply\sof\sInventory\s(--|\d{1,2}?.\d{1})\s(--|\d{1,2}?.\d{1})\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)\s(--|\d{1,2}?.\d{1})\s(--|\d{1,2}?.\d{1})\s(0.0%|--|[+-]\s\d{1,3}?.\d{1}%)')
            supply_search = list(supply_pattern.findall(target))
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

            category_list = ['City', 'County', 'Quarter', 'Month', 'Year', 'New Listings',
                             'New Listing % Change (YoY)', 'Closed Sales',
                             'Closed Sale % Change (YoY)', 'Days on Markets', 'Days on Market % Change (YoY)',
                             'Median Sales Prices',
                             'Median Sales Price % Change (YoY)', 'Percent of Listing Price Received',
                             'Percent of Listing Price Receive % Change (YoY)', 'Inventory of Homes for Sales',
                             'Inventory of Homes for Sale % Change (YoY)', 'Months of Supply',
                             'Months of Supplies % Change (YoY)']

            variable_list = [city, county, quarter, month, current_year, new_listings_current,
                             new_listings_per_change,
                             closed_sales_current,
                             closed_sales_per_change, DOM_current, DOM_per_change, median_sales_current,
                             median_sales_per_change, percent_lpr_current, percent_lpr_per_change,
                             inventory_current, inventory_per_change, supply_current, supply_per_change]

            assert month == month1 and current_year == year1, f'{pdfname} is corrupted. ' \
                                                              f'Giving data for {city} {month} {current_year}.pdf'

            if main_dictionary[current_year] == {}:
                for idx, n in enumerate(category_list):
                    main_dictionary[current_year].setdefault(n, [])
                    main_dictionary[current_year][n].append(variable_list[idx])
            else:
                self.duplicate_vector_check(pdfname, variable_list, current_year)
                for idx, n in enumerate(category_list):
                    main_dictionary[current_year][n].append(variable_list[idx])

            if month == 'December':
                category_list1 = category_list[:]
                del category_list1[2]
                new_listings_fy = int(new_listing_search[0][4])
                new_listings_pc_fy = str(new_listing_search[0][5]).split(' ')
                new_listings_per_change_fy = ''.join(new_listings_pc_fy).rstrip('%')
                if '+' in new_listings_per_change_fy:
                    new_listings_per_change_fy.lstrip('+')
                    new_listings_per_change_fy = float(new_listings_per_change_fy) / 100
                elif '--' in new_listings_per_change_fy:
                    new_listings_per_change_fy = 0.0
                else:
                    new_listings_per_change_fy = float(new_listings_per_change_fy) / 100

                closed_sales_fy = int(closed_sales_search[0][4])
                closed_sales_pc_fy = closed_sales_search[0][5].split(' ')
                closed_sales_per_change_fy = ''.join(closed_sales_pc_fy).rstrip('%')
                if '+' in closed_sales_per_change_fy:
                    closed_sales_per_change_fy.lstrip('+')
                    closed_sales_per_change_fy = float(closed_sales_per_change_fy) / 100
                elif '--' in closed_sales_per_change_fy:
                    closed_sales_per_change_fy = 0.0
                else:
                    closed_sales_per_change_fy = float(closed_sales_per_change_fy) / 100

                dom_fy = int(DOM_search[0][4])
                dom_pc_fy = DOM_search[0][5].split(' ')
                dom_per_change_fy = ''.join(dom_pc_fy).rstrip('%')
                if '+' in dom_per_change_fy:
                    dom_per_change_fy.lstrip('+')
                    dom_per_change_fy = float(dom_per_change_fy) / 100
                elif '--' in dom_per_change_fy:
                    dom_per_change_fy = 0.0
                else:
                    dom_per_change_fy = float(dom_per_change_fy) / 100

                median_sales_fy = median_sales_search[0][4]
                median_sales_fy = int("".join(median_sales_fy.split(',')).lstrip('$'))
                median_sales_pc_fy = median_sales_search[0][5].split(' ')
                median_sales_per_change_fy = ''.join(median_sales_pc_fy).rstrip('%')
                if '+' in median_sales_per_change_fy:
                    median_sales_per_change_fy.lstrip('+')
                    median_sales_per_change_fy = float(median_sales_per_change_fy) / 100
                elif '--' in median_sales_per_change_fy:
                    median_sales_per_change_fy = 0.0
                else:
                    median_sales_per_change_fy = float(median_sales_per_change_fy) / 100

                percent_lpr_fy = float(percent_lpr_search[0][4].rstrip('%'))
                percent_lpr_pc_fy = percent_lpr_search[0][5].split(' ')
                percent_lpr_per_change_fy = ''.join(percent_lpr_pc_fy).rstrip('%')
                if '+' in percent_lpr_per_change_fy:
                    percent_lpr_per_change_fy.lstrip('+')
                    percent_lpr_per_change_fy = float(percent_lpr_per_change_fy) / 100
                elif '--' in percent_lpr_per_change_fy:
                    percent_lpr_per_change_fy = 0.0
                else:
                    percent_lpr_per_change_fy = float(percent_lpr_per_change_fy) / 100

                inventory_fy = inventory_search[0][4]
                if inventory_fy == '--':
                    inventory_fy = 0.0
                inventory_pc_fy = inventory_search[0][5].split(' ')
                inventory_per_change_fy = ''.join(inventory_pc_fy).rstrip('%')
                if '+' in inventory_per_change_fy:
                    inventory_per_change_fy.lstrip('+')
                    inventory_per_change_fy = float(inventory_per_change_fy) / 100
                elif '--' in inventory_per_change_fy:
                    inventory_per_change_fy = 0.0
                else:
                    inventory_per_change_fy = float(inventory_per_change_fy) / 100

                supply_fy = supply_search[0][4]
                if supply_fy == '--':
                    supply_fy = 0.0
                supply_pc_fy = supply_search[0][5].split(' ')
                supply_per_change_fy = ''.join(supply_pc_fy).rstrip('%')
                if '+' in supply_per_change_fy:
                    supply_per_change_fy.lstrip('+')
                    supply_per_change_fy = float(supply_per_change_fy) / 100
                elif '--' in supply_per_change_fy:
                    supply_per_change_fy = 0.0
                else:
                    supply_per_change_fy = float(supply_per_change_fy) / 100

                fy_variable_list = [city, county, month, current_year, new_listings_fy,
                                    new_listings_per_change_fy,
                                    closed_sales_fy, closed_sales_per_change_fy, dom_fy, dom_per_change_fy,
                                    median_sales_fy,
                                    median_sales_per_change_fy, percent_lpr_fy, percent_lpr_per_change_fy,
                                    inventory_fy, inventory_per_change_fy, supply_fy, supply_per_change_fy]

                if full_year[current_year] == {}:
                    for idx, n in enumerate(category_list1):
                        full_year[current_year].setdefault(n, [])
                        full_year[current_year][n].append(fy_variable_list[idx])
                else:
                    for idx, n in enumerate(category_list1):
                        full_year[current_year][n].append(fy_variable_list[idx])
        except re.error as ree:
            return [ree, 'ree']
        except AssertionError as AE:
            return [AE, 'AE']
        except Exception as E:
            return [E, 'E']
        else:
            return None

    # Used in case the njr10k or the update_njr10k functions are used recursively.
    # This function will find the latest file downloaded and continue from that point
    def latest_file(self):

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
                        # Files are stores in alphabetical order
                        # Wyckoff Twp are the last pdfs to be downloaded
                        # If the target pdf name is either equal to Wyckoff Twp Sept for this year or last year
                        # or Wyckoff Twp of the current month and year
                        # All files may be downloaded, need to do a second check
                        check1 = 'Wyckoff Twp September ' + str(int(Scraper.current_data_avail.split(' ')[1]) - 1) + '.pdf'
                        check2 = 'Wyckoff Twp September ' + Scraper.current_data_avail.split(' ')[1] + '.pdf'
                        check3 = 'Wyckoff Twp ' + Scraper.current_data_avail + '.pdf'
                        if target == check1 or target == check2 or target == check3:
                            print(f'Latest file downloaded is: {target}\nAll files maybe downloaded...')
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
                    check4 = 'Wyckoff Twp ' + Scraper.current_data_avail + '.pdf'
                    if os.path.exists(base_path + '\\' + check4):
                        print('All files have been downloaded. Now reading previous logger file...')
                        return 'All Files Downloaded'
                    else:
                        full_town_list = self.__towns
                        return full_town_list[full_town_list.index(town):]

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

            url = 'https://www.njrealtor.com/login/?rd=10&passedURL=/goto/10k/'
            url2 = 'https://www.njrealtor.com/ramco-api/web-services/login_POST.php'

            session.get(url) # Request to arrive at the log-in page
            session.post(url2, data=payload1) # Response object to logging into website

            # If this is a recursive run, towns_list will be a sliced list starting from the last run city
            # Latest_file searches for the last
            towns_list = self.latest_file()

            if towns_list == 'None':
                towns_list = self.__towns
            elif towns_list == 'All Files Downloaded':
                return 'All Files Downloaded'

            try:
                for i in towns_list:
                    # Takes the name of the city from the list and splits the string at the space,
                    # then joins the strings in the newly created list
                    # This is needed to use in the url3 variable to access the correct 10k pdfs
                    city0 = i.split(' ')
                    city = ''.join(city0)
                    for y in self.__years:

                        if y == '2019':
                            # If year = 2019, there is no data available from January to Sept.
                            # Also some cities will not have data available for Sept and produce data for other dates
                            months1 = months[8:13]
                            for m in months1:

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
                                        # Casting the bytes into a str type
                                        # and slicing the first 20 characters to check if 'PDF' is in
                                        check_pdf = str(chunk)[:20]
                                        # print(check_pdf)
                                        if 'PDF' in check_pdf:
                                            writer.write(chunk)
                                        else:
                                            logger.warning(f'WARNING! {new_filename} is possibly a corrupted file')
                                            possible_corrupted_files.append(new_filename)

                        elif y == self.__years[-1]:
                            # If year is the latest year, months1 will equal a sliced list of the
                            # first month represented by 01 to the latests month represent by 2 digits
                            for k, v in self.__months.items():
                                # If v equals the month of the most current data
                                if v == Scraper.current_data_avail.split(' ')[0]:
                                    months1 = months[:months.index(k) + 1]
                            for m in months1:

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
                                        # Casting the bytes into a str type
                                        # and slicing the first 20 characters to check if 'PDF' is in
                                        check_pdf = str(chunk)[:20]
                                        # print(check_pdf)
                                        if 'PDF' in check_pdf:
                                            writer.write(chunk)
                                        else:
                                            logger.warning(f'WARNING! {new_filename} is possibly a corrupted file')
                                            possible_corrupted_files.append(new_filename)

                        elif y != '2019':
                            for m in months:

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
                                        # Casting the bytes into a str type
                                        # and slicing the first 20 characters to check if 'PDF' is in
                                        check_pdf = str(chunk)[:20]
                                        # print(check_pdf)
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

            except Exception as e:
                logger.exception(f'An Unhandled Error Has Occurred: {e}')

            else:

                end_time = datetime.datetime.now()
                run_time = end_time - start_time

                self.event_log_update(run_time, logger)
                winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)

                logger.removeHandler(f_handler)
                logger.removeHandler(c_handler)
                logging.shutdown()

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
        driver = webdriver.Edge(service=Service(), options=options)
        # driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        # driver = webdriver.Chrome(service=Service
        # (ChromeDriverManager(version='114.0.5735.90').install()), options = options)
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
            page_results = driver.page_source
            soup = BeautifulSoup(page_results, 'html.parser')
            self.area_results(soup)
            self.current_data_avail(soup)

        except TimeoutException as te:
            logger.exception(f'Timeout Error Occurred: {te}')

        except NoSuchElementException as nse:
            logger.exception(f'So Such Element Was Found: {nse}')

        except ElementNotVisibleException as env:
            logger.exception(f'The Element Is Not Visible: {env}')

        except Exception as e:
            logger.exception(f'An Unhandled Error Has Occurred: {e} ')

        else:
            logger.info(f'BeautifulSoup Has Run for NJ Realtor Successfully')
            logger.removeHandler(f_handler)
            logger.removeHandler(c_handler)
            logging.shutdown()

    # Function which will organize the PDF Temp Files Folder by year, month and city
    def OrganizeFiles(self, results_from_corrupt):
        print('Now organizing files into folders separated by year and city...')
        base_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder\\PDF Temp Files'
        target_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder'

        os.chdir(target_path)

        for root, dirs, filenames in os.walk(target_path):
            # If the first index of the list is 'No Corrupted Files', the send2trash function isn't necessary
            if results_from_corrupt[0] == 'No Corrupted Files':
                for filename in filenames:
                    if filename.endswith('.pdf'):
                        target = filename.rstrip('.pdf').split(' ')
                        year = target[-1]
                        # month = target[-2]
                        city = ' '.join(target[:len(target) - 2])

                        first_path = os.path.join(base_path, year)
                        second_path = os.path.join(first_path, city)
                        abs_file_path = os.path.abspath(filename)
                        # If the first target path exists, check to see if the city path exists as well
                        if os.path.exists(first_path):

                            if os.path.exists(second_path):
                                shutil.move(abs_file_path, second_path)

                            elif not os.path.exists(second_path):
                                os.makedirs(second_path)
                                shutil.move(abs_file_path, second_path)

                        elif not os.path.exists(first_path):
                            os.makedirs(second_path)
                            shutil.move(abs_file_path, second_path)
                    else:
                        continue
            else:
                for filename in filenames:
                    # If the filename is in the List, it is labeled corrupted and sent to the Trash Bin
                    if filename in results_from_corrupt:
                        send2trash(filename)
                    else:
                        if filename.endswith('.pdf'):
                            target = filename.rstrip('.pdf').split(' ')
                            year = target[-1]
                            month = target[-2]
                            city = ' '.join(target[:len(target) - 2])

                            first_path = os.path.join(base_path, year)
                            second_path = os.path.join(first_path, city)
                            abs_file_path = os.path.abspath(filename)
                            # If the first target path exists, check to see if the city path exists as well
                            if os.path.exists(first_path):

                                if os.path.exists(second_path):
                                    shutil.move(abs_file_path, second_path)

                                elif not os.path.exists(second_path):
                                    os.makedirs(second_path)
                                    shutil.move(abs_file_path, second_path)

                            elif not os.path.exists(first_path):
                                os.makedirs(second_path)
                                shutil.move(abs_file_path, second_path)
                        else:
                            continue
        print('All files have been organized...')

    def pandas2excel(self, dict1, dict2):
        """ The dict arguement will be a nested dictionary.
              dict = {'2019' : {'New Listings': [],
                                'Closed Sales' : [],
                                etc}"""
        print('Storing Quarterly and Full Year Data into an Excel Spreadsheet...')
        previous_dir = os.getcwd()
        os.chdir('F:\\Real Estate Investing\\JQH Holding Company LLC\\Real Estate Data')
        # Store the dfs in one Excel file under different sheets for later processing
        with pd.ExcelWriter('NJ 10k Real Estate Data ' + str(datetime.datetime.today().date()) + '.xlsx') as writer:
            list1 = []
            list2 = []
            for k in dict1.keys():
                df = pd.DataFrame(dict1[k])
                list1.append(df)
                df1 = pd.DataFrame(dict2[k])
                list2.append(df1)
                df.to_excel(writer, sheet_name= k + ' By Qtr')
                df1.to_excel(writer, sheet_name= k + ' Full Year')

                # Create a function which joins/unions all the dfs together from the lists
        os.chdir(previous_dir)
        print('Quarterly and Full Year dataframes have been created and stored in Excel\n'
              f'Filename: NJ 10k Real Estate Data {str(datetime.datetime.today().date())}')

    # Generator function which will be used in tandem with the extract_re_data function to put data into main dictionary
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

    def quarter(self, month):
        if month in ['January', 'February', 'March']:
            quarter = 'Q1'
        elif month in ['April', 'May', 'June']:
            quarter = 'Q2'
        elif month in ['July', 'August', 'September']:
            quarter = 'Q3'
        elif month in ['October', 'November', 'December']:
            quarter = 'Q4'

        return quarter


    # A function I can use for debuggin purposes.
    # In the event a failure occurs before reaching the extract_re_data function
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

                    key_names = ['Run Type', 'Latest Available Data', 'Run Time', 'Run Date', 'Days Between Update']
                    Scraper.event_log.setdefault(int(ev_pattern.search(i).group(1)), {})

                    for kn in key_names:
                        Scraper.event_log[Scraper.no_of_runs].setdefault(kn, '')

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
    # This function will compare the last key-value pair of the class event_log variable
    # and the results of the current_data_avail function. If the values are not the same, run the program
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

                    # Takes the name of the city from the list and splits the string at the space, then joins the strings in the newly created list
                    # This is needed to use in the url3 variable to access the correct 10k pdfs
                    city0 = i.split(' ')
                    city = ''.join(i.split(' '))
                    for y in years:

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

            except AssertionError as AE:
                # The program should not be allowed to continue should this error occur
                logger.exception(f'An AssertionError Has Occured:\n{AE}')


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

                self.event_log_update(run_time, logger)
                winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)

                logger.removeHandler(f_handler)
                logger.removeHandler(c_handler)
                logging.shutdown()

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

    full_year = {
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

        os.chdir('C:\\Users\\Omar\\Desktop\\Python Temp Folder')

    else:
        obj = Scraper()

    # # load the shelf file for the saved data
    # # If this code has never been run before, the full NJR10k will need to be run all the way back from 2018
    if obj.no_of_runs == 0:
        # obj.njrdata()
        results = obj.njr10k()
        # results = obj.read_logger()
        # The NJR10k function will return a list if the pdfs found to be possibly corrupted
        # If length of the list is created than 0, the program will trigger the next function to download corrupted data
        if results == 'All Files Downloaded':
            # Read latest logger file to get a list of the corrupted files
            results1 = obj.corrupted_files(obj.read_logger())

        elif len(results) > 0:
            results1 = obj.corrupted_files(results)
        else:
            results1 = ['No Corrupted Files']

        print('Beginning PDF extraction...')
        time.sleep(1)
        for pdf in obj.pdf_generator():
            obj.extract_re_data(pdf, results1)

        winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)

        old_dir = os.getcwd()
        # Use the Shelve module to save data for later use
        print('Saving your data...')
        os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')
        with shelve.open('NJ Scrapper Data Dictionary') as saved_data_file:
            saved_data_file['Main Dictionary'] = main_dictionary
            saved_data_file['Full Year'] = full_year
            saved_data_file['Event Log'] = Scraper.event_log

        os.chdir(old_dir)

        # Organize all the files into their respective folders according to the year of the data
        obj.OrganizeFiles(results1)

        now = datetime.datetime.strptime(time.ctime(), "%a %b %d %H:%M:%S %Y")
        end_of_year = datetime.datetime.strptime(Scraper.current_data_avail.split(' ')[1] + "/12/31", "%Y/%m/%d")

        # If today's date is the last day of the year or greater run zip functino. If not, stay sleep
        if now >= end_of_year:
            obj.CreateZip()

        obj.pandas2excel(main_dictionary, full_year)

    # If this code has been run before, the Updated NJR10k will need to be run from last pulled data
    elif obj.no_of_runs > 0:
        # obj.njrdata()
        while obj.check_results() == 'No Results':
            pass
            # if Scraper.event_log[Scraper.no_of_runs]['Run Time'] <= todays_date
            #     if todays_date <= Next_month:
            #         waiting(time difference between today and next month)
            #     elif todays_date = > Next_month:
            #         waiting(24 hours)
        else:
            start1, finish1 = obj.check_results()
            results = obj.update_njr10k(start1,finish1)
            if results == 'All Files Downloaded':
                # Read latest logger file to get a list of the corrupted files
                results1 = obj.corrupted_files(obj.read_logger())

            elif len(results) > 0:
                results1 = obj.corrupted_files(results)
            else:
                results1 = ['No Corrupted Files']

        old_dir = os.getcwd()
        # Use the Shelve module to save data for later use
        os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')
        with shelve.open('NJ Scrapper Data Dictionary') as saved_data_file:
            main_dictionary = saved_data_file['Main Dictionary']
            full_year = saved_data_file['Full Year']

        os.chdir(old_dir)
        print('Beginning PDF extraction...')
        time.sleep(1)
        for pdf in obj.pdf_generator():
            obj.extract_re_data(pdf, results1)

        winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)

        os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')
        with shelve.open('NJ Scrapper Data Dictionary', writeback=True) as saved_data_file:
            saved_data_file['Main Dictionary'] = main_dictionary
            saved_data_file['Full Year'] = full_year
            saved_data_file['Event Log'] = Scraper.event_log
            saved_data_file.sync()

        os.chdir(old_dir)
        obj.OrganizeFiles(results1)

        now = datetime.datetime.strptime(time.ctime(), "%a %b %d %H:%M:%S %Y")
        end_of_year = datetime.datetime.strptime(Scraper.current_data_avail.split(' ')[1] + "/12/31", "%Y/%m/%d")

        # If today's date is the last day of the year or greater run zip functino. If not, stay sleep
        if now >= end_of_year:
            obj.CreateZip()

        # Load previous pandas df and put new dates in
        obj.pandas2excel(main_dictionary, full_year)

