import os
import winsound
import openpyxl
import PyPDF2
import shutil
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
                    city = ''.join(i.split(' '))
                    for y in self.__years:
                        # time.sleep(0.1)
                        # year_tracker.update()
                        if y == '2019':
                            # If 2019 is the year, there is no data available from January to Sept. And some cities dont have data available for Sept
                            months1 = months[8:13]
                            for m in months1:
                                # time.sleep(0.1)
                                # months_tracker.update()
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
                                if v == Scraper.current_data_avail.split(' ')[0]:
                                    months1 = months[:months.index(k) + 1]
                            for m in months1:
                                # time.sleep(0.1)
                                # months_tracker.update()
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
                                                             'Run Time' : run_time,
                                                             'Run Date' : time.ctime(),
                                                             'Days Between Update' : 0 }
                    logger.info(f'New Event Log Created: \n{Scraper.event_log}')
                elif Scraper.no_of_runs > 0:
                    Scraper.event_log[Scraper.no_of_runs] = {'Run Type' : name,
                                                             'Latest Available Data': self.current_data_avail,
                                                             'Run Time' : run_time,
                                                             'Run Date': time.ctime(),
                                                             'Days Between Update': self.daysuntilupdate(Scraper.no_of_runs)}
                    logger.info(f'New Event Log Created: \n{Scraper.event_log}')
                Scraper.no_of_runs += 1
                #This is a very lengthy program so I'd like play a sound that signifies the process is done
                winsound.PlaySound('F:\\Python 2.0\\SoundFiles\\Victory.wav', 0)

        return possible_corrupted_files

    # Function which calculates the difference between the current download date and previous date
    # Use this to calculate the average amount of time it takes between new update periods
    def daysuntilupdate(self, no_of_runs):
        current = Scraper.no_of_runs
        previous = current - 1
        current_date = datetime.datetime.now()
        previous_date = datetime.datetime.strptime(Scraper.event_log[previous]['Run Date'], "%a %b %d %H:%M:%S %Y")
        delta = current_date - previous_date

        return delta.days

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


    # Function which scrapes the cities and counties from the njrealtor 10k state page
    def area_results(self, soup):
        area = soup.find('select', id="lmuArea").children
        for obj in area:
            newobj = obj.get_text()
            if newobj in ['Select an area...', 'Entire State', '\n']:
                continue
            else:
                if 'County' in newobj:
                    #I believe there are multiple towns in different counties with the same name. Their county is attached
                    #and need to be seperated from the target values
                    if '/' in newobj:
                        newobj = newobj.split('/')
                        city = newobj[0]
                        self.__towns.append(city)
                    else:
                        self.__counties.append(newobj)
                else:
                    self.__towns.append(newobj)

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
        previous_year = str(int(year) -1)
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
            main_dictionary[current_year].setdefault('New Listings % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Closed Sales % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + previous_year,[])
            main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + current_year,[])
            main_dictionary[current_year].setdefault('Percent of Listing Price Received % Change ' + month + ' (YoY)',[])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Months of Supply % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year]['City'].append(city)
            main_dictionary[current_year]['County'].append(county)
            main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(new_listings_previous)
            main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(new_listings_current)
            main_dictionary[current_year]['New Listings % Change ' + month + ' (YoY)'].append(new_listings_per_change)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(closed_sales_previous)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(closed_sales_current)
            main_dictionary[current_year]['Closed Sales % Change ' + month + ' (YoY)'].append(closed_sales_per_change)
            main_dictionary[current_year]['Days on Market ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Days on Market ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
            main_dictionary[current_year]['Median Sales Price ' + month + ' ' + previous_year].append(median_sales_previous)
            main_dictionary[current_year]['Median Sales Price ' + month + ' ' + current_year].append(median_sales_current)
            main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(median_sales_per_change)
            main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
            main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + current_year].append(percent_lpr_current)
            main_dictionary[current_year]['Percent of Listing Price Received % Change ' + month + ' (YoY)'].append(percent_lpr_per_change)
            main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + previous_year].append(inventory_previous)
            main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + current_year].append(inventory_current)
            main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(inventory_per_change)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Months of Supply % Change ' + month + ' (YoY)'].append(DOM_per_change)

        # For each city inserted into the dictionary, a new data column needs to be created for each month
        # I'm tracking 21 data columns. So for a every city created, 21 new data columns are needed
        # Only 21 data columns need to be created if the city already exists in the dictionary. City would be appended 12x if not for this elif block
        elif main_dictionary[current_year] != {} and city in main_dictionary[current_year]['City']:

            main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('New Listings % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Closed Sales % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + previous_year,[])
            main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + current_year,[])
            main_dictionary[current_year].setdefault('Percent of Listing Price Received % Change ' + month + ' (YoY)',[])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Months of Supply % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(new_listings_previous)
            main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(new_listings_current)
            main_dictionary[current_year]['New Listings % Change ' + month + ' (YoY)'].append(new_listings_per_change)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(closed_sales_previous)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(closed_sales_current)
            main_dictionary[current_year]['Closed Sales % Change ' + month + ' (YoY)'].append(closed_sales_per_change)
            main_dictionary[current_year]['Days on Market ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Days on Market ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
            main_dictionary[current_year]['Median Sales Price ' + month + ' ' + previous_year].append(median_sales_previous)
            main_dictionary[current_year]['Median Sales Price ' + month + ' ' + current_year].append(median_sales_current)
            main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(median_sales_per_change)
            main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
            main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + current_year].append(percent_lpr_current)
            main_dictionary[current_year]['Percent of Listing Price Received % Change ' + month + ' (YoY)'].append(percent_lpr_per_change)
            main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + previous_year].append(inventory_previous)
            main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + current_year].append(inventory_current)
            main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(inventory_per_change)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Months of Supply % Change ' + month + ' (YoY)'].append(DOM_per_change)

        # For each city inserted into the dictionary, a new data column needs to be created for each month
        # I'm tracking 21 data columns. So for a every city created, 21 new data columns are needed
        # The city and county need to be appended to the existing list & 21 new data columns created
        elif main_dictionary[current_year] != {} and city not in main_dictionary[current_year]['City']:
            main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('New Listings % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Closed Sales % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + previous_year,[])
            main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + current_year,[])
            main_dictionary[current_year].setdefault('Percent of Listing Price Received % Change ' + month + ' (YoY)',[])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year, [])
            main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year, [])
            main_dictionary[current_year].setdefault('Months of Supply % Change ' + month + ' (YoY)', [])
            main_dictionary[current_year]['City'].append(city)
            main_dictionary[current_year]['County'].append(county)
            main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(new_listings_previous)
            main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(new_listings_current)
            main_dictionary[current_year]['New Listings % Change ' + month + ' (YoY)'].append(new_listings_per_change)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(closed_sales_previous)
            main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(closed_sales_current)
            main_dictionary[current_year]['Closed Sales % Change ' + month + ' (YoY)'].append(closed_sales_per_change)
            main_dictionary[current_year]['Days on Market ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Days on Market ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
            main_dictionary[current_year]['Median Sales Price ' + month + ' ' + previous_year].append(median_sales_previous)
            main_dictionary[current_year]['Median Sales Price ' + month + ' ' + current_year].append(median_sales_current)
            main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(median_sales_per_change)
            main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
            main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + current_year].append(percent_lpr_current)
            main_dictionary[current_year]['Percent of Listing Price Received % Change ' + month + ' (YoY)'].append(percent_lpr_per_change)
            main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + previous_year].append(inventory_previous)
            main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + current_year].append(inventory_current)
            main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(inventory_per_change)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(DOM_previous)
            main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
            main_dictionary[current_year]['Months of Supply % Change ' + month + ' (YoY)'].append(DOM_per_change)

    # Function which extracts the month, current and previous year, new listing, closing sales, DOM, median sales, etc
    # Data will then be stored in a dictionary
    def extract_re_data(self, pdfname):

        logger = logging.getLogger("Extract_Data")
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
        # Information will be used in data_na function
        info = pdfname.rstrip('.pdf').split(' ')
        town = info[0:len(info) - 2]
        if len(town) > 1:
            town = ' '.join(town)
        else:
            town = info[0]
        month = info[-2]
        year = info[-1]

        try:

            pdfread = PyPDF2.PdfReader(pdfname)
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

            month_pattern = re.compile(r'(January|February|March|April|May|June|July|August|September|October|November|December)\sYear\sto\sDate\sSingle\sFamily')
            month = month_pattern.search(target)
            month = month.group()[0:3]
            key_metrics_basic_pattern = re.compile(r'Key\sMetrics\s(\d{4})\s(\d{4})\sPercent\sChange\sThru\s\d{1,2}?-\d{4}\sThru\s\d{1,2}?-\d{4}\sPercent\sChange')
            km_search = list(key_metrics_basic_pattern.findall(target))
            current_year = km_search[0][1]
            previous_year = str(int(current_year) - 1)
            new_listings_pattern = re.compile(r'New\sListings\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)')
            new_listing_search = list(new_listings_pattern.findall(target))
            new_listings_current = int(new_listing_search[0][1])
            new_listings_previous = int(new_listing_search[0][0])
            new_listings_pc = str(new_listing_search[0][2]).split(' ')
            new_listings_per_change = ''.join(new_listings_pc).rstrip('%')
            if '+' in new_listings_per_change:
                new_listings_per_change.lstrip('+')
                new_listings_per_change = float(new_listings_per_change) / 100
            elif '--' in new_listings_per_change:
                new_listings_per_change = 0.0
            else:
                new_listings_per_change = float(new_listings_per_change) / 100
            closed_sales_pattern = re.compile(r'Closed\sSales\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)')
            closed_sales_search = list(closed_sales_pattern.findall(target))
            closed_sales_current = int(closed_sales_search[0][1])
            closed_sales_previous = int(closed_sales_search[0][0])
            closed_sales_pc = closed_sales_search[0][2].split(' ')
            closed_sales_per_change = ''.join(closed_sales_pc).rstrip('%')
            if '+' in closed_sales_per_change:
                closed_sales_per_change.lstrip('+')
                closed_sales_per_change = float(closed_sales_per_change) / 100
            elif '--' in closed_sales_per_change:
                closed_sales_per_change = 0.0
            else:
                closed_sales_per_change = float(closed_sales_per_change) / 100
            DOM_pattern = re.compile(r'Days\son\sMarket\sUntil\sSale\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)\s(\d{0,3}?)\s(\d{0,3}?)\s(--|[+-]\s\d{0,3}?.\d{0,1}?%)')
            DOM_search = list(DOM_pattern.findall(target))
            DOM_current = int(DOM_search[0][1])
            DOM_previous = int(DOM_search[0][0])
            DOM_pc = DOM_search[0][2].split(' ')
            DOM_per_change = ''.join(DOM_pc).rstrip('%')
            if '+' in DOM_per_change:
                DOM_per_change.lstrip('+')
                DOM_per_change = float(DOM_per_change) / 100
            elif '--' in DOM_per_change:
                DOM_per_change = 0.0
            else:
                DOM_per_change = float(DOM_per_change) / 100
            median_sales_pattern = re.compile(r'Median\sSales\sPrice\*\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(--|[+-]\s\d{1,3}?.\d{1}%)\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(\$\d{1}|\$\d{0,3}?,?\d{0,3}?,\d{1,3})\s(--|[+-]\s\d{1,3}?.\d{1}%)')
            median_sales_search = list(median_sales_pattern.findall(target))
            median_sales_current = median_sales_search[0][1]
            median_sales_current = int("".join(median_sales_current.split(',')).lstrip('$'))
            median_sales_previous = median_sales_search[0][0]
            median_sales_previous = int("".join(median_sales_previous.split(',')).lstrip('$'))
            median_sales_pc = median_sales_search[0][2].split(' ')
            median_sales_per_change = ''.join(median_sales_pc).rstrip('%')
            if '+' in median_sales_per_change:
                median_sales_per_change.lstrip('+')
                median_sales_per_change = float(median_sales_per_change) / 100
            elif '--' in median_sales_per_change:
                median_sales_per_change = 0.0
            else:
                median_sales_per_change = float(median_sales_per_change) / 100
            percent_lpr_pattern = re.compile(r'Percent\sof\sList\sPrice\sReceived\*\s(\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(--|[+-]\s\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(\d{1,3}?.\d{1}%)\s(--|[+-]\s\d{1,3}?.\d{1}%)')
            percent_lpr_search = list(percent_lpr_pattern.findall(target))
            # Divide this by 100 and figure out how to format these to show the percent sign
            percent_lpr_current = float(percent_lpr_search[0][1].rstrip('%'))
            percent_lpr_previous = float(percent_lpr_search[0][0].rstrip('%'))
            percent_lpr_pc = percent_lpr_search[0][2].split(' ')
            percent_lpr_per_change = ''.join(percent_lpr_pc).rstrip('%')
            if '+' in percent_lpr_per_change:
                percent_lpr_per_change.lstrip('+')
                percent_lpr_per_change = float(percent_lpr_per_change) / 100
            elif '--' in percent_lpr_per_change:
                percent_lpr_per_change = 0.0
            else:
                percent_lpr_per_change = float(percent_lpr_per_change) / 100
            inventory_pattern = re.compile(r'Inventory\sof\sHomes\sfor\sSale\s(--|\d{0,3}?)\s(--|\d{0,3}?)\s(--|[+-]\s\d{1,3}?.\d{1}%)\s(--|\d{0,3}?)\s(--|\d{0,3}?)\s(--|[+-]\s\d{1,3}?.\d{1}%)')
            inventory_search = list(inventory_pattern.findall(target))
            inventory_current = int(inventory_search[0][1])
            inventory_previous = int(inventory_search[0][0])
            inventory_pc = inventory_search[0][2].split(' ')
            inventory_per_change = ''.join(inventory_pc).rstrip('%')
            if '+' in inventory_per_change:
                inventory_per_change.lstrip('+')
                inventory_per_change = float(inventory_per_change) / 100
            elif '--' in inventory_per_change:
                inventory_per_change = 0.0
            else:
                inventory_per_change = float(inventory_per_change) / 100
            supply_pattern = re.compile(r'Months\sSupply\sof\sInventory\s(--|\d{1,2}?.\d{1})\s(--|\d{1,2}?.\d{1})\s(--|[+-]\s\d{1,3}?.\d{1}%)\s(--|\d{1,2}?.\d{1})\s(--|\d{1,2}?.\d{1})\s(--|[+-]\s\d{1,3}?.\d{1}%)')
            supply_search = list(supply_pattern.findall(target))
            supply_current = float(supply_search[0][1])
            supply_previous = float(supply_search[0][0])
            supply_pc = supply_search[0][2].split(' ')
            supply_per_change = ''.join(supply_pc).rstrip('%')
            if '+' in supply_per_change:
                supply_per_change.lstrip('+')
                supply_per_change = float(supply_per_change) / 100
            elif '--' in supply_per_change:
                supply_per_change = 0.0
            else:
                supply_per_change = float(supply_per_change) / 100


            if main_dictionary[current_year] == {}:
                #self.create_dictionary(month, current_year)
                main_dictionary[current_year].setdefault('City', [])
                main_dictionary[current_year].setdefault('County', [])
                main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('New Listings % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Closed Sales % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Percent of Listing Price Received % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + previous_year,[])
                main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + current_year,[])
                main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Months of Supply % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year]['City'].append(city)
                main_dictionary[current_year]['County'].append(county)
                main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(new_listings_previous)
                main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(new_listings_current)
                main_dictionary[current_year]['New Listings % Change ' + month + ' (YoY)'].append(new_listings_per_change)
                main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(closed_sales_previous)
                main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(closed_sales_current)
                main_dictionary[current_year]['Closed Sales % Change ' + month + ' (YoY)'].append(closed_sales_per_change)
                main_dictionary[current_year]['Days on Market ' + month + ' ' + previous_year].append(DOM_previous)
                main_dictionary[current_year]['Days on Market ' + month + ' ' + current_year].append(DOM_current)
                main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
                main_dictionary[current_year]['Median Sales Price ' + month + ' ' + previous_year].append(median_sales_previous)
                main_dictionary[current_year]['Median Sales Price ' + month + ' ' + current_year].append(median_sales_current)
                main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(median_sales_per_change)
                main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
                main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + current_year].append(percent_lpr_current)
                main_dictionary[current_year]['Percent of Listing Price Received % Change ' + month + ' (YoY)'].append(percent_lpr_per_change)
                main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + previous_year].append(inventory_previous)
                main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + current_year].append(inventory_current)
                main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(inventory_per_change)
                main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(DOM_previous)
                main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
                main_dictionary[current_year]['Months of Supply % Change ' + month + ' (YoY)'].append(DOM_per_change)

            # For each city inserted into the dictionary, a new data column needs to be created for each month
            # I'm tracking 21 data columns. So for a every city created, 21 new data columns are needed
            # Only 21 data columns need to be created if the city already exists in the dictionary. City would be appended 12x if not for this elif block
            elif main_dictionary[current_year] != {} and city in main_dictionary[current_year]['City']:

                main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('New Listings % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Closed Sales % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Percent of Listing Price Received % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year,[])
                main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year,[])
                main_dictionary[current_year].setdefault('Months of Supply % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(new_listings_previous)
                main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(new_listings_current)
                main_dictionary[current_year]['New Listings % Change ' + month + ' (YoY)'].append(new_listings_per_change)
                main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(closed_sales_previous)
                main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(closed_sales_current)
                main_dictionary[current_year]['Closed Sales % Change ' + month + ' (YoY)'].append(closed_sales_per_change)
                main_dictionary[current_year]['Days on Market ' + month + ' ' + previous_year].append(DOM_previous)
                main_dictionary[current_year]['Days on Market ' + month + ' ' + current_year].append(DOM_current)
                main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
                main_dictionary[current_year]['Median Sales Price ' + month + ' ' + previous_year].append(median_sales_previous)
                main_dictionary[current_year]['Median Sales Price ' + month + ' ' + current_year].append(median_sales_current)
                main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(median_sales_per_change)
                main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
                main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + current_year].append(percent_lpr_current)
                main_dictionary[current_year]['Percent of Listing Price Received % Change ' + month + ' (YoY)'].append(percent_lpr_per_change)
                main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + previous_year].append(inventory_previous)
                main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + current_year].append(inventory_current)
                main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(inventory_per_change)
                main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(DOM_previous)
                main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
                main_dictionary[current_year]['Months of Supply % Change ' + month + ' (YoY)'].append(DOM_per_change)

            # For each city inserted into the dictionary, a new data column needs to be created for each month
            # I'm tracking 21 data columns. So for a every city created, 21 new data columns are needed
            # The city and county need to be appended to the existing list & 21 new data columns created
            elif main_dictionary[current_year] != {} and city not in main_dictionary[current_year]['City']:
                main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('New Listings ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('New Listings % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Closed Sales ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Closed Sales % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Days on Market ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Days on Market % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Median Sales Price ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Median Sales Price % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Percent of Listing Price Received ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Percent of Listing Price Received % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + previous_year,[])
                main_dictionary[current_year].setdefault('Inventory of Homes for Sale ' + month + ' ' + current_year,[])
                main_dictionary[current_year].setdefault('Inventory of Homes for Sale % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + previous_year, [])
                main_dictionary[current_year].setdefault('Months of Supply ' + month + ' ' + current_year, [])
                main_dictionary[current_year].setdefault('Months of Supply % Change ' + month + ' (YoY)', [])
                main_dictionary[current_year]['City'].append(city)
                main_dictionary[current_year]['County'].append(county)
                main_dictionary[current_year]['New Listings ' + month + ' ' + previous_year].append(new_listings_previous)
                main_dictionary[current_year]['New Listings ' + month + ' ' + current_year].append(new_listings_current)
                main_dictionary[current_year]['New Listings % Change ' + month + ' (YoY)'].append(new_listings_per_change)
                main_dictionary[current_year]['Closed Sales ' + month + ' ' + previous_year].append(closed_sales_previous)
                main_dictionary[current_year]['Closed Sales ' + month + ' ' + current_year].append(closed_sales_current)
                main_dictionary[current_year]['Closed Sales % Change ' + month + ' (YoY)'].append(closed_sales_per_change)
                main_dictionary[current_year]['Days on Market ' + month + ' ' + previous_year].append(DOM_previous)
                main_dictionary[current_year]['Days on Market ' + month + ' ' + current_year].append(DOM_current)
                main_dictionary[current_year]['Days on Market % Change ' + month + ' (YoY)'].append(DOM_per_change)
                main_dictionary[current_year]['Median Sales Price ' + month + ' ' + previous_year].append(median_sales_previous)
                main_dictionary[current_year]['Median Sales Price ' + month + ' ' + current_year].append(median_sales_current)
                main_dictionary[current_year]['Median Sales Price % Change ' + month + ' (YoY)'].append(median_sales_per_change)
                main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + previous_year].append(percent_lpr_previous)
                main_dictionary[current_year]['Percent of Listing Price Received ' + month + ' ' + current_year].append(percent_lpr_current)
                main_dictionary[current_year]['Percent of Listing Price Received % Change ' + month + ' (YoY)'].append(percent_lpr_per_change)
                main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + previous_year].append(inventory_previous)
                main_dictionary[current_year]['Inventory of Homes for Sale ' + month + ' ' + current_year].append(inventory_current)
                main_dictionary[current_year]['Inventory of Homes for Sale % Change ' + month + ' (YoY)'].append(inventory_per_change)
                main_dictionary[current_year]['Months of Supply ' + month + ' ' + previous_year].append(DOM_previous)
                main_dictionary[current_year]['Months of Supply ' + month + ' ' + current_year].append(DOM_current)
                main_dictionary[current_year]['Months of Supply % Change ' + month + ' (YoY)'].append(DOM_per_change)

        except re.error as ree:
            logger.exception(f'A Regex Error Has Occurred: {ree}')

        except PyPDF2._reader.EmptyFileError as efe:
            logger.exception(f'An Error Has Occured (File Possibly Corrupted): {efe}')
            logger.info(f'The city of {town} for {month} {year} will have no data')
            # If function encounters an empty/ corrupted pdf, the data_na function will render all information available for that file equal to zero
            # The generator will then pick up at the next pdf to continue extracting data
            self.data_na(town, month, year)
            for pdf in self.pdf_generator(pdfname):
                self.extract_re_data(pdf)

        except:
            logger.exception(f'A Regex Error Has Occurred')




        pprint.pprint(main_dictionary)

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
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')
        # Set the formatter for each handler
        f_handler.setFormatter(formatter)
        c_handler.setFormatter(formatter)
        logger.addHandler(f_handler)
        logger.addHandler(c_handler)

        options = Options()
        # Change this directory to the new one: ('C:\\Users\\Omar\\Desktop\\Python Temp Folder')
        s = {"savefile.default_directory": 'C:\\Users\\Omar\\Desktop\\Selenium Temp Folder'}
        #options.add_argument('window-postion=2000,0')
        #options.add_experimental_option("detach", True)
        options.add_experimental_option("prefs", s)
        options.add_argument("--headless=new")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
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

    # Generator function which will be used in tandem with the extractre_data function to put data into main dictionary
    def pdf_generator(self, pdfname = None):

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
                filenames = filenames[filename.index(pdfname) + 1 :]
                for filename in filenames:
                    if filename.endswith('.pdf'):
                        yield filename
                    else:
                        continue


    # Function which will organize the PDF Temp Files Folder by year, month and city
    def OrganizeFiles(self, list):

        base_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder\\PDF Temp Files'
        target_path = 'C:\\Users\\Omar\\Desktop\\Python Temp Folder'
        years = {}
        for root, dirs, filenames in os.walk(target_path):
            # If the first index of the list is 'No Corrupted Files', the send2trash function isnt necessary
            if list[0] == 'No Corrupted Files':
                for filename in filenames:
                    target = filename.rstrip('.pdf').split(' ')
                    year = target[-1]
                    month = target[-2]
                    city = ' '.join(target[:len(target) - 2])
                    if year not in years:
                        years.add(year)
                    for i in years:
                        first_path = base_path + '\\' + i
                        second_path = first_path + '\\' + month
                        third_path = second_path + '\\' + city
                        # If the first target path exists, check to see if the month and city paths exists as well
                        if os.path.exists(first_path):

                            if os.path.exists(second_path):

                                if os.path.exists(third_path):
                                    shutil.move(filename, third_path)

                                elif os.path.exists(third_path) == False:
                                    os.mkdir(city)
                                    shutil.move(filename, third_path)

                            elif os.path.exists(second_path) == False:
                                os.makedirs(month + '\\' + city)
                                shutil.move(filename, third_path)

                        elif os.path.exists(first_path) == False:
                            os.makedirs(i + '\\' + month + '\\' + city)
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
                        if year not in years:
                            years.add(year)
                        for i in years:
                            first_path = base_path + '\\' + i
                            second_path = first_path + '\\' + month
                            third_path = second_path + '\\' + city
                            # If the first target path exists, check to see if the month and city paths exists as well
                            if os.path.exists(first_path):

                                if os.path.exists(second_path):

                                    if os.path.exists(third_path):
                                        shutil.move(filename, third_path)

                                    elif os.path.exists(third_path) == False:
                                        os.mkdir(city)
                                        shutil.move(filename, third_path)

                                elif os.path.exists(second_path) == False:
                                    os.makedirs(month + '\\' + city)
                                    shutil.move(filename, third_path)

                            elif os.path.exists(first_path) == False:
                                os.makedirs(i + '\\' + month + '\\' + city)
                                shutil.move(filename, third_path)


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
                town = ' '.join(town)
            else:
                town = info[0]
            month = info[-2]
            year = info[-1]

            if year == '2019':
                possible_corrupted_files.append(i)
                continue
            else:
                for m, i in enumerate(self.__towns):
                    if town in i:
                        town = self.__towns[m]
                        dict[n] = [town, month, year]

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
        zip_folder = 'F:\\Python 2.0\\Projects\\Real Life Projects\\NJR10k Zips'
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






if __name__ == '__main__':
    #l1 = ['Aberdeen September 2019.pdf', 'Aberdeen October 2019.pdf']
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
    obj = Scraper()
    # #load the shelf file for the saved data
    # #If this code has never been run before, the full NJR10k will need to be run all the way back from 2018
    if obj.no_of_runs == 0:
        obj.njrdata()
        # print(Scraper.current_data_avail)
        # print(obj._Scraper__towns)
        results = obj.njr10k()
        #The NJR10k function will return a list if there and pdfs found to be possibly corrupted
        #If length of the list is created than 0, the program will trigger the next function to download corrupted data
        if results == 'All Files Downloaded':
            # Read latest logger file to get a list of the corrupted files
            # results1 = obj.corrupted_files(obj.read_logger())
            pass
        elif len(results) > 0:
            results1 = obj.corrupted_files(results)
        else:
            results1 = ['No Corrupted Files']

        for pdf in obj.pdf_generator():
            obj.extract_re_data(pdf)
        #Organize all the files into their respective folders according to the year of the data
        obj.OrganizeFiles(results1)

        now = datetime.datetime.strptime(time.ctime(), "%a %b %d %H:%M:%S %Y")
        end_of_year = datetime.datetime.strptime(Scraper.current_data_avail.split(' ')[0] + "/12/31", "%Y/%m/%d")

        #If todays date is the last day of the year or greater run zip functino. If not, stay sleep
        if now >= end_of_year:
            obj.CreateZip()

        #Run the function that turns the main_dictionary into a Pandas dataframe
        #Create seperate dataframes for the year 2018 to 2023 (this holds columns for all 12 individual months)
        #Create seperate dataframes for Q1 - Q4 for every year
        #Create a full year dataframe for 2018 to 2023 (this hold columns for the full year data pulled from December pdfs)
        #Run Pandas2Excel which will put all dataframes in a single file in their own respective tabs

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

        for pdf in obj.pdf_generator():
            obj.extract_re_data(pdf)

        obj.OrganizeFiles(results1)

        now = datetime.datetime.strptime(time.ctime(), "%a %b %d %H:%M:%S %Y")
        end_of_year = datetime.datetime.strptime(Scraper.current_data_avail.split(' ')[0] + "/12/31", "%Y/%m/%d")

        # If todays date is the last day of the year or greater run zip functino. If not, stay sleep
        if now >= end_of_year:
            obj.CreateZip()

        # Run the function that uplaods the main file, create Pandas dataframes from the new data and append them to respective dictionaries
        # Update seperate dataframes for the current year (this holds columns for all 12 individual months)
        # Update seperate dataframes for Q1 - Q4 for the current year
        # Create a full year dataframe for the current year (this hold columns for the full year data pulled from December pdfs)
        # Run Pandas2Excel which will put all dataframes in a single file in their own respective tabs








    #
    # for i in obj.pdf_generator():
    #     obj.extract_re_data(i)
    #
    # pprint.pprint(main_dictionary)

    # username, pw = obj.get_us_pw('NJRealtor')
    # print(type(username))
    # print(type(pw))


