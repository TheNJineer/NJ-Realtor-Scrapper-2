from NJRScrapper import Scraper
import os
import time
import shelve
from pprint import pprint


def main():

    old_dir = os.getcwd()
    # Use the Shelve module to save data for later use
    os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')
    with shelve.open('NJ Scrapper Data Dictionary_v2') as saved_data_file:
        main_dictionary = saved_data_file['Main Dictionary']
        full_year = saved_data_file['Full Year']

    os.chdir(old_dir)

    obj = Scraper()
    print('Beginning PDF extraction...')
    time.sleep(1)
    obj.fill_missing_data(update_list, main_dictionary, full_year)

    os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\NJR Scrapper\\Saved Data')
    print('Saving the data for Main Dictionary, Full Year and Event Log...')
    with shelve.open('NJ Scrapper Data Dictionary_v2', writeback=True) as saved_data_file:
        saved_data_file['Main Dictionary'] = main_dictionary
        saved_data_file['Full Year'] = full_year
        saved_data_file['Event Log'] = Scraper.event_log
        saved_data_file.sync()

    print('All data has been saved...')
    os.chdir(old_dir)


update_list = ['Fairfield Twp Cumberland County', 'Fairfield Twp Essex County',
                      'Franklin Twp Gloucester County', 'Franklin Twp Hunterdon County',
                      'Franklin Twp Somerset County', 'Franklin Twp Warren County',
                      'Greenwich Twp Cumberland County', 'Greenwich Twp Gloucester County',
                      'Greenwich Twp Warren County', 'Hamilton Twp Atlantic County',
                      'Hamilton Twp Mercer County', 'Hopewell Twp Cumberland County',
                      'Hopewell Twp Mercer County', 'Lawrence Twp Cumberland County',
                      'Lawrence Twp Mercer County', 'Mansfield Twp Burlington County',
                      'Mansfield Twp Warren County', 'Monroe Twp Gloucester County',
                      'Monroe Twp Middlesex County', 'Ocean Twp Monmouth County',
                      'Ocean Twp Ocean County', 'Springfield Twp Burlington County',
                      'Springfield Twp Union County', 'Union Twp Hunterdon County',
                      'Union Twp Union County', 'Washington Twp Bergen County',
                      'Washington Twp Burlington County', 'Washington Twp Gloucester County',
                      'Washington Twp Morris County', 'Washington Twp Warren County']


if __name__ == '__main__':

    main()
