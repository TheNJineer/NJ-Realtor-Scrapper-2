import re
import pandas as pd
from tabulate import tabulate
from bs4 import BeautifulSoup
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



main_url = 'https://www.homedepot.com'
Main_Department = {"Appliances": "/b/Appliances/N-5yc1vZbv1w",
		"Flooring": "/b/Flooring/N-5yc1vZaq7r",
		"Bath & Faucets": "/b/Bath/N-5yc1vZbzb3",
		"Building Material": "/b/Building-Materials/N-5yc1vZaqns",
		"Doors & Windows": "/b/Doors-Windows/N-5yc1vZaqih",
		"Electrical": "/b/Electrical/N-5yc1vZarcd",
		"Hardware": "/b/Hardware/N-5yc1vZc21m",
		"Heating & Cooling": "/b/Heating-Venting-Cooling/N-5yc1vZc4k8",
		"Kitchen & Kitchenware": "/b/Kitchen/N-5yc1vZar4i",
		"Lighting & Ceiling Fans": "/b/Lighting/N-5yc1vZbvn5",
		"Paint": "/b/Paint/N-5yc1vZar2d",
		"Plumbing": "/b/Plumbing/N-5yc1vZbqew"
		}
'''Find a way to sort all of the sub dictionaries into the right place in the main dictionary'''
Master_Dict = {"Appliances": {},
		"Flooring": {},
		"Bath & Faucets": {},
		"Building Material": {},
		"Doors & Windows": {},
		"Electrical": {},
		"Hardware": {},
		"Heating & Cooling": {},
		"Kitchen & Kitchenware": {},
		"Lighting & Ceiling Fans": {},
		"Paint": {},
		"Plumbing": {}
}
def general_hd_scrape():
	global sub_directory_dict
	options = Options()
	#options.add_experimental_option("detach", True)
	#options.add_argument("--headless=new")
	driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
	target1 = re.compile(r'((href="/b/(\w+)(-\w+?){0,7}((/\w+(-\w+){0,5}))?/N(-\w+)")>((\w+)(\s\w+?){0,10})</a>)')
	sub_directory_dict = {
			'Category' : [],
			'Link' : []
		}

	for department, link in Main_Department.items():
		url = main_url + link
		driver.get(url)
		try:
				main_page = WebDriverWait(driver, 5).until(
                	EC.presence_of_element_located((By.XPATH, "//a[normalize-space()='Home']"))
				)
				page_html = driver.page_source
				soup = BeautifulSoup(page_html, 'lxml')
				#print(soup.prettify())
		except:
			pass

		finally:
			sub_directory_list1 = soup.find_all('a', class_="")
			# print(sub_directory_list1)
			for i in sub_directory_list1:
				#print(i)
				try:
					target2 = target1.search(str(i))
					#print(type(target2))
					link = target2.group(2)
					category = target2.group(9)
					sub_directory_dict['Category'].append(category)
					sub_directory_dict['Link'].append(link)
				except TypeError:
					print('No Match')
				except:
					pass


def hd_scrape_clean():
	global item_trash
	global sub_directory_dict
	item_trash = ['Mini Refrigerators', 'Beverage Coolers', 'Wine Coolers', 'Kegerators', 'Ice Makers', 'Vacuum Cleaners', 'Carpet Cleaners', 'Hard Surface Cleaners', 'Coffee and Espresso',
				  'Cookers', 'Mixers', 'Blenders', 'Toaster Ovens', 'Café', 'Electrolux', 'GE', 'LG', 'Samsung', 'Unique Appliances', 'Vissani', 'ZLINE', 'vacuum',
				  'microwave', 'convection oven', 'kegerator', 'wine cooler', 'ice makers', 'washing machine', 'beverage cooler', 'air purifiers', 'humidifier', 'dehumidifier', 'Area Rugs',
				  'Outdoor Rugs', 'Runner Rugs', 'Mats', 'Garage Flooring Rolls', 'Garage Floor Tile', 'Garage Floor Mats', 'Foam Gym Mats', 'Interlocking Floor Mats',
				  'Lifeproof', 'Pergo', 'Home Decorators Collection', 'white oak flooring', 'engineered hardwood flooring', 'waterproof wood flooring', 'bamboo flooring',
				  'cork flooring', 'laminate wood flooring', 'Waterproof laminate flooring', 'vinyl sheet flooring', 'vinyl plank flooring', 'luxury vinyl plank', 'patterned carpet',
				  'waterproof vinyl flooring', 'shower floor tiles', 'subway tile backsplash', 'hexagon tiles', 'mosaic tiles', 'Luxury vinyl tile', 'area rug', 'rugs', 'washable rugs',
				  'welcome mat', 'outdoor rug', 'sheepskin rug', 'jute rug', 'cowhide rugs', 'artificial turf', 'garage flooring', 'garage floor tiles', 'Bath Vanities Savings', 'Bath Faucets Savings',
				  'Bathtub Savings', 'Shower Head Savings', 'Bath Vanity Tops Savings', 'Bath Hardware Savings', 'Toilets Savings', 'Bath Sinks Savings', 'Bath Storage Savings', 'Bath Towels',
				  'Home Depot bath sale', 'bathtubs', 'bathroom faucets', 'toilets', 'wood fencing', 'Space Heaters', 'Special Offers', 'Holiday Decorations', 'Christmas Lights', 'Trimmers',
				  'Air Compressor', 'Power Tools', 'infrared thermometers', 'TV antennas', 'Safety Equipment', 'Workwear', 'Work Gloves', 'Flashlights', 'Safes', 'Batteries', 'gate latch', 'batteries', 'flashlights',
				  'Modern', 'Industrial', 'Classic', 'Rustic', 'Transitional', 'lamps', 'Low profile ceiling fans', 'modern ceiling fans', 'flood lights', 'landscape lighting', 'LED lighting', 'modern bathroom lighting',
				  'crystal chandelier', 'Bathroom sconces', 'vanity light bars', 'Pool Paint', 'Automotive Paint', 'Appliance Paint', 'Pond Paint', 'Spray Paint', 'Furniture Paint', 'Craft Paint', 'Airless Paint Sprayers',
				  'HVLP Paint Sprayers', 'Paint Brushes', 'Masking Tape', 'Packaging Tape', 'Paint Buckets', 'Paint Trays', 'Drop Cloths', 'Face Masks', 'Paint Rags', 'Sandpaper', 'Sanding Sponges', 'Heat Guns', 'Kilz', 'Zinsser',
				  'Olympic', 'Varathane', 'PlastiDip', 'Graco', 'Wagner', 'DAP', 'Wooster Pro', '3M', 'tarps', 'caulk', 'wall paint', 'mildew', 'wood stain', 'chalked paint', 'countertop paints', 'garage floor epoxies', 'interior paint',
				  'garage floor paint', 'Acrylic paints', 'spray paint', 'glass paint', 'metal paint', 'concrete stain', 'Sewer Machines', 'Drain Snakes', 'Drain Cleaners', 'Pipe Cutters', 'SharkBite', 'Rheem',
				  'RIDGID', 'Milwaukee', 'Everbilt', 'Oatey', 'BrassCraft', 'Watts', 'Viega', 'sump pumps', 'water heaters', 'pipes']
	for i in range(len(item_trash)):
		target = item_trash[i]
		if target in sub_directory_dict['Category']:
			num = sub_directory_dict['Category'].index(target)
			del sub_directory_dict['Category'][num]
			del sub_directory_dict['Link'][num]


	df = pd.DataFrame(sub_directory_dict)
	print(tabulate(df, headers='keys', tablefmt='plain'))


def next_page(current_page, total_pages):
	if int(current_page) < (total_pages):
		next_page = driver.find_element(By.XPATH, "//div[@class='browse-search__pod-col-no-padding col__12-12 col__8-12--xs col__9-12--sm col__10-12--md col__10-12--lg']//li[" + str(int(current_page + 1)) + "]")





def material_list():
	options = Options()
	options.add_experimental_option("detach", True)
	#options.add_argument("--headless=new")
	driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
	target1 = re.compile(r'(href="(/b/(\w+)(-\w+?){0,7}((/\w+(-\w+){0,5}))?/N(-\w+))")')
	price_list = []
	rating_per = []
	rating_total = []
	brand_list = []
	model_num_list = []
	prod_name_list = []
	prod_attr_list = []
	prod_attr_val_list = []
	for link in sub_directory_dict['Link']:
		target2 = target1.search(link)
		url = main_url + target2.group(2)
		driver.get(url)
		try:
			'''What this program will be doing is webscraping dynamic webpages. This means that
			the webpage loads based on user input and navigation which means the full webpage cannot be scraped in one go.
			Therefore, I need to scrape the webpage in segments:
			Top Page Scrape
			Middle Page Scrape
			Bottom Page Scrape
			Then create a loop which would input all items from the scrape in one list while ommitting duplicates.
			Run this program for how ever many pages there are'''
			main_page = WebDriverWait(driver, 5).until(
				EC.presence_of_element_located((By.XPATH, "//a[normalize-space()='Home']"))
			)
			'''Top Page Scrape'''
			top_scrape = driver.page_source
			soup = BeautifulSoup(top_scrape, 'html.parser')
			cp = soup.find('a', class_="hd-pagination__link hd-pagination__current")
			#Current page number
			current_page = cp.string
			pages1 = soup.find_all('a', class_="hd-pagination__link")
			pages2 = pages1[-2]
			total_pages = pages2.string
			#print(soup.prettify())
			'''I need to make sure all categories align.'''

			categories = soup.find_all('div', class_="breadcrumb__item--cosdh")
			home = categories[0].get_text()
			dept = categories[1].get_text()
			#Might be wise to just do about 6 or 7 if-else statements for possible subcategories. Fill variable if there's a list item, else variable equals NULL
			#for c in range(len(categories - 2)):
					#for C in categories[2:len(categories + 1)]:
						#name = subcat
						#name = subcat + str(c)
			#Product price patterns are found using BeautifulSoup then combined using string concatenation
			#Becasue all of the values found are still BS4 object types, they need to be casted into str() to be further used
			prices = soup.find_all('div', class_="price-format__main-price")
			price_pattern = re.compile(r'<span>(\d+)</span><span\sclass="price__format">(\d+)</span>')
			for p in prices:
				found = price_pattern.search(str(p))
				full_price = '$' + found.group(1) + '.' + found.group(2)
				price_list.append(full_price)
			# Ratings patterns are found using BeautifulSoup then combined using string concatenation
			# Becasue all of the values found are still BS4 object types, they need to be casted into str() to be further used
			ratings = soup.find_all('div', class_="ratings--6r7g3")
			ratings_pattern = re.compile(r'style="width:\s?(\d+.\d+%)(;)?"></span></div><span class="ratings__count--6r7g3">')
			for r in ratings:
				found = ratings_pattern.search(str(r))
				ratings_full = found.group(1)
				ratings_tot = str(r.get_text())
				'''use .lstrip() and .rstrip() on ratings_tot before putting it in list'''
				ratings_tot.lstrip("(")
				ratings_tot.rstrip(")")
				rating_per.append(ratings_full)
				rating_total.append(ratings_tot)
			# Model number patterns are found using BeautifulSoup then combined using string concatenation
			# Becasue all of the values found are still BS4 object types, they need to be casted into str() to be further used
			mn_pattern = re.compile(r'<div class="product-identifier--bd1f5">(Model#\s\w+)</div>')
			model_nums = soup.find_all('div', class_="product-identifier--bd1f5")
			#Store the model numbers in a temporary list to use for later reference to seperate duplicate products
			temp_model_nums = []
			for i in model_nums:
				found = mn_pattern.search(str(i))
				mn = found.group(1)
				temp_model_nums.append(mn)
				model_num_list.append(mn)
			brands = soup.find_all('p', class_="product-header__title__brand--bold--4y7oa")
			for b in brands:
				b_full = str(b.get_text())
				'''use .strip() on ratings_tot before putting it in list''' #This still isnt working, revisit this
				b_full.strip()
				brand_list.append(b_full)
			product_names = soup.find_all('span', class_="product-header__title-product--4y7oa")
			for p in product_names:
				p_full = str(p.get_text())
				prod_name_list.append(p_full)
			print(prod_name_list)
			#badges = soup.find_all('span', class_="badge-container--dimm8 u__bold product-badge--dvs1t product-badge--large--dvs1t")  # If its there, get it. If not, NULL
			'''These product attributes arent guaranteed to be here. Create an if clause for this section'''
			product_attrs = soup.find_all('div', class_="kpf__name kpf__name--simple kpf__name--one-column")
			for names in product_attrs:
				names_full = str(names.get_text())
				prod_attr_list.append(names_full)
			print(prod_attr_list)
			product_attrs_vals = soup.find_all('div', class_="kpf__value kpf__value--simple kpf__value--one-column")
			for vals in product_attrs_vals:
				vals_full = str(vals.get_text())
				prod_attr_val_list.append(vals_full)
			print(prod_attr_val_list)
			#print(f'The length of lists are: \n'
				  #f'Prices: {len(prices)}\n'
				  #f'Ratings: {len(ratings)}\n'
				  #f'Model Numberss: {len(model_nums)}\n'
				  #f'Brands: {len(brands)}\n'
				  #f'Product Names: {len(product_names)}\n')

			'''Middle Scrape
			Find the middle of the page using the XPATH of the last item found in the Top Page Scrape'''
			#Uses the last model number in the list as a reference point of where to scroll to
			middle_page = driver.find_element(By.XPATH,"//div[contains(text()," + "'" + temp_model_nums[-1] + "')]")
			driver.execute_script("arguments[0].scrollIntoView();", middle_page)
			middle_scrape = driver.page_source
			soup1 = BeautifulSoup(middle_scrape, 'html.parser')
			'''Use the same scraping code from top scrape but ommit the duplicates'''
			model_nums1 = soup1.find_all('div', class_="product-identifier--bd1f5")
			prices1 = soup1.find_all('div', class_="price-format__main-price")
			ratings1 = soup1.find_all('div', class_="ratings--6r7g3")
			brands1 = soup1.find_all('p', class_="product-header__title__brand--bold--4y7oa")
			product_names1 = soup1.find_all('span', class_="product-header__title-product--4y7oa")
			product_attrs1 = soup1.find_all('div', class_="kpf__name kpf__name--simple kpf__name--one-column")
			product_attrs_vals1 = soup1.find_all('div', class_="kpf__value kpf__value--simple kpf__value--one-column")
			#Sorts through the Soup1 BeautifulSoup object to find all products and their respective attributes and sort them into the main list based on thier indexed location
			for i in range(len(model_nums1)):
				found = mn_pattern.search(str(model_nums1[i]))
				mn1 = found.group(1)
				if mn1 in temp_model_nums:
					continue
				else:
					found1 = price_pattern.search(str(prices1[i]))
					full_price1 = '$' + found1.group(1) + '.' + found1.group(2)
					found2 = ratings_pattern.search(str(ratings1[i]))
					ratings_full1 = found2.group(1)
					ratings_tot1 = str(ratings1[i].get_text())
					'''use .lstrip() and .rstrip() on ratings_tot before putting it in list'''
					ratings_tot1.lstrip("(")
					ratings_tot1.rstrip(")")
					b_full1 = str(brands1[i].get_text())
					'''use .strip() on ratings_tot before putting it in list'''
					b_full1.strip()
					p_full1 = str(product_names1[i].get_text())
					names_full = str(product_attrs1[i].get_text())
					vals_full1 = str(product_attrs_vals1[i].get_text())
					temp_model_nums.append(mn1)
					model_num_list.append(mn1)
					price_list.append(full_price1)
					rating_per.append(ratings_full1)
					rating_total.append(ratings_tot1)
					brand_list.append(b_full1)
					#prod_name_list.append(p_full1)
			print(price_list)
			print(rating_per)
			print(rating_total)
			print(model_num_list)
			print(brand_list)


			'''Bottom Scrape'''
			#bottom_page = driver.find_element(By.XPATH, "//div[@class='browse-search__pod-col-no-padding col__12-12 col__8-12--xs col__9-12--sm col__10-12--md col__10-12--lg']//li[" + str(int(current_page)) + "]")
			#driver.execute_script("arguments[0].scrollIntoView();", bottom_page)
			#bottom_scrape = driver.page_source
			#soup2 = BeautifulSoup(bottom_scrape, 'lxml')


			'''Create a method to click the next page. Find the current page number using the tag: <a class="hd-pagination__link hd-pagination__current"
			 if current page is less than total pages, run "Next Page" Method'''

		except:
			raise

		#finally:
			#categories = soup.find_all('div', class_="breadcrumb__nowrap--cosdh")
			#print(categories)
			#Drill down on the tags to get the strings from the content
			sub_item_list1 = soup.find_all('div', class_="browse-search__pod col__6-12 col__6-12--xs col__4-12--sm col__4-12--md col__3-12--lg")
			#print(sub_item_list1)




general_hd_scrape()
hd_scrape_clean()
material_list()





