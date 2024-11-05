<div id="header" align="center">
  <img src="https://www.njrealtor.com/site-content/images/logo-large.png" width="700"/>
</div>

<b>Introduction:</b>

Imagine being new real estate agent; wide-eyed, excited and ready to take on your first client. Miraculously, you get a call on your first day from a buyer with a particular set of criteria: “I’m moving from another state so I know nothing about New Jersey, but they have a max budget of $500k, want to live in the suburbs but close to public transportation, a good school system and plenty of businesses and entertainment. Please show me all cities which meet that criteria, thank!”. You hang the phone up less enthusiastic thank when you picked up. You have no idea where to begin getting this information but you heard from another senior agent that’s there’s four (4) different private multiple listing services (MLS), Zillow, Redfin, Trulia and maybe Wikipedia. Other than that you have to either drive to other cities to do recon or call other agents to get a detailed layout of their city. Only if there was an easier way to get all of this information. You also wish there was an easier way to classify cities with this criteria, because more than likely other people would want to live somewhere just like that. Lastly, how would a municipality having all those great qualities affect the change in population?

<b>Problem Statement:</b>

The current system (or lack thereof) for real estate agents to analyze their local and distance markets is inefficient and time consuming. To properly represent clients and meet their needs, an agent needs to spend many hours throughout the week tracking recent sales, inventory, home prices and conducting comparable sales analysis. With this information, an agent can thoroughly understand market dynamics, when, where and how a buyer should purchase a property or to sell a home. However, markets change on a daily, weekly and monthly basis. This can be overwhelming for new and even established agents as NJ has 4-5 different private MLS services which organize and display their data with different methods. This inefficiency can lead to increased frustration and lack of understanding with ones job, increased inability to keep up with fast paced markets, decreased client satisfaction and loss of revenue (salary).

<b>Goals:</b>
- Build and maintain an ETL (extract, transform, load) pipeline of real estate data from various sources through the use of Python, PostgrSQL and Apache Airflow
- Perform exploratory data analysis (EDA) to gain insight and recognize trends in acquired data
- Visualization the data using tools such as Matplotlib, Seaborn and GeoPandas for static images, and Streamlit to create real estate dashboards
- Use regression algorithms to understand the relationship between the real estate data and municipality populations
- Use unsupervised machine learning algorithms to cluster “similar municipalities” based on data compiled from the MLS, US Census Bureau, NJ Department of Labor and third party data resources
- Use results from real estate data analysis and machine learning to help retail sellers, buyers and investors make data driven decisions
- Allow agents to effectively access and analyze all markets across the state of NJ, not only ones within their immediate radius

<b>Data Sources:</b>
- NJ Realtor Association
- Wikipedia
- USPS (Zipcode Lookup) / UnitedStatesZipCodes.org
- US Cenus for the 2019-2022 Per Capita Income
- US Cenus for the 2019-2022 Median Household Income
- US Cenus for the 2019-2022 Median Age
- US Cenus for the 2019-2022 Home Ownership Rates
- US Cenus for the 2019-2022 Commuting Data
- County Business Patterns (CBP) by Zipcode
- FBI Uniform Crime Reporting Program (UCRP)
- NJ Deptartment of Labor for 2019-2023 Population Estimates
- NJ Deptartment of Labor for 2019-2023 General Tax Rates
- NJ Deptartment of Labor for 2019-2023 Average Property Tax Bills
- NJ Deptartment of Labor for 2019-2023 Unemployment Rates
- U.S Environmental Protection Agency Air Quality Index (AQI)
- School Digger Best High School Rankings (for NJ)

<b>Designing the System:</b>
The pipeline operates as follows:
1. Requests will be used to send inidividual requests to receive files from the host server for each municipality for that month and year
2. After all target pdfs are streamed, begin scrapping the data using PyPDF2 and Regex
3. Store data in a temporary Python dictionary
4. Convert Python dictionary into Pandas DataFrame
5. Initiate data cleaning and transformations
6. Store DataFrame in PostgreSQL
7. Schedule monthly data aggregation with Apache Airflow

<b>Data Cleaning, Enrichment, Imputations and Transformations</b>
- Used BeautifulSoup and Requests to scrape all municipality's latitudinal and longitudunal information to merge with real estate data
- Used BeautifulSoup and Requests to scrape of NJs Census Designated Places (CDPs) and convert names to host cities
- Transformed all of US Census Data from JSON to Pandas Dataframes
- School names cleaned to match their host municipality. Municipality's School Rank was averaged if multiple school systems existed
- Transformed FBI Crime Data from unconventional xlsx and pdf formats to Pandas Dataframes
- Used the Census Bureau API to request CBP zipcode data. Transformed and filtered data to save NJ Zipcodes early. Use the full zipcode database to match NJ zipcodes with their municipality name
- Air Quality Index Data was transformed and filtered NJ Counties only. Data was then converted from a daily to a yearly timeframe
- After merging all dataframes, necessary transformations (added constants, log, exp and power) were applied to ensure data was as close to normally distributed as possible

<b>Exploratory Data Analysis & Data Visualization</b>
<div id="header" align="center">
  <br>What is the Median Sales Price of a NJ Home Since 2019?</br>
  <img src="https://github.com/TheNJineer/NJRealtor-Scrapper/blob/updated_main/Project%20Images/Median%20Sales%20Prices%20by%20Year.jpeg" width="500"/>
</div>
<div id="header" align="center">
  <br>What is the Average Median Gross Rent by County for 2022?</br>
  <img src="https://github.com/TheNJineer/NJRealtor-Scrapper/blob/updated_main/Project%20Images/Avg%20Estimated%20Median%20Gross%20Rent%20-%20Dollar%20by%20County.jpeg" width="500"/>
</div>
<div id="header" align="center">
  <br>What is the best month to sell your home?</br>
  <img src="https://github.com/TheNJineer/NJRealtor-Scrapper/blob/updated_main/Project%20Images/Closed%20Sales%20by%20Month%20(All%20Time).jpeg" width="1500"/>
</div>

<b>Machine Learning</b>

<div id="header" align="center">
  <br>Supervised Regression R2 Prediction and Best Fit Scores</br>
  <img src="https://github.com/TheNJineer/NJRealtor-Scrapper/blob/updated_main/Project%20Images/Estimator%20Scores.jpeg.jpeg" width="3000"/>
</div>
<div id="header" align="center">
  <br>UMAP Dimension Reduction Results (Pre-Clustering)</br>
  <img src="https://github.com/TheNJineer/NJRealtor-Scrapper/blob/updated_main/Project%20Images/Rotating%20Clusters%20UMAP_PreLabel.gif" width="3000"/>
</div>
<div id="header" align="center">
  <br>UMAP Dimension Reduction Clustering Results</br>
  <img src="https://github.com/TheNJineer/NJRealtor-Scrapper/blob/updated_main/Project%20Images/Rotating%20Clusters%20UMAP.gif" width="3000"/>
</div>






