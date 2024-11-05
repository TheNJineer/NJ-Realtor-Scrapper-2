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
- US Cenus for the 2019-2022 Per Capita Income
- US Cenus for the 2019-2022 Median Household Income
- FBI Uniform Crime Reporting Program (UCRP)
- NJ Deptartment of Labor for 2019-2023 Population Estimates
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
