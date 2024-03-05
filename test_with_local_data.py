import logging
import pandas as pd
from datetime import datetime, timedelta
from pytrends.request import TrendReq
from prefect import task, flow, serve
#from prefect_snowflake.database import SnowflakeConnector
#from sqlalchemy import create_engine
from snowflake_config import SNOWFLAKE_CONFIG  # Importing the Snowflake connection parameters
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
 

@task
def fetch_google_trends_data(keywords, timeframe='now 7-d', geo=''):
    # Create a mock DataFrame with the same columns as in the original function
    data = {
        'geoName': ['United Kingdom', 'Switzerland', 'Germany'],
        'geoCode' : ['UK', 'SW', 'DE'],
        'vpn': [60, 60, 60],
        'hack': [40, 60, 70],
        'cyber': [30, 60, 10],
        'security': [20, 90, 40],
        'wifi': [10, 100, 50]
    }

    interest_by_country_df = pd.DataFrame(data)

    interest_by_country_df = interest_by_country_df.rename(columns={'geoName': 'Country'})
    interest_by_country_df = interest_by_country_df.rename(columns={'geoCode': 'CountryCode'})
    interest_by_country_df = interest_by_country_df.rename(columns={'vpn': 'VPN'})
    interest_by_country_df = interest_by_country_df.rename(columns={'hack': 'Hack'})
    interest_by_country_df = interest_by_country_df.rename(columns={'cyber': 'Cyber'})
    interest_by_country_df = interest_by_country_df.rename(columns={'security': 'Security'})
    interest_by_country_df = interest_by_country_df.rename(columns={'wifi': 'WiFi'})

    
    
    # Set the data extraction date for each country
    end_date = datetime.now().date().strftime('%Y-%m-%d')
    interest_by_country_df['DateExtracted'] = end_date

    # Calculate the start date by subtracting seven days from the end date
    start_date = datetime.now().date() - timedelta(days=7)
    start_date = start_date.strftime('%Y-%m-%d')
    interest_by_country_df['StartDate'] = start_date

    # Set the end date for each country
    interest_by_country_df['EndDate'] = end_date
  
    return interest_by_country_df

@task
def rank_columns(data):
    processed_data = data.copy()
    
    # Loop through each row (country) in the DataFrame
    for index, row in processed_data.iterrows():
        # Extract values for 'vpn', 'hack', 'cyber', 'security', 'wifi' for the current country
        keywords = ['vpn', 'hack', 'cyber', 'security', 'wifi']
        values = {keyword: row[keyword] for keyword in keywords if keyword in processed_data.columns}
        
        # Sort values in descending order and keywords alphabetically
        sorted_values = sorted(values.items(), key=lambda x: (-x[1], x[0]))
        
        # Assign ranks to each keyword
        ranks = {keyword: rank+1 for rank, (keyword, value) in enumerate(sorted_values)}
        
        # Check if 'vpn' is equal to at least one other column
        equal_values = [keyword for keyword in keywords if keyword in values and values['vpn'] == values[keyword]]
        
        if len(equal_values) >= 2:
            # If 'vpn' is equal to at least one other column, put 'vpn' last among them
            vpn_rank = max(ranks[keyword] for keyword in equal_values)
            for keyword in equal_values:
                if keyword != 'vpn':
                    ranks[keyword] = min(ranks[keyword], vpn_rank - 1)
                else:
                    ranks[keyword] = min(ranks[keyword], min(ranks[k] for k in equal_values if k != 'vpn'))
        
        # Update the DataFrame with the ranks using .loc
        for keyword in keywords:
            if keyword in processed_data.columns and keyword in ranks:
                processed_data.loc[index, keyword] = ranks[keyword]
    return processed_data

@task
def store_data_in_snowflake(data):
    print("Storing data in Snowflake...")
    try:
        # Using the imported connection parameters
        conn = snow.connect(**SNOWFLAKE_CONFIG)  

        # Convert 'Rank' column to integer
        #data['Rank'] = data['Rank'].astype(int)      
        
        # Dataframe
        df = data.copy()
        # Write data from dataframe
        write_pandas(conn, df, "IB_google_trends_STAGING_data", auto_create_table=True)
        
        
        # Loggs will be outputed in terminal
        logging.info("Data stored in Snowflake successfully.")
        return 'Data stored in Snowflake table IB_staging_trends_data'
    except Exception as e:
        logging.error(f"An error occurred while storing data in Snowflake: {e}")
        # Close connection
        conn.close()
@flow
def homework_data_flow():

    # Fetch Google Trends data
    interest_by_country_data = fetch_google_trends_data(['vpn', 'hack', 'cyber', 'security', 'wifi'])

    # Rank columns. Simple ranking, each keyword as sepparate column
    ranked_data = rank_columns(interest_by_country_data)

    # Store prod data in BigQuery
    store_data_in_snowflake(ranked_data)

    # Keywords and their values transposed for better analytical capabilities
    #transposed_data = transpose_data(ranked_data)
    
# Run the flow
if __name__ == "__main__":
    deploy = homework_data_flow.to_deployment(name="homework_deployment", interval=120)
    serve(deploy)
