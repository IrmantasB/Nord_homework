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
    pytrends = TrendReq(hl='en-US', tz=0)
    pytrends.build_payload(keywords, cat=0, timeframe=timeframe, geo=geo)
    interest_by_country_df = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True, inc_geo_code=True)

    # Rename columns
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

    # Define the columns to be ranked
    columns_to_rank = ['vpn', 'hack', 'cyber', 'security', 'wifi']
    
    # Loop through each row (country) in the DataFrame
    for index, row in processed_data.iterrows():

        # Extract values for the columns to be ranked for the current country
        values = {column: row[column] for column in columns_to_rank if column in processed_data.columns}
        
        # Sort values in descending order and keywords alphabetically
        sorted_values = sorted(values.items(), key=lambda x: (-int(x[1]) if str(x[1]).isdigit() else -float('inf'), x[0]))
        
        # Assign ranks to each column
        ranks = {column: rank+1 for rank, (column, value) in enumerate(sorted_values)}
        
        # Check if 'vpn' is equal to at least one other column
        equal_values = [column for column in columns_to_rank if column in values and values['vpn'] == values[column]]
        
        if len(equal_values) >= 2:
            # If 'vpn' is equal to at least one other column, put 'vpn' last among them
            vpn_rank = max(ranks[column] for column in equal_values)
            for column in equal_values:
                if column != 'vpn':
                    ranks[column] = min(ranks[column], vpn_rank - 1)
                ranks['vpn'] = vpn_rank
        
        # Update the DataFrame with the ranks
        for column in columns_to_rank:
            if column in processed_data.columns and column in ranks:
                processed_data.at[index, column] = ranks[column]
    
    return processed_data

@task
def transpose_data(data):
    processed_data = data.copy()
    
    # Get list of all available columns
    original_columns = processed_data.columns.tolist()
    
    # Initialize a list to store the transposed data
    transposed_data = []

    # Loop through each row (country) in the DataFrame
    for _, row in processed_data.iterrows():

        # Extract additional columns
        country = row['Country']
        country_code = row['CountryCode']
        start_date = row['StartDate']
        end_date = row['EndDate']
        date_extracted = row['DateExtracted']

        # Append the transposed data for the current country
        for keyword in original_columns:
            if keyword in processed_data.columns:
                transposed_data.append([country, country_code, keyword, row[keyword], start_date, end_date, date_extracted])

    # Create a DataFrame from the transposed data
    transposed_df = pd.DataFrame(transposed_data, columns=['Country', 'CountryCode', 'Keywords', 'Rank', 'StartDate', 'EndDate', 'DateExtracted'])

    return transposed_df

@task
def store_data_in_snowflake(data):
    print("Storing data in Snowflake...")
    try:
        # Using the imported connection parameters
        conn = snow.connect(**SNOWFLAKE_CONFIG)  
        # Dataframe
        df = pd.DataFrame(data)
        # Write data from dataframe
        write_pandas(conn, df, "IB_google_trends_data", auto_create_table=True)
        # Close connection
        conn.close()
        # Loggs will be outputed in terminal
        logging.info("Data stored in Snowflake successfully.")
        return 'Data stored in Snowflake table IB_staging_trends_data'
    except Exception as e:
        logging.error(f"An error occurred while storing data in Snowflake: {e}")
        return None
    

@flow
def homework_data_flow():

    # Fetch Google Trends data
    interest_by_country_data = fetch_google_trends_data(['vpn', 'hack', 'cyber', 'security', 'wifi'])

    # Rank columns. Simple ranking, each keyword as sepparate column
    ranked_data = rank_columns(interest_by_country_data)

    # Keywords and their values transposed for better analytical capabilities
    transposed_data = transpose_data(ranked_data)

    # Store prod data in BigQuery
    store_data_in_snowflake(transposed_data)
    
# Run the flow
if __name__ == "__main__":
    deploy = homework_data_flow.to_deployment(name="homework_deployment", interval=120)
    serve(deploy)
