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
    interest_by_country_df = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True, inc_geo_code=True).reset_index()

    # Rename columns
    interest_by_country_df = interest_by_country_df.rename(columns={'geoName': 'Country', 'geoCode': 'CountryCode', 
                                                                  'vpn': 'VPN', 'hack': 'Hack', 'cyber': 'Cyber', 
                                                                  'security': 'Security', 'wifi': 'WiFi'})

    # Set the data extraction date for each country
    end_date = datetime.now().date().strftime('%Y-%m-%d')
    interest_by_country_df['DateExtracted'] = end_date

    # Calculate the start date by subtracting seven days from the end date
    start_date = datetime.now().date() - timedelta(days=7)
    start_date = start_date.strftime('%Y-%m-%d')
    interest_by_country_df['StartDate'] = start_date

    # Set the end date for each country
    interest_by_country_df['EndDate'] = end_date
  
    # Export the DataFrame to a CSV file
    csv_file_path = 'interest_by_country.csv'
    interest_by_country_df.to_csv(csv_file_path, index=False)
    
    return interest_by_country_df

@task
def rank_columns(data):
    logging.info("Ranking columns...")
    processed_data = data.copy()

    # Define the columns to be ranked
    columns_to_rank = ['VPN', 'Hack', 'Cyber', 'Security', 'WiFi']
    
    # Loop through each row (country) in the DataFrame
    for index, row in processed_data.iterrows():

        # Extract values for the columns to be ranked for the current country
        values = {column: row[column] for column in columns_to_rank if column in processed_data.columns}
        
        # Sort values in descending order and keywords alphabetically
        sorted_values = sorted(values.items(), key=lambda x: (-int(x[1]) if str(x[1]).isdigit() else -float('inf'), x[0]))
        
        # Assign ranks to each column
        ranks = {column: rank+1 for rank, (column, value) in enumerate(sorted_values)}
        
        # Check if 'VPN' is equal to at least one other column
        equal_values = [column for column in columns_to_rank if column in values and values['VPN'] == values[column]]
        
        if len(equal_values) >= 2:
            # If 'VPN' is equal to at least one other column, put 'VPN' last among them
            VPN_rank = max(ranks[column] for column in equal_values)
            for column in equal_values:
                if column != 'VPN':
                    ranks[column] = min(ranks[column], VPN_rank - 1)
                ranks['VPN'] = VPN_rank
        
        # Update the DataFrame with the ranks
        for column in columns_to_rank:
            if column in processed_data.columns and column in ranks:
                processed_data.at[index, column] = ranks[column]
    
    return processed_data

@task
def transpose_data(data):
    logging.info("Transposing columns...")
    processed_data = data.copy()

    # Define the columns to be transposed
    columns_to_transpose = ['VPN', 'Hack', 'Cyber', 'Security', 'WiFi']
    
    # Melt the DataFrame to transpose the columns
    melted_df = processed_data.melt(id_vars=[col for col in processed_data.columns if col not in columns_to_transpose], 
                                    value_vars=columns_to_transpose, var_name='Keyword', value_name='Ranking')

    # Sort the melted DataFrame by 'Country' and 'Ranking'
    melted_df = melted_df.sort_values(by=['Country', 'Ranking'], ascending=[True, False])

    melted_df = melted_df[['Country', 'CountryCode', 'Keyword', 'Ranking', 'StartDate', 'EndDate', 'DateExtracted']]
    # Reset the index of the melted DataFrame
    melted_df = melted_df.reset_index(drop=True)
    
    return melted_df

@task
def store_data_in_snowflake(data):
    logging.info("Storing data in Snowflake...")
    try:
        # Using the imported connection parameters
        conn = snow.connect(**SNOWFLAKE_CONFIG)  

        # Reset the DataFrame's index (excluding the index column)        
        data = data.reset_index(drop=True)

        # Write data from dataframe        
        write_pandas(conn, data, "IB_google_trends_data", auto_create_table=True)

        # Logs will be outputted in terminal
        logging.info("Data stored in Snowflake successfully.")
        return 'Data stored in Snowflake table IB_google_trends_data'
    except Exception as e:
        logging.error(f"An error occurred while storing data in Snowflake: {e}")

        # Close connection
        conn.close()

@flow
def homework_data_flow():

    # Fetch Google Trends data
    interest_by_country_data = fetch_google_trends_data(['vpn', 'hack', 'cyber', 'security', 'wifi'])

    # Rank columns. Simple ranking, each keyword as sepparate column and VPN last among those that are equal value (the rest ranked alphabetically among them)
    ranked_data = rank_columns(interest_by_country_data)

    #Transpose columns.
    transposed_data = transpose_data(ranked_data)

    # Store prod data in BigQuery
    store_data_in_snowflake(transposed_data)
        
# Run the flow
if __name__ == "__main__":
    deploy = homework_data_flow.to_deployment(name="homework_deployment", interval=120)
    serve(deploy)