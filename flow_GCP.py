# start virtual environment
# pip install prefect (see prefect.io for documentation on how to setup cloud intance)
# pip install or pip install -U required libraries
# create account on prefect.io
# login to prefect cloud from console withn
#  'prfect cloud login' command, authenticate via API key or via webpage
# store your GCP service account credentials JSON to https://prefecthq.github.io/prefect-gcp/blocks_catalog/ and call it from prefect_gcp.credentials import GcpCredentials
# run the code
# fingers crossed :)
#An error occurred while storing data in BigQuery: 403 POST https://bigquery.googleapis.com/bigquery/v2/projects/homework-data2020/datasets/data_engineer/tables?prettyPrint=false: Access Denied: Dataset homework-data2020:data_engineer: Permission bigquery.tables.create denied on dataset homework-data2020:data_engineer (or it may not exist).
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from pytrends.request import TrendReq
from prefect import task, flow, serve
from prefect_gcp.credentials import GcpCredentials
from google.cloud.exceptions import NotFound
#from prefect.tasks import task_input_hash



@task
def fetch_google_trends_data(keywords, timeframe='now 7-d', geo=''):
    pytrends = TrendReq(hl='en-US', tz=0)
    pytrends.build_payload(keywords, cat=0, timeframe=timeframe, geo=geo)
    interest_by_country_df = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True, inc_geo_code=True)
    # Renaming columns
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
                ranks['vpn'] = vpn_rank
        
        # Update the DataFrame with the ranks
        for keyword in keywords:
            if keyword in processed_data.columns and keyword in ranks:
                processed_data.at[index, keyword] = ranks[keyword]
    
    return processed_data


@task
def store_data_in_bigquery():
    print("Storing data in BigQuery...")
    try:        

        gcp_credentials = GcpCredentials.load("nord-homework")
        google_auth_credentials = gcp_credentials.get_credentials_from_service_account()
        client = bigquery.Client(credentials=google_auth_credentials)

        #my_block = GcpCredentials.load("nord-homework")
        # Create a BigQuery client using the default credentials
        #project_id = my_block.project_id()
        #client = bigquery.Client(project=project_id)
        
        dataset_id = 'data_engineer'
        table_id = 'homework-data2020.data_engineer.IB_staging_data_google_trends'  # Update with analyst's name
        #project_ref = client.project(project_id)
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        
        # Create table if not exists
        try:
            client.get_table('homework-data2020.data_engineer.IB_staging_data_google_trends')
        except NotFound:
            table = bigquery.Table('homework-data2020.data_engineer.IB_staging_data_google_trends')

            client.create_table(table)
            print("Table {} created.".format(table_id))

        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        # You can append NEW data to existing data
        #job_config.write_disposition = 'WRITE_APPEND'  # Append data to the table

        # Or you can truncate existing table every time new data loads
        job_config.write_disposition = 'WRITE_TRUNCATE'  # Truncate the table before loading data
        
       #df = pd.DataFrame(data)
        #client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
        print("Data stored in BigQuery successfully.")
        return f'Data stored in BigQuery table {dataset_id}.{table_id}'
    except Exception as e:
        print(f"An error occurred while storing data in BigQuery: {e}")
        return None

@flow
def homework_data_flow():

    # Fetch Google Trends data
    interest_by_country_data = fetch_google_trends_data(['vpn', 'hack', 'cyber', 'security', 'wifi'])      

    # Rank columns
    ranked_data = rank_columns(interest_by_country_data)            
    # Store data in BigQuery
    store_data_in_bigquery()

# Run the flow

if __name__ == "__main__":
    deploy = homework_data_flow.to_deployment(name="homework_deployment", interval = 60)
    serve(deploy)
