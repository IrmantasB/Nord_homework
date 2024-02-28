# main.py

import requests
from google.cloud import bigquery

def fetch_data(request):
    # Fetch data from Google Trends API
    url = "https://trends.google.com/trends/api/explore?date=now%201-d&q=vpn,hack,cyber,security,wifi"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        transformed_data = transform_data(data)
        load_data(transformed_data)
        return "Data fetched and loaded into BigQuery successfully."
    else:
        return "Failed to fetch data from Google Trends API."

def transform_data(data):
    transformed_data = {}  # Structure to store transformed data
    for country, terms_data in data.items():
        country_data = {}
        # Sort terms by their search term shares in descending order
        sorted_terms = sorted(terms_data.items(), key=lambda x: x[1], reverse=True)
        # Assign rankings
        rank = 1
        prev_share = None
        prev_rank = None
        for term, share in sorted_terms:
            if term == 'vpn':
                vpn_rank = rank  # Store vpn's rank
                vpn_share = share  # Store vpn's share
            if prev_share is not None and prev_share != share:
                rank += 1
            elif prev_share is not None and prev_share == share:
                rank = prev_rank  # Set rank to previous rank if shares are equal
            country_data[term] = rank
            prev_share = share
            prev_rank = rank
        # Set vpn's rank to the lowest rank among terms with the same share
        for term, share in sorted_terms:
            if share == vpn_share and term != 'vpn':
                country_data['vpn'] = min(vpn_rank, country_data.get('vpn', float('inf')))
        transformed_data[country] = country_data
    return transformed_data

def load_data(transformed_data):
    # Store the transformed data into BigQuery tables
    client = bigquery.Client()
    dataset_id = 'data_engineer'
    table_id = 'your_table_name'
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = 'WRITE_TRUNCATE'  # Replace existing data
    job = client.load_table_from_json(transformed_data, table_ref, job_config=job_config)
    job.result()  # Waits for the job to complete
