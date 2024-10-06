# import logging
# import azure.functions as func
# from fetch_complaints import fetch_and_append

# def main(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Python HTTP trigger function processed a request.')

#     # Call the function to fetch and append data
#     csv_file = 'consumer_complaints.csv'
#     base_url = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?date_received_max=2024-10-04&date_received_min=2011-12-01&field=all&size=500&sort=created_date_desc"
#     fetch_and_append(base_url, csv_file)
    
#     return func.HttpResponse("Python script executed successfully.")


import azure.functions as func
import logging
# from fetch_complaints import fetch_and_append
import requests
import csv
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="us_customer_complaints_scrapper")


def us_customer_complaints_scrapper(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Call the function to fetch and append data
    csv_file = 'consumer_complaints.csv'
    base_url = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?date_received_max=2024-10-04&date_received_min=2011-12-01&field=all&size=500&sort=created_date_desc"
    fetch_and_append(base_url, csv_file)
    
    return func.HttpResponse("Extracting of US Customer Complaints script executed successfully.")


# Define the base API endpoint (without search_after)
base_url = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?date_received_max=2024-10-04&date_received_min=2011-12-01&field=all&size=1000&sort=created_date_desc"

# Global variable to keep track of the total number of records imported
total_records_imported = 0

# Function to make an API request with optional search_after parameter
def fetch_consumer_complaints(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Check for request errors
        data = response.json()
        return data['hits']['hits'], data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return [], None

# Function to append data to a CSV file
def append_to_csv(data, csv_file):
    global total_records_imported
    # Define the CSV headers
    headers = ['date_received', 'complaint_id', 'product', 'sub_product', 'issue', 'sub_issue', 
               'company', 'state', 'zip_code', 'consumer_complaint_narrative', 'company_response']

    # Check if file exists
    file_exists = os.path.isfile(csv_file)

    with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)

        # Write the header only if the file does not exist
        if not file_exists:
            writer.writeheader()

        for complaint in data:
            # Prepare a dictionary for the current row
            row = {
                'date_received': complaint['_source'].get('date_received'),
                'complaint_id': complaint['_source'].get('complaint_id'),
                'product': complaint['_source'].get('product'),
                'sub_product': complaint['_source'].get('sub_product'),
                'issue': complaint['_source'].get('issue'),
                'sub_issue': complaint['_source'].get('sub_issue'),
                'company': complaint['_source'].get('company'),
                'state': complaint['_source'].get('state'),
                'zip_code': complaint['_source'].get('zip_code'),
                'consumer_complaint_narrative': complaint['_source'].get('complaint_what_happened'),
                'company_response': complaint['_source'].get('company_response')
            }
            writer.writerow(row)

        # Update the total number of records imported
        total_records_imported += len(data)
        print(f"Appended {len(data)} rows to {csv_file}. Total records imported: {total_records_imported}")

# Function to build the next URL with search_after
def build_next_url(last_complaint, base_url):
    sort_values = last_complaint['sort']
    search_after = f"{sort_values[0]}_{sort_values[1]}"
    next_url = f"{base_url}&search_after={search_after}"
    return next_url

# Recursive function to fetch data and append it to CSV
def fetch_and_append(api_url, csv_file):
    global total_records_imported

    # Fetch the data from API
    complaints_data, full_response = fetch_consumer_complaints(api_url)

    # If there are no more results or 1000 records have been imported, stop
    if not complaints_data or total_records_imported >= 1000:
        print(f"Stopping: Imported {total_records_imported} records.")
        return

    # Append the current batch to the CSV
    append_to_csv(complaints_data, csv_file)

    # Get the last complaint to build the search_after parameter for the next API call
    last_complaint = complaints_data[-1]

    # Build the next URL
    next_url = build_next_url(last_complaint, base_url)

    # Recursively call the function with the next URL
    fetch_and_append(next_url, csv_file)
