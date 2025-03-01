import os
import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Function to download data from the URL
def download_data(url, output_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, 'wb') as f:
            f.write(response.content)
    else:
        raise Exception(f"Failed to download file from {url}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'cycling_data_download_to_gcs',
    default_args=default_args,
    description='Download CSV files and upload to GCS, categorized by year',
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2025, 2, 17),
    catchup=False,
) as dag:

    start_task = EmptyOperator(task_id='start')

    # Define file URLs (base year is 2023)
    base_urls = [
        "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W1%20spring-Central.csv",
        "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W1%20spring-Cycleways.csv",
        "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W1%20spring-Inner-Part1.csv",
        "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W1%20spring-Inner-Part2.csv",
        "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W1%20spring-Outer.csv",
        "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W2%20autumn-Cycleways.csv"
    ]

    # Process files year by year (2023, then 2024)
    prev_end_task = start_task  # Track the last task for dependencies

    for year in [2023, 2024]:
        # Generate URLs for the year (replacing 2023 with 2024 in second iteration)
        file_urls = [url.replace("2023", str(year)) for url in base_urls]

        download_upload_tasks = []

        def format_filename(url):
            return os.path.basename(url).replace("%20", "_")

        for url in file_urls:
            file_name = format_filename(url)

            # Download the file
            download_task = PythonOperator(
                task_id=f"download_{year}_{file_name}",
                python_callable=download_data,
                op_args=[url, f"/tmp/{file_name}"]
            )

            # Upload the file to GCS with the correct year in the path
            upload_task = BashOperator(
                task_id=f"upload_{year}_{file_name}",
                bash_command=f"gsutil cp /tmp/{file_name} gs://yelp-de-project-451206-bucket/cyclingdata_{year}/{file_name}"
            )

            download_task >> upload_task
            download_upload_tasks.append(download_task)
            download_upload_tasks.append(upload_task)

        # Create an end task for the year to ensure all files finish before moving to the next year
        year_end_task = EmptyOperator(task_id=f"end_{year}")

        prev_end_task >> download_upload_tasks >> year_end_task
        prev_end_task = year_end_task  # Update previous end task

    final_task = EmptyOperator(task_id='final_end')
    prev_end_task >> final_task  # Ensure everything finishes before ending the DAG
