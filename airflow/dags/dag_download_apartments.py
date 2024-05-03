import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import plotly.express as px
from tabulate import tabulate  

# Default arguments for tasks
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 3),  
    'retries': 1,
    'retry_delay': timedelta(minutes=5)  
}

# Define function to generate versioned filename with timestamp
def get_versioned_filename(base_filename):
    """
    Generates a versioned filename with a timestamp appended to the base filename.

    Args:
        base_filename (str): The base filename to be versioned.

    Returns:
        str: Versioned filename with timestamp in the format "{base_filename}_{timestamp}.csv".
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    versioned_filename = f"{base_filename}_{timestamp}.csv"
    return versioned_filename

# Define DAG details
dag = DAG(
    dag_id='download_and_analyze_apartments',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False  # Skip backfilling if needed
)

def download_and_filter_apartments():
    """
    Downloads apartment data, filters based on property type, and saves to a versioned CSV file.

    This task reads apartment data from a remote URL, filters for properties of type 'APARTMENT',
    and saves the filtered dataset as a versioned CSV file in the specified output folder.
    """
    url = "https://raw.githubusercontent.com/swetajainh/immo-eliza-MAS-FN-analysis/main/data/Cleaned/cleaned_apartment.csv"
    airflow_home = os.getenv('AIRFLOW_HOME')
    output_folder = os.path.join(airflow_home, 'dags', 'data')

    # Read data from CSV URL
    df_raw_apartments = pd.read_csv(url, index_col=0)

    # Filter for apartments
    df_apartment = df_raw_apartments[df_raw_apartments["property_type"] == "APARTMENT"]

    # Generate versioned filename
    output_filename = get_versioned_filename('apartments_raw_data')
    output_path = os.path.join(output_folder, output_filename)

    # Save filtered data with versioned filename
    df_apartment.to_csv(output_path, index=False)

    # Store versioned filename as a variable in Airflow
    Variable.set('apartments_raw_data_latest', output_filename)

    print(f"Apartment data saved to: {output_path}")


def create_price_histogram():
    """
    Reads filtered apartment data from the latest versioned CSV file, calculates statistics, and creates a price histogram.

    This task reads the latest versioned apartment dataset, calculates the average price per square meter
    by province, generates a bar plot using Plotly Express, and saves the plot as an HTML file for visualization.
    """
    airflow_home = os.getenv('AIRFLOW_HOME')
    output_folder = os.path.join(airflow_home, 'dags', 'data')

    # Retrieve the latest versioned filename from Airflow variables
    latest_filename = Variable.get('apartments_raw_data_latest')
    csv_file_path = os.path.join(output_folder, latest_filename)

    # Read the data from the latest versioned CSV file
    df_apartment = pd.read_csv(csv_file_path)

    # Calculate the mean price per square meter by province
    price_stats = df_apartment.groupby('province')['price_per_sqm'].agg('mean').sort_values()

    # Convert price_stats to a formatted table using tabulate
    table = tabulate(price_stats.reset_index(), headers=['Province', 'Average Price per sqm'], tablefmt='pretty')

    # Print the table
    print("Average Price per Square Meter by Province:")
    print(table)

    # Create a bar plot with Plotly Express
    fig = px.bar(price_stats.reset_index(), x='province', y='price_per_sqm', labels={'price_per_sqm': 'Average Price per sqm'})
    fig.update_layout(title='Average Price per Square Meter by Province')

    # Save the plot as an HTML file
    output_html_path = os.path.join(output_folder, 'price_histogram.html')
    fig.write_html(output_html_path)

    print(f"Price histogram saved to: {output_html_path}")


# Define PythonOperator tasks
download_and_filter_task = PythonOperator(
    task_id='download_and_filter_apartments',
    python_callable=download_and_filter_apartments,
    dag=dag
)

create_histogram_task = PythonOperator(
    task_id='create_price_histogram',
    python_callable=create_price_histogram,
    dag=dag,
)

# Set task dependencies
download_and_filter_task >> create_histogram_task
