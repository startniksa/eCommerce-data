#%%
import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
from io import StringIO
import os
from airflow.models import Variable

# Define a function to get the variable first from Airflow and then from the OS environment
def get_variable(name, default=None):
    try:
        # Try to get from Airflow Variables
        return Variable.get(name)
    except KeyError:
        # Fall back to OS environment variable
        from dotenv import load_dotenv
        load_dotenv()
        return os.environ.get(name, default)
    except Exception as e:
        # If there's any other error (like running this code outside of Airflow), use environment variable
        print(f"Error retrieving variable {name}: {e}")
        return os.environ.get(name, default)

# Configuration for database connection
DB_NAME = 'electroworld'
DB_PORT = 5432
DB_USER = get_variable('DB_USER')
DB_PASSWORD = get_variable('DB_PASSWORD')
DB_HOST = get_variable('DB_HOST')

URL_PRODUCT_DATA = 'https://temus-northstar.github.io/data_engineering_case_study_public/product_data.html'
URL_VENDOR_DATA = 'https://temus-northstar.github.io/data_engineering_case_study_public/vendor_data.html'

def connect_to_db():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = True
    return conn

def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        print("Read from the URL")
        html_content = response.text
        return html_content
    else:
        raise Exception(f"Failed to fetch data from {url}")

def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    return soup

def extract_table(soup):
    table = soup.find('table')
    df = pd.read_html(StringIO(str(table)))[0]
    return df

def transform_data(df, columns, drop_column='Unnamed: 0'):
    df.drop(columns=[drop_column], inplace=True)
    df.columns = columns
    return df

def df_to_sql_upsert(conn, df, table_name, unique_cols):
    columns = ', '.join(['"%s"' % col for col in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))
    updates = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col not in unique_cols + ['updated_on']])
    update_columns = f"{updates}, \"updated_on\" = CURRENT_TIMESTAMP"

    sql = f"""
    INSERT INTO "{table_name}" ({columns})
    VALUES ({placeholders})
    ON CONFLICT ({', '.join(['"%s"' % col for col in unique_cols])}) DO UPDATE SET
    {update_columns};
    """
    with conn.cursor() as cursor:
        cursor.executemany(sql, [tuple(x) for x in df.to_numpy()])

def main():
    conn = connect_to_db()
    try:
        product_html = fetch_data(URL_PRODUCT_DATA)
        vendor_html = fetch_data(URL_VENDOR_DATA)

        product_soup = parse_html(product_html)
        vendor_soup = parse_html(vendor_html)
        
        product_df = extract_table(product_soup)
        vendor_df = extract_table(vendor_soup)

        vendor_df = transform_data(vendor_df, ['vendor_name', 'shipping_cost', 'customer_review_score', 'number_of_feedbacks'])
        product_df = transform_data(product_df, ['item', 'category', 'vendor', 'sale_price', 'stock_status'])
        
        df_to_sql_upsert(conn, vendor_df, 'vendors', ['vendor_name'])
        df_to_sql_upsert(conn, product_df, 'products', ['item', 'vendor'])

    finally:
        conn.close()
