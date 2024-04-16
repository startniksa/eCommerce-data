# -*- coding: utf-8 -*-
"""Electroworld analysis.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1sNZJJeSGmn9OIc6WGhKwxXN873rSblGe

# eCommerce data about products provided by various vendors - Exploratory analysis

The data is stored in a Postgres database on AWS. It contains two tables: `products` and `vendors`.
"""

# @title importing the data
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import os

# URLs for fetching data
URL_PRODUCT_DATA = 'https://temus-northstar.github.io/data_engineering_case_study_public/product_data.html'
URL_VENDOR_DATA = 'https://temus-northstar.github.io/data_engineering_case_study_public/vendor_data.html'

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

def save_df_to_csv(df, file_name):
    df.to_csv(file_name, index=False)
    print(f"Data saved to {file_name}")

product_html = fetch_data(URL_PRODUCT_DATA)
vendor_html = fetch_data(URL_VENDOR_DATA)

product_soup = parse_html(product_html)
vendor_soup = parse_html(vendor_html)

product_df = extract_table(product_soup)
vendor_df = extract_table(vendor_soup)

vendor_df = transform_data(vendor_df, ['vendor_name', 'shipping_cost', 'customer_review_score', 'number_of_feedbacks'])
product_df = transform_data(product_df, ['item', 'category', 'vendor', 'sale_price', 'stock_status'])

# Saving dataframes to CSV files
save_df_to_csv(product_df, 'products.csv')
save_df_to_csv(vendor_df, 'vendors.csv')

products_df = pd.read_csv('products.csv')
vendors_df = pd.read_csv('vendors.csv')

products_df.head()

vendors_df.head()

"""## Assumption: ElectroWorld is our client, but it exists as vendor in the database. We assume it's the same company.

# Insights from the data:

## The data seems to be randomly generated.

### The different vendors have roughly the same prices, even though they have very different "customer review scores", "number of feedbacks" and "shipping costs".
"""

# @title vendor vs sale_price

figsize = (12, 1.2 * len(products_df['vendor'].unique()))
plt.figure(figsize=figsize)
sns.violinplot(products_df, x='sale_price', y='vendor', inner='box', hue='vendor')
sns.despine(top=True, right=True, bottom=True, left=True)

"""### The "Stock status" seems to be randomly distributed between the 1000 products + vendor combinations. Having less or more stock of an item has no relationship with the prices of the items"""

# @title
# Compute stock status summary
stock_status_summary = products_df.pivot_table(index='category', columns='stock_status', aggfunc='size', fill_value=0)

# Plot
stock_status_summary.plot(kind='bar', figsize=(10, 6), stacked=True)
plt.xlabel('Category')
plt.ylabel('Count')
plt.title('Stock Status Summary')
plt.xticks(rotation=0)
plt.show()

# @title stock_status vs sale_price
figsize = (12, 1.2 * len(products_df['stock_status'].unique()))
plt.figure(figsize=figsize)
sns.violinplot(products_df, x='sale_price', y='stock_status', inner='box', hue='stock_status')
sns.despine(top=True, right=True, bottom=True, left=True)

# @title vendor vs stock_status

from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
plt.subplots(figsize=(8, 8))
df_2dhist = pd.DataFrame({
    x_label: grp['stock_status'].value_counts()
    for x_label, grp in products_df.groupby('vendor')
})
sns.heatmap(df_2dhist, cmap='viridis')
plt.xlabel('vendor')
_ = plt.ylabel('stock_status')

"""### The items in the different categories have the same prices on average."""

# @title category vs sale_price

figsize = (12, 1.2 * len(products_df['category'].unique()))
plt.figure(figsize=figsize)
sns.violinplot(products_df, x='sale_price', y='category', inner='box', hue='category')
sns.despine(top=True, right=True, bottom=True, left=True)

"""### The "Total Sales by Vendor" and "Average Price by Vendor" are also almost evenly distributed."""

# @title
# Aggregate data
sales_summary = products_df.groupby('vendor').agg(total_sales=('sale_price', 'sum'),
                                                  average_price=('sale_price', 'mean'),
                                                  item_count=('item', 'count')).reset_index()

# Plot total sales
plt.figure(figsize=(10, 6))
plt.bar(sales_summary['vendor'], sales_summary['total_sales'], color='lightgreen')
plt.xlabel('Vendor')
plt.ylabel('Total Sales')
plt.title('Total Sales by Vendor')
plt.xticks(rotation=45)
plt.show()

# @title
# Plot average price
plt.figure(figsize=(10, 6))
plt.bar(sales_summary['vendor'], sales_summary['average_price'], color='lightblue')
plt.xlabel('Vendor')
plt.ylabel('Average Price')
plt.title('Average Price by Vendor')
plt.xticks(rotation=45)
plt.show()

"""### We have very large "Item Price Variability" - the difference between the maximum and minimum prices for a product. Here I have visualized the data for the smartphones."""

# @title
# Calculate price variability
price_variability = products_df[products_df["item"].map(lambda x: "Smartphone" in str(x))].groupby('item').agg(minimum_price=('sale_price', 'min'),
                                                    maximum_price=('sale_price', 'max')).reset_index()
price_variability['price_variance'] = price_variability['maximum_price'] - price_variability['minimum_price']

# Plot
plt.figure(figsize=(14, 7))
plt.bar(price_variability['item'], price_variability['price_variance'], color='purple')
plt.xlabel('Item')
plt.ylabel('Price Variance')
plt.title('Item Price Variability - Maximum Price minus Minimum Price')
plt.xticks(rotation=90)
plt.show()

# @title
# Filter for items containing the word "Smartphone"
smartphone_data = products_df[products_df['item'].str.contains("Smartphone")]

# We only want to visualize prices for smartphones
smartphone_prices = products_df[products_df['item'].str.contains("Smartphone")]

# Create a pivot table where the index is the item, columns are the vendors, and values are the sale prices
price_comparison = smartphone_prices.pivot_table(index='item', columns='vendor', values='sale_price', aggfunc='mean')

# Plotting
fig, ax = plt.subplots(figsize=(14, 7))

# Iterate over each column (vendor) to plot the bars individually
for vendor in price_comparison.columns:
    ax.bar(price_comparison.index, price_comparison[vendor], label=vendor)

# Remove the bars with 0 values
for bar in ax.patches:
    if bar.get_height() == 0:
        bar.set_visible(False)
    else:
        # Add value labels for non-zero bars
        ax.annotate(str(round(bar.get_height(), 2)),
                    (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    ha='center', va='bottom')

# Final touches on the plot
plt.xlabel('Item')
plt.ylabel('Price')
plt.title('Comparison of Smartphone Prices by Vendor')
plt.xticks(rotation=90)
plt.legend(title='Vendor')
plt.tight_layout()
plt.show()

"""# Conclusion

This doesn't look like real-world data.

# If we assume that the data is real, some useful aggregation could be a table which shows the cheapest prices of the competition, together with the prices of Electroworld. The client can then adjust the prices.
"""

# @title
# Step 1: Create a mask for the desired stock statuses and vendors
mask = (
    (products_df['stock_status'].isin(['In Stock', 'Low Stock'])) &
    ((products_df['vendor'] != 'ElectroWorld') | (products_df['vendor'].isnull()))
)

# Compute the minimum sale price for each item under the conditions
min_prices = products_df[mask].groupby('item')['sale_price'].min().reset_index(name='min')

# Step 2: Prepare the DataFrame for the left join operation
electroworld_products = products_df[
    (products_df['vendor'] == 'ElectroWorld') &
    (products_df['stock_status'].isin(['In Stock', 'Low Stock']))
]

# Step 3: Left join the two DataFrames on the 'item' column
result = pd.merge(
    min_prices,
    electroworld_products,
    on='item',
    how='left',
    suffixes=('', '__ElectroWorld')
)

# Renaming columns to match the original SQL output (optional)
result.rename(columns={
    'sale_price': 'Products - Min of Sale Price__sale_price',
    'category': 'Products - Min of Sale Price__category',
    'vendor': 'Products - Min of Sale Price__vendor',
    'stock_status': 'Products - Min of Sale Price__stock_status',
    'updated_on': 'Products - Min of Sale Price__updated_on'
}, inplace=True)

# Step 4: Sort by 'item' if needed (though merge should retain order)
result_sorted = result.sort_values('item')
result_sorted.columns = ['item', 'Minimum Price of Competition', 'Products - Min of Sale Price__category',
       'Products - Min of Sale Price__vendor',
       'Minimum Price of Electroworld',
       'Stock Status of Electroworld']
# Create an independent copy of the result_sorted DataFrame to avoid chained assignment
df_cleaned = result_sorted[['item', 'Minimum Price of Competition', 'Minimum Price of Electroworld',
       'Stock Status of Electroworld']].copy().dropna(subset=['Minimum Price of Electroworld'])

# Calculate the price difference using .loc for safe assignment
df_cleaned.loc[:, 'Price Difference'] = df_cleaned['Minimum Price of Electroworld'] - df_cleaned['Minimum Price of Competition']

# Sort the DataFrame based on the 'Price Difference' column
df_sorted_by_difference = df_cleaned.sort_values('Price Difference', ascending=True)

# Display the sorted DataFrame
df_sorted_by_difference

# @title Price Difference

from matplotlib import pyplot as plt
df_sorted_by_difference['Price Difference'].plot(kind='hist', bins=20, title='Price Difference')
plt.gca().spines[['top', 'right',]].set_visible(False)

"""### The Price Difference is also normally distributed, which suggests not real-world data."""