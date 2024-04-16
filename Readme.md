## Visualizing the data

The exploratory analysis is stored in the file `electroworld_analysis.py`.

Created a Google Colab file with the same code:
https://colab.research.google.com/drive/1sNZJJeSGmn9OIc6WGhKwxXN873rSblGe?usp=sharing

This is a URL with a running application. The user needs to log in with a Google Account, then click on Runtime > Run All (Ctrl + F9).

The Jupyer Notebook code contains a data pipeline that will ensure regular updates in data storage.

The code runs some reasonable aggregations.

Assumption: ElectroWorld is our client, but it exists as vendor in the database. We assume it's the same company.

The conclusion of the analysis is that the data is not real, but randomly generated.
If the data was real, a useful dashboard would be a table which shows the cheapest prices of the competition, together with the prices of Electroworld. The client can then adjust the prices.

If the data provides actionable insights, we could:

* Sync it regularly to our Postgres database - I have created Airflow code for this in the folder `dags`. The database schema is in `sql`.

* Display it in a dashboard tool - I have created a prototype in Metabase.

## Syncing the data to a Postgres database

It is done using Airflow on AWS. The database is also hosted on AWS.

The Airflow DAG is defined in `dags/data_sync_dag.py`

The Python code for syncing the data from the website to the database is defined in `dags/data_sync_script.py`.

The data is downloaded from the HTML files and stored in an AWS RDS database.
The credentials to the database are stored:
- For local development in an `.env` file
- For production in Airflow variables.

In order to recreate the tables if needed, execute the file: `sql/schema.sql`