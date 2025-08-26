from helper import PorstgresFunctions
import pyarrow.dataset as ds
import os
from dotenv import load_dotenv
load_dotenv()

#Cofig con
conn_params = {
    "host": os.environ.get("POSTGRES_HOST"),
    "dbname": os.environ.get("POSTGRES_WAREHOUSE_DB"),
    "username": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "port": os.environ.get("POSTGRES_PORT")
}

postgres = PorstgresFunctions(**conn_params)
    
#Create connection
postgres.create_connection()

#Check table extist
check_table = postgres.check_table_exists('data_sample')
if not check_table:
    print("data_sample not exist")
    #Create table
    with open('/opt/airflow/sql/create_table_data_sample.sql', 'r') as create_table_data_sample_sql_file:
        create_table_data_sample = create_table_data_sample_sql_file.read()
    postgres.execute_query(create_table_data_sample)

#Create partion 
dataset = ds.dataset('/opt/airflow/data/data_sample/', format='parquet')
scanner_create_at = dataset.scanner(columns=['create_at'])
unique_dates = set()
for batch in scanner_create_at.to_batches():
    df = batch.to_pandas()
    unique_dates.update(df['create_at'].dt.date.unique())

postgres.create_partion_table("data_sample", unique_dates)

#Load Data to table
from io import StringIO
dataset = ds.dataset('/opt/airflow/data/data_sample/', format='parquet')
scanner = dataset.scanner(batch_size=10000)
postgres.create_connection()

for batch in scanner.to_batches():
    df = batch.to_pandas()
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t')
    buffer.seek(0)
    postgres.connection.cursor().copy_from(buffer, 'data_sample', sep='\t', columns=df.columns)
    postgres.connection.commit()
    
postgres.connection.cursor().close()
postgres.connection.close()