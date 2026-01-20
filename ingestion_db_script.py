import pandas as pd
import os 
from sqlalchemy import create_engine
import logging 
import time

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# log file name
logging.basicConfig(
    filename=r"C:\Users\RISHAB\Downloads\DA_project_data\data\logs\db_ingestion.log",  # Different name
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine(r'sqlite:///D:\Inventory_project.db')

def ingest_db(df, table_name, engine):
    ''' This function ingest dataframe into the database table. '''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    
def load_raw_data():
    ''' This function will load CSVs as a DataFrame into the Database. '''
    start = time.time()
    for file in os.listdir(r'C:\Users\RISHAB\Downloads\DA_project_data\data'):
        if '.csv' in file:
            df = pd.read_csv(r'C:\Users\RISHAB\Downloads\DA_project_data\data/' + file)
            logging.info(f'Ingesting {file} in db.')
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start)/60
    logging.info('-------Ingestion complete-------')
    logging.info(f'Total Time Taken: {total_time} minutes')
# To runn the script
if __name__ == "__main__":
    load_raw_data()