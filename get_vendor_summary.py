import pandas as pd
import sqlite3
import logging 
import time
from ingestion_db_script import ingest_db
logging.basicConfig(
    filename = "logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a" 
)
def create_vendor_summary(conn):
    ''' This function will merge different tables to get an overall vendor summary and add new columns in the resultant data.'''
    vendor_sales_summary = pd.read_sql_query(""" WITH freightSummary AS (
        SELECT 
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            pp.PurchasePrice AS ActualPrice,
            pp.Volume,
            SUM(p.Dollars) AS TotalPurchaseDollars,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            COUNT(DISTINCT p.Brand) AS TotalPurchaseBrands
        FROM purchases AS p
        JOIN purchase_prices AS pp
        ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    
    SalesSummary AS (
        SELECT
        VendorNo,
        VendorName,
        Brand,
        SUM(SalesPrice) AS TotalSalesPrice,
        SUM(SalesDollars) AS TotalSalesDollars,
        SUM(SalesQuantity) AS TotalSalesQuantity,
        SUM(ExciseTax) AS TotalExciseTax
        From sales
        GROUP BY VendorNo, VendorName, Brand
    )
    
    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary AS ps
    LEFT JOIN SalesSummary AS ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary AS fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC""",conn)
    return vendor_sales_summary
def clean_data(df):
    ''' This function is for data cleaning. '''
    # Changing data type in volume
    df['Volume'] = df['Volume'].astype('float')
    # Filling missing values with 0 in nulls.
    df.fillna(0,inplace = True)
    # Removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    # Creating New columns to add in the summary table
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit']/df['TotalSalesDollars'])*100
    df['StockTurnover'] = df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars']/df['TotalPurchaseDollars']
    return df
    
if __name__ == '__main__':
    #creating database connection
    conn = sqlite3.connect('D:\Inventory_project.db')
    # Creating the summary table
    logging.info('Creating vendor summary table.....')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())
    # Cleaning the data and inconsistencies in summary in tables 
    logging.info('Cleaning data....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())
    # Ingesting the data into the databse
    logging.info('Ingesting Data......')
    ingest_db(clean_df, 'vendor_sales_summary',conn)
    logging.info('Completed')