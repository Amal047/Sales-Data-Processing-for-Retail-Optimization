import os
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import pyodbc

# Load credentials securely (use environment variables instead)
storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_account_key = os.getenv("AZURE_STORAGE_KEY")
container_name = "sales-data"
csv_file_path = "path_to_your_sales_data.csv"
blob_file_name = "sales_data.csv"

# Initialize BlobServiceClient securely
try:
    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage_account_name}.blob.core.windows.net",
        credential=storage_account_key
    )
    
    # Create a container if it doesn't exist
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()
    
    # Upload CSV if it doesn’t already exist
    blob_client = container_client.get_blob_client(blob_file_name)
    if not blob_client.exists():
        with open(csv_file_path, "rb") as data:
            blob_client.upload_blob(data)
        print("File uploaded to Azure Blob Storage.")
    else:
        print("File already exists in Blob Storage.")
except Exception as e:
    print(f"Error with Azure Blob Storage: {e}")
    exit(1)

# Download CSV from Blob Storage
try:
    blob_data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(blob_data))
    print("Data loaded successfully from Azure Blob Storage.")
except Exception as e:
    print(f"Error downloading CSV: {e}")
    exit(1)

# Data Transformation
def mask_credit_card(card_number):
    """Masks credit card numbers except for the first and last 4 digits."""
    if pd.isna(card_number) or len(str(card_number)) < 8:
        return "****"
    return f"{str(card_number)[:4]} **** **** {str(card_number)[-4:]}"

# Apply transformations
if "CreditCardNumber" in df.columns:
    df["CreditCardNumber"] = df["CreditCardNumber"].astype(str).apply(mask_credit_card)

if "ExpiryDate" in df.columns:
    df.drop(columns=["ExpiryDate"], inplace=True)

df.fillna(method="ffill", inplace=True)  # Handling missing values

if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")  # Convert Date

print(df.info())

# Azure SQL Database Connection
server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DB")
username = os.getenv("AZURE_SQL_USER")
password = os.getenv("AZURE_SQL_PASSWORD")
driver = "{ODBC Driver 17 for SQL Server}"

try:
    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}"
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'SalesData')
    CREATE TABLE SalesData (
        OrderID NVARCHAR(50),
        CustomerName NVARCHAR(100),
        PhoneNumber NVARCHAR(20),
        Location NVARCHAR(100),
        Country NVARCHAR(50),
        StoreCode NVARCHAR(50),
        Product NVARCHAR(100),
        Quantity INT,
        Price DECIMAL(10, 2),
        Date DATE,
        CreditCardNumber NVARCHAR(20)
    )
    """)
    conn.commit()

    # Insert data efficiently using batch execution
    insert_query = """
    INSERT INTO SalesData (OrderID, CustomerName, PhoneNumber, Location, Country, StoreCode, Product, Quantity, Price, Date, CreditCardNumber)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    data_to_insert = [
        tuple(row) for row in df.itertuples(index=False, name=None)
    ]

    cursor.fast_executemany = True
    cursor.executemany(insert_query, data_to_insert)
    conn.commit()
    print("Data uploaded to Azure SQL Database.")

except Exception as e:
    print(f"Database error: {e}")
finally:
    conn.close()
