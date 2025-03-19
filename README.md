# Sales-Data-Processing-for-Retail-Optimization

This project aims to streamline and enhance retail operations by efficiently processing sales data for XYZ Retail Inc. The system involves data ingestion, transformation, and visualization to help optimize sales performance and decision-making.

# Sales Data Processing Pipeline

## Overview

This project automates the ingestion, transformation, and storage of sales data. The system fetches CSV files from Azure Blob Storage, processes the data (including masking sensitive information), and uploads it to an Azure SQL Database.

## Features

- **Secure Data Handling**: Uses environment variables for credentials.
- **Blob Storage Integration**: Uploads and downloads sales data CSV files.
- **Data Transformation**:
  - Masks credit card numbers.
  - Removes expired date columns.
  - Handles missing values.
  - Converts date fields.
- **Azure SQL Integration**:
  - Creates a `SalesData` table if it does not exist.
  - Inserts transformed data into the database.

## Prerequisites

- Python 3.8+
- Azure Storage Account
- Azure SQL Database
- `pip install -r requirements.txt` (for dependencies)

## Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/your-repo/sales-data-pipeline.git
   cd sales-data-pipeline
   ```
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## Environment Variables

Set up the following environment variables:

```sh
export AZURE_STORAGE_ACCOUNT="your_storage_account"
export AZURE_STORAGE_KEY="your_storage_key"
export AZURE_SQL_SERVER="your_sql_server"
export AZURE_SQL_DB="your_sql_database"
export AZURE_SQL_USER="your_sql_username"
export AZURE_SQL_PASSWORD="your_sql_password"
```

## Running the Script

```sh
python main.py
```

## Expected Output

- File uploaded to Azure Blob Storage (if not already present)
- Data loaded successfully from Azure Blob Storage
- Transformed data preview
- Data uploaded to Azure SQL Database

## Troubleshooting

- Ensure the correct environment variables are set.
- Check if Azure Storage and SQL credentials are valid.
- Verify `pyodbc` and ODBC drivers are installed.
- Use `print(df.head())` for debugging data issues.
