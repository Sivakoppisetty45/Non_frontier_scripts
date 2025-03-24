import pyodbc
import pandas as pd


# Database connection details
DB_SERVER = "Q5LCAJ00001JDAD\\JDASPACE1Q"  # SQL Server instance name or IP address
DB_NAME = "IKB_PROD"  # Database name

# Dates for the range
start_date = '2024-06-01 00:00:00'
end_date = '2024-06-01 23:59:59'

# Query for testing (using placeholders for parameters)
query = """
-- Step 1: Populate temp table with relevant data
SELECT
    SUBSTRING(description, 23, 7) AS POG,
    sp.Desc1 AS POG_ENGDesc,
    sp.Desc14 AS POG_DEPT,
    sp.Desc35 AS POG_TYPE,
    SUBSTRING(CONVERT(VARCHAR, FORMAT(sp.Date2, 'yyyy-MM-dd'), 103), 0, 5) + '-' + sp.desc26 AS DealNumber
INTO #tempLivePOGs
FROM [dbo].[ix_sys_event_log] se WITH (NOLOCK)
JOIN ix_SPC_Planogram sp ON SUBSTRING(se.description, 23, 7) = sp.Name
WHERE se.Description LIKE '%''Pending'' to ''Live'' successfully%'
AND se.DBTime BETWEEN ? AND ?
AND sp.DBStatus = 1;

-- Step 2: Select transformed data
SELECT
    CASE
        WHEN POG LIKE '1_2_%' THEN 'LEFT_POG_Start_With_1'
        WHEN POG LIKE '13_%' THEN 'RIGHT_POG_Start_With_1'
        WHEN POG LIKE '02_%' THEN 'LEFT_POG_Start_With_0'
        WHEN POG LIKE '03_%' THEN 'RIGHT_POG_Start_With_0'
        WHEN POG LIKE '22_%' THEN 'LEFT_POG_Start_With_2'
        WHEN POG LIKE '23_%' THEN 'RIGHT_POG_Start_With_2'
        WHEN POG LIKE '32_%' THEN 'LEFT_POG_Start_With_3'
        WHEN POG LIKE '33_%' THEN 'RIGHT_POG_Start_With_3'
        WHEN POG LIKE '72_%' THEN 'LEFT_POG_Start_With_7'
        WHEN POG LIKE '73_%' THEN 'RIGHT_POG_Start_With_7'
        WHEN POG LIKE '82_%' THEN 'LEFT_POG_Start_With_8'
        WHEN POG LIKE '83_%' THEN 'RIGHT_POG_Start_With_8'
        ELSE 'Other Groups'
    END AS POG,
    COUNT(POG) AS pog_count
INTO #temp12
FROM #tempLivePOGs
GROUP BY POG;

-- Step 3: Final results
SELECT
    POG,
    SUM(pog_count) AS pog_total
FROM #temp12
WHERE POG <> 'Other Groups'
GROUP BY POG
ORDER BY POG;
"""


# ---------------- Fetch Data ----------------
def fetch_data():
    """Fetches data from the database using the dynamically generated date range."""
    try:
        # Connect to the SQL Server using Windows Authentication
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={DB_SERVER};DATABASE={DB_NAME};Trusted_Connection=yes;TrustServerCertificate=yes"
        )
        cursor = conn.cursor()

        # Debugging: print the actual query being executed
        print(f"Executing SQL query with start_date={start_date} and end_date={end_date}")

        # Execute the query with parameters (pass them as a tuple)
        cursor.execute(query, (start_date, end_date))

        # Debugging: Check if query execution returned any results
        if cursor.description is None:
            print("Query did not return any results.")
            return pd.DataFrame()

        # Fetch the results and convert to a DataFrame
        df = pd.read_sql(query, conn, params=(start_date, end_date))

        # Close the connection
        conn.close()
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()


# ---------------- Main Function ----------------
def main():
    """Main function to run the report generation."""
    df = fetch_data()
    if not df.empty:
        print("Data fetched successfully:")
        print(df.head())  # Print first few rows to verify
    else:
        print("No data found for the given date range.")


# ---------------- Run the Script ----------------
if __name__ == "__main__":
    main()