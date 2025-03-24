import pandas as pd
from sqlalchemy import create_engine

# Database connection details
DB_SERVER = "Q5LCAJ00001JDAD\\JDASPACE1Q"  # SQL Server instance name or IP address
DB_NAME = "IKB_PROD"  # Database name
DB_DRIVER = "ODBC+Driver+17+for+SQL+Server"

# Date range for the query
START_DATE = '2024-06-01 00:00:00'
END_DATE = '2024-06-01 23:59:59'


# Function to fetch data from the database
def fetch_data():
    """Fetches data from the database."""
    try:
        # Create SQLAlchemy engine
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect="
            f"DRIVER={{{DB_DRIVER}}};SERVER={DB_SERVER};DATABASE={DB_NAME};Trusted_Connection=yes;"
        )

        # Step 1: Create #tempLivePOGs table with POG data
        QUERY_1 = f"""
        SELECT SUBSTRING(description, 23, 7) as POG,
               sp.Desc1 as POG_ENGDesc,
               sp.desc14 as POG_DEPT,
               sp.Desc35 as POG_TYPE,
               substring(CONVERT(varchar, FORMAT(sp.Date2, 'yyyy-mm-dd'), 103), 0, 5) + '-' + sp.desc26 AS DealNumber
        FROM [dbo].[ix_sys_event_log] se WITH (NOLOCK)
        JOIN ix_SPC_Planogram sp ON SUBSTRING(se.description, 23, 7) = sp.Name
        WHERE Description LIKE '%''Pending'' to ''Live'' successfully%'
        AND se.DBTime BETWEEN '{START_DATE}' AND '{END_DATE}'
        AND sp.DBStatus = 1;
        """

        # Fetch the data for step 1
        df_tempLivePOGs = pd.read_sql(QUERY_1, engine)

        # Step 2: Categorize POGs into LEFT/RIGHT groups
        def categorize_pog(pog):
            if pog.startswith('1') and '2' in pog[2:4]:
                return 'LEFT_POG_Start_With_1'
            elif pog.startswith('1') and '3' in pog[2:4]:
                return 'RIGHT_POG_Start_With_1'
            elif pog.startswith('0') and '2' in pog[2:4]:
                return 'LEFT_POG_Start_With_0'
            elif pog.startswith('0') and '3' in pog[2:4]:
                return 'RIGHT_POG_Start_With_0'
            elif pog.startswith('2') and '2' in pog[2:4]:
                return 'LEFT_POG_Start_With_2'
            elif pog.startswith('2') and '3' in pog[2:4]:
                return 'RIGHT_POG_Start_With_2'
            elif pog.startswith('3') and '2' in pog[2:4]:
                return 'LEFT_POG_Start_With_3'
            elif pog.startswith('3') and '3' in pog[2:4]:
                return 'RIGHT_POG_Start_With_3'
            elif pog.startswith('7') and '2' in pog[2:4]:
                return 'LEFT_POG_Start_With_7'
            elif pog.startswith('7') and '3' in pog[2:4]:
                return 'RIGHT_POG_Start_With_7'
            elif pog.startswith('8') and '2' in pog[2:4]:
                return 'LEFT_POG_Start_With_8'
            elif pog.startswith('8') and '3' in pog[2:4]:
                return 'RIGHT_POG_Start_With_8'
            else:
                return 'Other Groups'

        df_tempLivePOGs['POG_Group'] = df_tempLivePOGs['POG'].apply(categorize_pog)

        # Step 3: Group and count POGs by categories
        df_temp12 = df_tempLivePOGs.groupby('POG_Group').size().reset_index(name='pog_count')

        # Step 4: Summarize and order the POG counts
        df_result = df_temp12[df_temp12['POG_Group'] != 'Other Groups']
        df_result = df_result.groupby('POG_Group')['pog_count'].sum().reset_index()
        df_result = df_result.sort_values(by='POG_Group')

        print("Data fetched successfully.")
        return df_result

    except Exception as e:
        print(f"An error occurred while fetching data: {e}")
        return pd.DataFrame()


# Function to export the data to an Excel file
def export_to_excel(df, output_file="POG_report.xlsx"):
    """Exports the DataFrame to an Excel file."""
    try:
        if df.empty:
            print("No data to export.")
        else:
            df.to_excel(output_file, index=False)
            print(f"Data successfully exported to {output_file}")
    except Exception as e:
        print(f"An error occurred while exporting to Excel: {e}")


# Main function
def main():
    """Main function to fetch and export data."""
    df = fetch_data()
    if not df.empty:
        print("Preview of fetched data:")
        print(df.head())  # Display the first few rows
        export_to_excel(df)
    else:
        print("No data found for the specified date range.")


# Entry point
if __name__ == "__main__":
    main()