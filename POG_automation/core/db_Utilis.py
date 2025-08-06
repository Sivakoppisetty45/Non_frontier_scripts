import pandas as pd
from sqlalchemy import create_engine
import datetime
import pytz
from POG_automation.config.settings import DB_NAME, DB_DRIVER,DB_SERVER

def get_previous_month_date_range():
    now = datetime.datetime.now(pytz.utc)
    first_day_current_month = datetime.datetime(now.year, now.month, 1, tzinfo=pytz.utc)
    last_day_previous_month = first_day_current_month - datetime.timedelta(days=1)
    first_day_previous_month = datetime.datetime(last_day_previous_month.year, last_day_previous_month.month, 1, tzinfo=pytz.utc)
    est = pytz.timezone('US/Eastern')
    start_date = first_day_previous_month.astimezone(est).replace(hour=0, minute=0, second=0)
    end_date = last_day_previous_month.astimezone(est).replace(hour=23, minute=59, second=59)
    return start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')

START_DATE, END_DATE = get_previous_month_date_range()

def fetch_data():
    try:
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect="
            f"DRIVER={{{DB_DRIVER}}};SERVER={DB_SERVER};DATABASE={DB_NAME};Trusted_Connection=yes;"
        )
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
        df_tempLivePOGs = pd.read_sql(QUERY_1, engine)

        def categorize_pog(pog):
            if pog.startswith('1') and '2' in pog[2:4]: return 'LEFT_POG_Start_With_1'
            elif pog.startswith('1') and '3' in pog[2:4]: return 'RIGHT_POG_Start_With_1'
            elif pog.startswith('0') and '2' in pog[2:4]: return 'LEFT_POG_Start_With_0'
            elif pog.startswith('0') and '3' in pog[2:4]: return 'RIGHT_POG_Start_With_0'
            elif pog.startswith('2') and '2' in pog[2:4]: return 'LEFT_POG_Start_With_2'
            elif pog.startswith('2') and '3' in pog[2:4]: return 'RIGHT_POG_Start_With_2'
            elif pog.startswith('3') and '2' in pog[2:4]: return 'LEFT_POG_Start_With_3'
            elif pog.startswith('3') and '3' in pog[2:4]: return 'RIGHT_POG_Start_With_3'
            elif pog.startswith('7') and '2' in pog[2:4]: return 'LEFT_POG_Start_With_7'
            elif pog.startswith('7') and '3' in pog[2:4]: return 'RIGHT_POG_Start_With_7'
            elif pog.startswith('8') and '2' in pog[2:4]: return 'LEFT_POG_Start_With_8'
            elif pog.startswith('8') and '3' in pog[2:4]: return 'RIGHT_POG_Start_With_8'
            else: return 'Other Groups'

        df_tempLivePOGs['POG_Group'] = df_tempLivePOGs['POG'].apply(categorize_pog)
        df_temp12 = df_tempLivePOGs.groupby('POG_Group').size().reset_index(name='pog_count')
        df_result = df_temp12[df_temp12['POG_Group'] != 'Other Groups']
        df_result = df_result.groupby('POG_Group')['pog_count'].sum().reset_index()
        df_result = df_result.sort_values(by='POG_Group')
        return df_result

    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()
