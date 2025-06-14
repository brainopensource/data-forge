import duckdb
import pandas as pd
from app.config.settings import settings

# Set pandas display options for better output
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 200)

DB_PATH = settings.DATABASE_PATH
TABLE_NAME = "well_production"

def verify_data():
    """Connects to the database and prints specific rows for verification."""
    print(f"Connecting to database at: {DB_PATH}")
    try:
        con = duckdb.connect(database=DB_PATH, read_only=True)

        # Get total number of rows
        total_rows_query = f"SELECT COUNT(*) FROM {TABLE_NAME}"
        total_rows = con.execute(total_rows_query).fetchone()[0]
        print(f"Total rows in '{TABLE_NAME}': {total_rows}")

        if total_rows == 0:
            print("Table is empty. No data to display.")
            return

        print("\n--- Fetching rows with field_code 0, 3, and max value ---")

        # Queries
        query_fc_0 = f"SELECT * FROM {TABLE_NAME} WHERE field_code = 0 LIMIT 1"
        query_fc_3 = f"SELECT * FROM {TABLE_NAME} WHERE field_code = 3 LIMIT 1"
        query_fc_max = f"SELECT * FROM {TABLE_NAME} ORDER BY field_code DESC LIMIT 1"
        
        # Fetch data as pandas DataFrames
        df_fc_0 = con.execute(query_fc_0).fetchdf()
        df_fc_3 = con.execute(query_fc_3).fetchdf()
        df_fc_max = con.execute(query_fc_max).fetchdf()

        # Combine and print
        result_df = pd.concat([df_fc_0, df_fc_3, df_fc_max]).reset_index(drop=True)
        result_df.index = ['Field Code 0', 'Field Code 3', 'Max Field Code']
        
        print("\n--- Query Results ---")
        print(result_df)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'con' in locals():
            con.close()

if __name__ == "__main__":
    verify_data() 