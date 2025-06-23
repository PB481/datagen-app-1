import streamlit as st
import pandas as pd
import duckdb
import random
from datetime import datetime, timedelta

# --- Configuration ---
# IMPORTANT: Replace 'your_motherduck_database_name' with your desired MotherDuck database name.
# It will be created if it doesn't exist when you first connect.
MOTHERDUCK_DB_NAME = "my_financial_data_db"

# IMPORTANT: You can set your MotherDuck API Token here directly,
# but it's much safer to use Streamlit Secrets or environment variables in a real application.
# For this example, we'll get it from a text input, but be cautious with sensitive info.
# MOTHERDUCK_TOKEN = "YOUR_MOTHERDUCK_API_TOKEN_HERE"


# --- Functions for Data Generation ---
def generate_financial_data(num_records=10):
    """
    Generates synthetic financial statement data.
    Each record represents a month's data.
    """
    data = []
    start_date = datetime(2023, 1, 1)

    for i in range(num_records):
        current_date = start_date + timedelta(days=i * 30) # Approximately monthly

        # Income Statement
        revenue = round(random.uniform(50000, 150000), 2)
        cogs = round(random.uniform(0.3 * revenue, 0.6 * revenue), 2)
        gross_profit = revenue - cogs
        operating_expenses = round(random.uniform(0.15 * revenue, 0.3 * revenue), 2)
        ebit = gross_profit - operating_expenses
        interest_expense = round(random.uniform(0, 0.05 * ebit) if ebit > 0 else 0, 2)
        taxes = round(random.uniform(0.15 * ebit, 0.25 * ebit) if ebit > 0 else 0, 2)
        net_income = ebit - interest_expense - taxes

        # Balance Sheet (simplified for demonstration)
        cash = round(random.uniform(10000, 50000), 2)
        accounts_receivable = round(random.uniform(5000, 20000), 2)
        inventory = round(random.uniform(3000, 15000), 2)
        total_current_assets = cash + accounts_receivable + inventory
        property_plant_equipment = round(random.uniform(50000, 200000), 2)
        total_assets = total_current_assets + property_plant_equipment

        accounts_payable = round(random.uniform(4000, 18000), 2)
        short_term_debt = round(random.uniform(2000, 10000), 2)
        total_current_liabilities = accounts_payable + short_term_debt
        long_term_debt = round(random.uniform(20000, 80000), 2)
        total_liabilities = total_current_liabilities + long_term_debt

        # Equity (calculated to balance the balance sheet)
        equity = total_assets - total_liabilities

        data.append({
            "report_date": current_date.strftime("%Y-%m-%d"),
            "revenue": revenue,
            "cost_of_goods_sold": cogs,
            "gross_profit": gross_profit,
            "operating_expenses": operating_expenses,
            "ebit": ebit,
            "interest_expense": interest_expense,
            "taxes": taxes,
            "net_income": net_income,
            "cash": cash,
            "accounts_receivable": accounts_receivable,
            "inventory": inventory,
            "total_current_assets": total_current_assets,
            "property_plant_equipment": property_plant_equipment,
            "total_assets": total_assets,
            "accounts_payable": accounts_payable,
            "short_term_debt": short_term_debt,
            "total_current_liabilities": total_current_liabilities,
            "long_term_debt": long_term_debt,
            "total_liabilities": total_liabilities,
            "equity": equity,
        })
    return pd.DataFrame(data)

# --- Functions for MotherDuck Interaction ---
@st.cache_resource
def get_motherduck_connection(token):
    """Establishes and caches the connection to MotherDuck."""
    if not token:
        st.error("MotherDuck API Token is required to connect.")
        return None
    try:
        # Connect to MotherDuck using the DuckDB client
        # The 'read_only=False' is important for writing data
        conn = duckdb.connect(f"md:{MOTHERDUCK_DB_NAME}?motherduck_token={token}", read_only=False)
        st.success(f"Successfully connected to MotherDuck database: {MOTHERDUCK_DB_NAME}")
        return conn
    except Exception as e:
        st.error(f"Error connecting to MotherDuck: {e}. Please check your token and network.")
        return None

def create_table_if_not_exists(conn, table_name, df):
    """
    Creates a table in MotherDuck based on the DataFrame's schema if it doesn't exist.
    """
    try:
        # DuckDB's FROM_DF function makes this very convenient.
        # It infers the schema from the DataFrame.
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
        st.success(f"Table '{table_name}' ensured to exist in MotherDuck with appropriate schema.")
        return True
    except Exception as e:
        st.error(f"Error creating table '{table_name}': {e}")
        return False

def insert_data_into_motherduck(conn, table_name, df, truncate_before_insert=False):
    """
    Inserts data from a DataFrame into the specified MotherDuck table.
    Optionally truncates the table before inserting.
    """
    try:
        if truncate_before_insert:
            conn.execute(f"DELETE FROM {table_name}") # Use DELETE for a more robust truncate on MotherDuck
            st.warning(f"Existing data in table '{table_name}' has been cleared.")

        # Insert DataFrame directly into the table
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        st.success(f"Successfully inserted {len(df)} records into '{table_name}'.")
        return True
    except Exception as e:
        st.error(f"Error inserting data into '{table_name}': {e}")
        return False

def fetch_data_from_motherduck(conn, table_name):
    """
    Fetches all data from the specified MotherDuck table.
    """
    try:
        df = conn.execute(f"SELECT * FROM {table_name} ORDER BY report_date DESC").fetchdf()
        st.success(f"Successfully fetched {len(df)} records from '{table_name}'.")
        return df
    except Exception as e:
        st.error(f"Error fetching data from '{table_name}': {e}")
        return pd.DataFrame() # Return empty DataFrame on error

# --- Streamlit UI ---
st.set_page_config(layout="wide", page_title="Financial Data Generator for MotherDuck")

st.title("💰 Financial Data Generator & MotherDuck Uploader")
st.markdown("""
This app generates synthetic financial statement data (Income Statement & Balance Sheet)
and uploads it to your MotherDuck database.
""")

# Input for MotherDuck API Token
st.sidebar.header("MotherDuck Configuration")
motherduck_token = st.sidebar.text_input(
    "Enter your MotherDuck API Token:", type="password", help="Find this in your MotherDuck Dashboard under 'Settings'."
)

# Connect to MotherDuck
conn = None
if motherduck_token:
    conn = get_motherduck_connection(motherduck_token)

# Main App Logic
if conn:
    st.header("Generate and Upload Data")
    num_records_to_generate = st.slider(
        "Number of monthly records to generate:", min_value=1, max_value=120, value=12, step=1
    )
    table_name = st.text_input("MotherDuck Table Name:", value="financial_statements")
    truncate_option = st.checkbox(
        "Clear existing data before uploading?",
        help="If checked, all existing data in the table will be deleted before new data is inserted. Use with caution!",
        value=False
    )

    if st.button("Generate & Upload Data to MotherDuck"):
        if table_name:
            st.subheader("Generating Data...")
            generated_df = generate_financial_data(num_records_to_generate)
            st.write("Preview of Generated Data:")
            st.dataframe(generated_df)

            st.subheader(f"Uploading to MotherDuck table '{table_name}'...")
            if create_table_if_not_exists(conn, table_name, generated_df):
                if insert_data_into_motherduck(conn, table_name, generated_df, truncate_option):
                    st.success("Data generation and upload process completed!")
                else:
                    st.error("Data insertion failed.")
            else:
                st.error("Table creation failed, cannot proceed with insertion.")
        else:
            st.warning("Please enter a table name.")

    st.markdown("---")
    st.header("View Data in MotherDuck")
    if st.button(f"Fetch Data from '{table_name}'"):
        if table_name:
            fetched_df = fetch_data_from_motherduck(conn, table_name)
            if not fetched_df.empty:
                st.dataframe(fetched_df)
                st.download_button(
                    label="Download Data as CSV",
                    data=fetched_df.to_csv(index=False).encode('utf-8'),
                    file_name=f"{table_name}_data.csv",
                    mime="text/csv",
                )
            else:
                st.info("No data found or an error occurred while fetching.")
        else:
            st.warning("Please enter a table name to fetch data.")

else:
    st.info("Please enter your MotherDuck API Token in the sidebar to get started.")

# Close the connection when the app closes (Streamlit handles this implicitly for st.cache_resource)
# For more explicit control, one might add a st.stop() or more complex session management.
