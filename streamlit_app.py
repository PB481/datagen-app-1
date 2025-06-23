import streamlit as st
import pandas as pd
import duckdb
import random
from datetime import datetime, timedelta

# --- Configuration ---
MOTHERDUCK_DB_NAME = "my_financial_data_db" # The MotherDuck database name

# Define the supported fund types and their simplified characteristics for data generation
FUND_TYPES = {
    "Traditional UCITS": {
        "description": "Funds regulated by UCITS directives, typically investing in liquid assets like equities and bonds.",
        "asset_classes": ["Equity", "Fixed Income", "Cash"],
        "transaction_frequency": {"min": 1, "max": 5}, # Days between transactions
    },
    "Private Equity": {
        "description": "Funds investing in private companies, characterized by illiquid investments and long holding periods.",
        "asset_classes": ["Private Equity", "Cash"],
        "transaction_frequency": {"min": 30, "max": 180}, # Days between transactions (less frequent)
    },
    # Add other fund types as needed in future iterations
    # "AIFMD Alternatives Funds": {},
    # "FOHF": {},
    # "FOF": {},
    # "Real Estate": {},
    # "Crypto Fund": {},
}

# --- Functions for Data Generation ---

def generate_financial_statements(num_records=10):
    """
    Generates synthetic basic financial statement data (Income Statement & Balance Sheet).
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

def generate_securities_transactions(num_transactions, fund_type):
    """
    Generates synthetic securities transaction data based on fund type.
    """
    transactions = []
    fund_config = FUND_TYPES.get(fund_type, {})
    asset_classes = fund_config.get("asset_classes", ["Equity"])
    min_days_between_tx = fund_config["transaction_frequency"]["min"]
    max_days_between_tx = fund_config["transaction_frequency"]["max"]

    current_date = datetime(2023, 1, 1)

    for i in range(num_transactions):
        current_date += timedelta(days=random.randint(min_days_between_tx, max_days_between_tx))
        security_id = f"SEC{random.randint(1000, 9999)}"
        transaction_type = random.choice(["BUY", "SELL"])
        quantity = random.randint(100, 1000)
        price = round(random.uniform(10.0, 500.0), 2)
        amount = round(quantity * price, 2)
        currency = random.choice(["USD", "EUR", "GBP"])
        asset_class = random.choice(asset_classes)

        transactions.append({
            "transaction_id": f"TRN{i+1:05d}",
            "fund_type": fund_type,
            "transaction_date": current_date.strftime("%Y-%m-%d"),
            "security_id": security_id,
            "security_name": f"{asset_class} - {security_id}",
            "transaction_type": transaction_type,
            "quantity": quantity,
            "price": price,
            "amount": amount,
            "currency": currency,
            "asset_class": asset_class,
        })
    return pd.DataFrame(transactions)

def generate_portfolio_data(num_securities, fund_type):
    """
    Generates synthetic portfolio data based on fund type.
    """
    portfolio_holdings = []
    fund_config = FUND_TYPES.get(fund_type, {})
    asset_classes = fund_config.get("asset_classes", ["Equity"])
    valuation_date = datetime.now().strftime("%Y-%m-%d")

    for i in range(num_securities):
        security_id = f"SEC{random.randint(1000, 9999)}"
        quantity = random.randint(500, 5000)
        cost_basis = round(random.uniform(50.0, 400.0), 2)
        current_price = round(random.uniform(cost_basis * 0.8, cost_basis * 1.2), 2) # Price fluctuation
        market_value = round(quantity * current_price, 2)
        unrealized_gain_loss = round(market_value - (quantity * cost_basis), 2)
        currency = random.choice(["USD", "EUR", "GBP"])
        asset_class = random.choice(asset_classes)

        portfolio_holdings.append({
            "portfolio_date": valuation_date,
            "fund_type": fund_type,
            "security_id": security_id,
            "security_name": f"{asset_class} - {security_id}",
            "asset_class": asset_class,
            "quantity": quantity,
            "cost_basis": cost_basis,
            "current_price": current_price,
            "market_value": market_value,
            "unrealized_gain_loss": unrealized_gain_loss,
            "currency": currency,
        })
    return pd.DataFrame(portfolio_holdings)

# --- Centralized Report Generation Dispatcher ---
def generate_selected_reports(fund_type, num_records_per_report, reports_to_generate_list):
    """
    Dispatches calls to appropriate generation functions based on selected reports.
    Returns a dictionary of {table_name: DataFrame}.
    """
    generated_reports_dfs = {}

    if "Financial Statements" in reports_to_generate_list:
        generated_reports_dfs["financial_statements"] = generate_financial_statements(num_records_per_report)
    if "Securities Transactions" in reports_to_generate_list:
        generated_reports_dfs["securities_transactions"] = generate_securities_transactions(num_records_per_report * 5, fund_type) # More transactions than general statements
    if "Portfolio Holdings" in reports_to_generate_list:
        generated_reports_dfs["portfolio_holdings"] = generate_portfolio_data(num_records_per_report, fund_type)
    # Add conditions for other report types here as their generation functions are implemented

    return generated_reports_dfs

# --- Functions for MotherDuck Interaction ---
@st.cache_resource
def get_motherduck_connection(token):
    """
    Establishes and caches the connection to MotherDuck, ensuring the target database exists.
    """
    if not token:
        st.error("MotherDuck API Token is required to connect.")
        return None
    try:
        # First, connect to MotherDuck's default environment
        # This prevents the "database not found" error if the DB doesn't exist yet.
        conn = duckdb.connect(f"md:?motherduck_token={token}", read_only=False)
        st.success("Successfully connected to MotherDuck default environment.")

        # Ensure the target database exists and switch to it
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DB_NAME}")
        conn.execute(f"USE {MOTHERDUCK_DB_NAME}")
        st.success(f"Ensured MotherDuck database '{MOTHERDUCK_DB_NAME}' exists and is in use.")

        return conn
    except Exception as e:
        st.error(f"Error connecting to MotherDuck: {e}. Please check your token and network.")
        return None

def create_table_if_not_exists(conn, table_name, df):
    """
    Creates a table in MotherDuck based on the DataFrame's schema if it doesn't exist.
    This will execute within the currently USEd database.
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
    This will execute within the currently USEd database.
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
    This will execute within the currently USEd database.
    """
    try:
        df = conn.execute(f"SELECT * FROM {table_name}").fetchdf() # Removed ORDER BY for general fetch
        st.success(f"Successfully fetched {len(df)} records from '{table_name}'.")
        return df
    except Exception as e:
        st.error(f"Error fetching data from '{table_name}': {e}")
        return pd.DataFrame() # Return empty DataFrame on error

# --- Streamlit UI ---
st.set_page_config(layout="wide", page_title="Financial Data Generator for MotherDuck")

st.title("💰 Financial Data Generator & MotherDuck Uploader")
st.markdown("""
This app generates synthetic financial data for various fund types and report categories,
then uploads it to your MotherDuck database.
""")

# Input for MotherDuck API Token (using st.secrets for persistence)
st.sidebar.header("MotherDuck Configuration")
motherduck_token = None
if "MOTHERDUCK_TOKEN" in st.secrets:
    motherduck_token = st.secrets["MOTHERDUCK_TOKEN"]
    st.sidebar.success("MotherDuck API Token loaded from secrets.toml")
else:
    # Fallback to manual input if not in secrets (for local development without secrets.toml)
    motherduck_token = st.sidebar.text_input(
        "Enter your MotherDuck API Token:", type="password",
        help="Paste your token here. For production, add it to `.streamlit/secrets.toml`."
    )
    if not motherduck_token:
        st.sidebar.warning("Please enter your MotherDuck API Token or configure `secrets.toml`.")


# Connect to MotherDuck
conn = None
if motherduck_token:
    conn = get_motherduck_connection(motherduck_token)

# Main App Logic
if conn:
    st.header("Generate and Upload Data")

    # Fund Type Selection
    selected_fund_type = st.selectbox(
        "Select Fund Type:",
        options=list(FUND_TYPES.keys()),
        index=0,
        help="Choose the type of fund for which to generate data."
    )
    st.info(FUND_TYPES[selected_fund_type]["description"])


    # Report Selection
    available_reports = [
        "Financial Statements",
        "Securities Transactions",
        "Portfolio Holdings",
        # Add more reports here as their generation functions are implemented
    ]
    reports_to_generate_selected = st.multiselect(
        "Select Reports to Generate:",
        options=available_reports,
        default=["Financial Statements"],
        help="Choose which financial reports to generate data for."
    )

    num_records_per_report = st.slider(
        "Base number of records per report (approx. months/items):", min_value=1, max_value=120, value=12, step=1
    )

    truncate_all_selected_tables = st.checkbox(
        "Clear existing data from all selected report tables before uploading?",
        help="If checked, all existing data in the tables corresponding to the selected reports will be deleted before new data is inserted. Use with extreme caution!",
        value=False
    )

    if st.button("Generate & Upload Selected Reports to MotherDuck"):
        if not reports_to_generate_selected:
            st.warning("Please select at least one report to generate.")
        else:
            st.subheader(f"Generating Data for {selected_fund_type}...")
            generated_data_dfs = generate_selected_reports(
                selected_fund_type, num_records_per_report, reports_to_generate_selected
            )

            if generated_data_dfs:
                st.write("Preview of Generated Data (First Report):")
                # Show preview of the first generated dataframe
                first_report_name = list(generated_data_dfs.keys())[0]
                st.dataframe(generated_data_dfs[first_report_name].head())
                st.info(f"Showing first 5 rows of '{first_report_name}' table preview.")

                st.subheader("Uploading to MotherDuck...")
                for report_table_name, df_to_upload in generated_data_dfs.items():
                    st.write(f"Processing table: `{report_table_name}`")
                    if create_table_if_not_exists(conn, report_table_name, df_to_upload):
                        insert_data_into_motherduck(
                            conn, report_table_name, df_to_upload, truncate_all_selected_tables
                        )
                    else:
                        st.error(f"Failed to create/ensure table '{report_table_name}'. Skipping data insertion.")
                st.success("All selected report data generation and upload processes completed!")
            else:
                st.warning("No data was generated. Please check your selections.")

    st.markdown("---")
    st.header("View Data in MotherDuck")

    # Dropdown to select which table to view
    table_options_for_view = [
        "financial_statements",
        "securities_transactions",
        "portfolio_holdings",
        # Add more report table names here
    ]
    selected_table_to_view = st.selectbox(
        "Select a report table to view data from MotherDuck:",
        options=table_options_for_view,
        index=0,
        help="Choose which table's data to fetch and display."
    )

    if st.button(f"Fetch Data from '{selected_table_to_view}'"):
        fetched_df = fetch_data_from_motherduck(conn, selected_table_to_view)
        if not fetched_df.empty:
            st.dataframe(fetched_df)
            st.download_button(
                label=f"Download {selected_table_to_view} as CSV",
                data=fetched_df.to_csv(index=False).encode('utf-8'),
                file_name=f"{selected_table_to_view}_data.csv",
                mime="text/csv",
            )
        else:
            st.info(f"No data found in '{selected_table_to_view}' or an error occurred while fetching.")

else:
    st.info("Please ensure your MotherDuck API Token is correctly configured to get started.")

