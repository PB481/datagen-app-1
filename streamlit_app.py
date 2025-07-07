import streamlit as st
import pandas as pd
import duckdb
import random
from datetime import datetime, timedelta
import string

# --- Configuration ---
MOTHERDUCK_DB_NAME = "my_financial_data_db"

# Define the supported fund types and their simplified characteristics for data generation
FUND_TYPES = {
    "Traditional UCITS": {
        "description": "Funds regulated by UCITS directives, typically investing in liquid assets like equities and bonds.",
        "asset_classes": ["Equity", "Fixed Income", "Cash", "Money Market"],
        "transaction_frequency": {"min": 1, "max": 5}, # Days between transactions
        "avg_annual_return_pct": 0.08,
        "daily_volatility_pct": 0.015,
        "typical_expense_ratio_pct": 0.005,
        "isin_prefix": "LU",
        "target_aum_range": (500_000_000, 2_000_000_000) # USD
    },
    "Private Equity": {
        "description": "Funds investing in private companies, characterized by illiquid investments and long holding periods.",
        "asset_classes": ["Private Equity", "Cash"],
        "transaction_frequency": {"min": 30, "max": 180},
        "avg_annual_return_pct": 0.15,
        "daily_volatility_pct": 0.005,
        "typical_expense_ratio_pct": 0.02,
        "isin_prefix": "GG",
        "target_aum_range": (100_000_000, 1_000_000_000) # USD
    },
}

# --- Global Helper Functions for IDs and Codes ---
def generate_lei():
    """Generates a synthetic LEI (Legal Entity Identifier)."""
    return "LEI" + ''.join(random.choices(string.ascii_uppercase + string.digits, k=16)) + random.choice(string.digits) * 2

def generate_bic():
    """Generates a synthetic BIC (Bank Identifier Code)."""
    return ''.join(random.choices(string.ascii_uppercase, k=4)) + \
           ''.join(random.choices(string.ascii_uppercase + string.digits, k=2)) + \
           ''.join(random.choices(string.ascii_uppercase + string.digits, k=2)) + \
           ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))

def generate_isin(prefix="XS"):
    """Generates a synthetic ISIN."""
    return prefix + ''.join(random.choices(string.ascii_uppercase + string.digits, k=9)) + random.choice(string.digits)

def generate_security_id():
    return f"SEC{random.randint(100000, 999999)}"

# --- Master Data Generation & Lookup ---

# Pre-populate some static lists for more realistic lookups
SHAREHOLDER_TYPES = ["Individual", "Corporate", "Pension Fund", "Trust"]
DISTRIBUTOR_NAMES = [
    "Global Wealth Management", "Apex Financial Advisors", "Horizon Investments Ltd",
    "Prime Capital Partners", "Liberty Financial Group"
]
REPRESENTATIVE_NAMES = [
    "John Doe", "Jane Smith", "Michael Brown", "Emily White", "David Green"
]
CURRENCIES = ["USD", "EUR", "GBP", "CHF", "JPY", "CAD", "AUD"]
COUNTRIES = ["USA", "GBR", "DEU", "FRA", "LUX", "IRL", "CAN", "JPN", "AUS"] # ISO 3166-1 alpha-3

GICS_SECTORS = [
    "Information Technology", "Financials", "Health Care", "Industrials",
    "Consumer Discretionary", "Consumer Staples", "Communication Services",
    "Energy", "Utilities", "Materials", "Real Estate"
]
GICS_SUB_SECTORS = { # Simplified mapping
    "Information Technology": ["Software", "IT Services", "Semiconductors"],
    "Financials": ["Banks", "Capital Markets", "Insurance"],
    "Health Care": ["Pharmaceuticals", "Biotechnology", "Health Care Providers"],
    "Industrials": ["Aerospace & Defense", "Construction", "Machinery"],
    "Energy": ["Oil & Gas Exploration", "Oil & Gas Services"]
}
EXCHANGE_MICS = ["XNYS", "XLON", "XETR", "TSE", "NSE", "NASDAQ", "SSE"] # Common Market Identifier Codes

MASTER_CUSTODIANS = [
    {"custodian_id": "CUST001", "name": "Global Custody Solutions", "lei": generate_lei(), "country": "USA", "bic": generate_bic()},
    {"custodian_id": "CUST002", "name": "Euro Clearing Bank", "lei": generate_lei(), "country": "DEU", "bic": generate_bic()},
    {"custodian_id": "CUST003", "name": "Asia Pacific Custody", "lei": generate_lei(), "country": "SGP", "bic": generate_bic()},
]

FUND_ACCOUNTING_CHART_OF_ACCOUNTS = {
    "Assets": [
        ("1001", "Cash at Bank"), ("1002", "Investments - Equities"), ("1003", "Investments - Fixed Income"),
        ("1004", "Investments - Private Equity"), ("1005", "Receivables - Subscriptions"), ("1006", "Receivables - Dividends"),
        ("1007", "Accrued Income"), ("1008", "Prepaid Expenses")
    ],
    "Liabilities": [
        ("2001", "Payables - Redemptions"), ("2002", "Payables - Expenses"), ("2003", "Accrued Expenses"),
        ("2004", "Management Fees Payable"), ("2005", "Performance Fees Payable")
    ],
    "Equity": [
        ("3001", "Fund Capital"), ("3002", "Retained Earnings"), ("3003", "Net Unrealized Gain/Loss")
    ],
    "Income": [
        ("4001", "Dividend Income"), ("4002", "Interest Income"), ("4003", "Realized Gain/Loss on Investments"),
        ("4004", "Unrealized Gain/Loss on Investments")
    ],
    "Expenses": [
        ("5001", "Management Fees"), ("5002", "Administration Fees"), ("5003", "Custody Fees"),
        ("5004", "Audit Fees"), ("5005", "Legal Fees"), ("5006", "Operating Expenses")
    ]
}

# Cached generation of common master entities (Shareholders, Distributors, TA Accounts)
@st.cache_resource
def generate_master_entities(num_shareholders=100, num_distributors=10, num_representatives=50):
    entities = {
        "shareholders": [], "ta_accounts": [], "distributors": [], "representatives": [],
    }

    for i in range(num_distributors):
        dist_id = f"DIST{i+1:03d}"
        dist_name = random.choice(DISTRIBUTOR_NAMES) + f" {i+1}"
        entities["distributors"].append({
            "distributor_id": dist_id, "distributor_name": dist_name, "distributor_lei": generate_lei(),
            "distributor_country": random.choice(COUNTRIES)
        })

    for i in range(num_representatives):
        rep_id = f"REP{i+1:04d}"
        rep_name = random.choice(REPRESENTATIVE_NAMES) + f" {i+1}"
        associated_distributor = random.choice(entities["distributors"])
        entities["representatives"].append({
            "representative_id": rep_id, "representative_name": rep_name,
            "distributor_id": associated_distributor["distributor_id"]
        })

    for i in range(num_shareholders):
        shareholder_id = f"SH{i+1:05d}"
        shareholder_type = random.choice(SHAREHOLDER_TYPES)
        shareholder_name = f"Investor {shareholder_id}" if shareholder_type == "Individual" else f"{shareholder_type} Co. {i+1}"
        shareholder_country = random.choice(COUNTRIES)
        shareholder_lei = generate_lei() if shareholder_type != "Individual" and random.random() > 0.1 else None

        entities["shareholders"].append({
            "shareholder_id": shareholder_id, "shareholder_name": shareholder_name, "shareholder_type": shareholder_type,
            "shareholder_country": shareholder_country, "shareholder_lei": shareholder_lei
        })

        ta_account_id = f"TAACC{i+1:05d}"
        linked_distributor = random.choice(entities["distributors"])
        linked_representative = random.choice(entities["representatives"])
        entities["ta_accounts"].append({
            "ta_account_id": ta_account_id, "shareholder_id": shareholder_id, "distributor_id": linked_distributor["distributor_id"],
            "representative_id": linked_representative["representative_id"], "account_currency": random.choice(CURRENCIES),
            "opening_date": (datetime.now() - timedelta(days=random.randint(365, 365*5))).strftime("%Y-%m-%d")
        })
    return entities

# Cached generation of master funds and securities
@st.cache_resource
def generate_master_funds_and_securities(num_funds=5, num_securities_per_fund=50):
    master_funds = []
    master_securities = []

    fund_names = ["Global Equity Alpha", "Emerging Markets Growth", "Euro Fixed Income",
                  "Sustainable Solutions", "Private Assets Opportunities", "Money Market Prime"]
    legal_structures = ["UCITS", "AIF", "LP", "Unit Trust"]
    management_companies = {
        "Acme Asset Mgmt": generate_lei(), "Quantum Capital": generate_lei(),
        "Pinnacle Investments": generate_lei(), "Evergreen Funds": generate_lei()
    }

    for i in range(num_funds):
        fund_type_key = random.choice(list(FUND_TYPES.keys()))
        fund_config = FUND_TYPES[fund_type_key]
        fund_id = f"FUND{i+1:03d}"
        fund_name = random.choice(fund_names) + f" {fund_id}"
        fund_isin = generate_isin(fund_config["isin_prefix"])
        mgmt_co_name, mgmt_co_lei = random.choice(list(management_companies.items()))
        min_aum, max_aum = fund_config["target_aum_range"]

        fund_entry = {
            "fund_id": fund_id, "fund_name": fund_name, "fund_isin": fund_isin,
            "fund_type": fund_type_key, "legal_structure": random.choice(legal_structures),
            "inception_date": (datetime.now() - timedelta(days=random.randint(365*2, 365*10))).strftime("%Y-%m-%d"),
            "base_currency": random.choice(CURRENCIES),
            "management_company_name": mgmt_co_name, "management_company_lei": mgmt_co_lei,
            "expense_ratio_pct": fund_config["typical_expense_ratio_pct"],
            "target_aum_min": min_aum, "target_aum_max": max_aum # Add AUM range
        }
        master_funds.append(fund_entry)

        # Generate Securities for each fund (as if held by the fund)
        for j in range(num_securities_per_fund):
            asset_class = random.choice(fund_config["asset_classes"])
            sec_isin_prefix = random.choice(["US", "GB", "DE", "FR"])

            if asset_class == "Private Equity":
                sec_isin = f"PRV{generate_security_id()[-6:]}"
                sec_name = f"Private Co. {sec_isin[-4:]} {random.choice(['Series A', 'Growth', 'Venture'])}"
                exchange_mic = "N/A"
                instrument_type = "Private Equity"
                sector = "N/A"
                sub_sector = "N/A"
            elif asset_class == "Cash":
                sec_isin = "N/A"
                sec_name = f"Cash Balance {random.choice(CURRENCIES)}"
                exchange_mic = "N/A"
                instrument_type = "Cash"
                sector = "N/A"
                sub_sector = "N/A"
            else:
                sec_isin = generate_isin(sec_isin_prefix)
                instrument_type = random.choice(["Equity", "Bond", "ETF", "Mutual Fund Share"])
                if instrument_type == "Equity":
                    sec_name = f"{random.choice(string.ascii_uppercase * 3)} Corp. {sec_isin[-4:]}"
                    sector = random.choice(GICS_SECTORS)
                    sub_sector = random.choice(GICS_SUB_SECTORS.get(sector, ["Other"]))
                else:
                    sec_name = f"{instrument_type} {random.randint(1,10)}% {sec_isin[-4:]}"
                    sector = "Financials" if "Bond" in instrument_type else "Diversified"
                    sub_sector = "N/A"
                exchange_mic = random.choice(EXCHANGE_MICS)


            security_entry = {
                "security_id": generate_security_id(),
                "isin": sec_isin,
                "fund_id": fund_id, # Link security to a fund it might be held in
                "security_name": sec_name,
                "asset_class": asset_class,
                "currency": random.choice(CURRENCIES),
                "issuer": f"Issuer {random.randint(1,10)}",
                "issuer_lei": generate_lei(),
                "exchange_mic": exchange_mic,
                "instrument_type": instrument_type,
                "sector_gics": sector,
                "sub_sector_gics": sub_sector
            }
            master_securities.append(security_entry)

    return pd.DataFrame(master_funds), pd.DataFrame(master_securities)

# Global variables to hold generated master data
MASTER_ENTITIES = {}
MASTER_FUNDS_DF = pd.DataFrame()
MASTER_SECURITIES_DF = pd.DataFrame()

# Helper to get random entity/asset info (now using generated master data)
def get_random_ta_account_info():
    return random.choice(MASTER_ENTITIES["ta_accounts"])

def get_random_fund_info(fund_type_filter=None):
    if fund_type_filter:
        filtered_funds = MASTER_FUNDS_DF[MASTER_FUNDS_DF['fund_type'] == fund_type_filter]
        if not filtered_funds.empty:
            return filtered_funds.sample(1).iloc[0].to_dict()
    # Default to picking any fund if filter not provided or no funds match
    return MASTER_FUNDS_DF.sample(1).iloc[0].to_dict()

def get_random_security_info(fund_id=None):
    if fund_id:
        filtered_securities = MASTER_SECURITIES_DF[MASTER_SECURITIES_DF['fund_id'] == fund_id]
        if not filtered_securities.empty:
            return filtered_securities.sample(1).iloc[0].to_dict()
    return MASTER_SECURITIES_DF.sample(1).iloc[0].to_dict()

# --- Core Data Generation Functions (Existing & Modified) ---

def generate_financial_statements(num_records=10, fund_info=None):
    data = []
    start_date = datetime(2023, 1, 1)
    if fund_info is None:
        fund_info = get_random_fund_info()

    min_aum = fund_info.get("target_aum_min", 100_000_000)
    max_aum = fund_info.get("target_aum_max", 1_000_000_000)
    base_revenue = random.uniform(min_aum * 0.005, max_aum * 0.015) / 12 # Monthly revenue estimate scaled by AUM

    for i in range(num_records):
        current_date = start_date + timedelta(days=i * 30)

        revenue = round(base_revenue * random.uniform(0.8, 1.2), 2) # Fluctuating around base
        cogs = round(random.uniform(0.3 * revenue, 0.6 * revenue), 2)
        gross_profit = revenue - cogs
        operating_expenses = round(random.uniform(0.15 * revenue, 0.3 * revenue), 2)
        depreciation_expense = round(random.uniform(0.01 * revenue, 0.05 * revenue), 2) # New
        ebit = gross_profit - operating_expenses - depreciation_expense # Adjusted
        interest_expense = round(random.uniform(0, 0.05 * ebit) if ebit > 0 else 0, 2)
        taxes = round(random.uniform(0.15 * ebit, 0.25 * ebit) if ebit > 0 else 0, 2)
        net_income = ebit - interest_expense - taxes

        # Balance Sheet (scaled by AUM more consistently)
        base_assets = random.uniform(min_aum * 0.9, max_aum * 1.1)
        cash = round(base_assets * random.uniform(0.05, 0.15), 2)
        accounts_receivable = round(base_assets * random.uniform(0.01, 0.05), 2)
        inventory = round(base_assets * random.uniform(0.005, 0.02), 2)
        total_current_assets = cash + accounts_receivable + inventory
        property_plant_equipment = round(base_assets * random.uniform(0.1, 0.3), 2)
        total_assets = total_current_assets + property_plant_equipment

        accounts_payable = round(total_assets * random.uniform(0.01, 0.04), 2)
        short_term_debt = round(total_assets * random.uniform(0.005, 0.02), 2)
        total_current_liabilities = accounts_payable + short_term_debt
        long_term_debt = round(total_assets * random.uniform(0.05, 0.15), 2)
        total_liabilities = total_current_liabilities + long_term_debt

        equity = total_assets - total_liabilities

        data.append({
            "fund_id": fund_info["fund_id"], "report_date": current_date.strftime("%Y-%m-%d"),
            "revenue": revenue, "cost_of_goods_sold": cogs, "gross_profit": gross_profit,
            "operating_expenses": operating_expenses, "depreciation_expense": depreciation_expense, # New
            "ebit": ebit, "interest_expense": interest_expense, "taxes": taxes, "net_income": net_income,
            "cash": cash, "accounts_receivable": accounts_receivable, "inventory": inventory,
            "total_current_assets": total_current_assets, "property_plant_equipment": property_plant_equipment,
            "total_assets": total_assets, "accounts_payable": accounts_payable, "short_term_debt": short_term_debt,
            "total_current_liabilities": total_current_liabilities, "long_term_debt": long_term_debt,
            "total_liabilities": total_liabilities, "equity": equity,
        })
    return pd.DataFrame(data)

def generate_securities_transactions(num_transactions, fund_type_filter=None, fund_info=None):
    transactions = []
    fund_config = FUND_TYPES.get(fund_type_filter, {})
    min_days_between_tx = fund_config["transaction_frequency"]["min"]
    max_days_between_tx = fund_config["transaction_frequency"]["max"]

    current_date = datetime(2023, 1, 1)
    if fund_info is None:
        fund_info = get_random_fund_info(fund_type_filter)

    fund_isin = fund_info["fund_isin"]
    fund_share_name = f"{fund_info['fund_name']} Share Class"
    asset_class = "Fund Share"

    for i in range(num_transactions):
        current_date += timedelta(days=random.randint(min_days_between_tx, max_days_between_tx))

        transaction_type = random.choice(["SUBSCRIPTION", "REDEMPTION", "SWITCH_IN", "SWITCH_OUT", "DIVIDEND_REINVEST", "DIVIDEND_PAYOUT"])
        quantity = round(random.uniform(10.0, 10000.0), 4)
        price_per_share = round(random.uniform(10.0, 500.0), 2)
        gross_amount = round(quantity * price_per_share, 2)
        currency = fund_info["base_currency"]
        transaction_fee = round(gross_amount * random.uniform(0.0001, 0.001), 2)

        ta_account_info = get_random_ta_account_info()
        net_amount = gross_amount
        if transaction_type in ["SUBSCRIPTION", "SWITCH_IN", "DIVIDEND_REINVEST"]:
            net_amount = round(gross_amount - transaction_fee, 2)
        elif transaction_type in ["REDEMPTION", "SWITCH_OUT", "DIVIDEND_PAYOUT"]:
            net_amount = round(gross_amount + transaction_fee, 2)

        transactions.append({
            "transaction_id": f"TRN{i+1:05d}", "fund_id": fund_info["fund_id"], "fund_isin": fund_isin,
            "fund_type": fund_info["fund_type"], "transaction_date": current_date.strftime("%Y-%m-%d"),
            "isin": fund_isin, "security_name": fund_share_name, "asset_class": asset_class,
            "transaction_type": transaction_type, "quantity": quantity, "price_per_share": price_per_share,
            "gross_amount": gross_amount, "net_amount": net_amount, "currency": currency,
            "transaction_fee": transaction_fee, "ta_account_id": ta_account_info["ta_account_id"],
            "shareholder_id": ta_account_info["shareholder_id"], "distributor_id": ta_account_info["distributor_id"],
            "representative_id": ta_account_info["representative_id"],
            "settlement_date": (current_date + timedelta(days=random.randint(1,3))).strftime("%Y-%m-%d"),
            "payment_method": random.choice(["BANK_TRANSFER", "CHEQUE", "OTHER"])
        })
    return pd.DataFrame(transactions)

def generate_portfolio_data(num_securities_in_portfolio, fund_type_filter=None, fund_info=None):
    portfolio_holdings = []
    fund_config = FUND_TYPES.get(fund_type_filter, {})
    avg_annual_return_pct = fund_config.get("avg_annual_return_pct", 0.05)
    daily_volatility_pct = fund_config.get("daily_volatility_pct", 0.01)
    daily_return = (1 + avg_annual_return_pct)**(1/252) - 1

    valuation_date = datetime.now()
    if fund_info is None:
        fund_info = get_random_fund_info(fund_type_filter)

    fund_isin = fund_info["fund_isin"]
    fund_share_name = f"{fund_info['fund_name']} Share Class"

    num_accounts_to_generate = min(num_securities_in_portfolio, len(MASTER_ENTITIES["ta_accounts"]))
    unique_ta_accounts_in_scope = random.sample(MASTER_ENTITIES["ta_accounts"], num_accounts_to_generate)

    for ta_account_info in unique_ta_accounts_in_scope:
        quantity = round(random.uniform(500.0, 50000.0), 4)
        cost_basis_per_share = round(random.uniform(10.0, 400.0), 2)
        
        current_nav_per_share = round(cost_basis_per_share * (1 + daily_return + random.gauss(0, daily_volatility_pct)), 2)
        if current_nav_per_share < 0.01: current_nav_per_share = 0.01

        market_value = round(quantity * current_nav_per_share, 2)
        cost_basis_total = round(quantity * cost_basis_per_share, 2)
        unrealized_gain_loss = round(market_value - cost_basis_total, 2)
        currency = ta_account_info["account_currency"]

        portfolio_holdings.append({
            "portfolio_date": valuation_date.strftime("%Y-%m-%d"), "fund_id": fund_info["fund_id"],
            "fund_isin": fund_isin, "fund_type": fund_info["fund_type"], "isin": fund_isin,
            "security_name": fund_share_name, "asset_class": "Fund Share", "quantity": quantity,
            "cost_basis_per_share": cost_basis_per_share, "total_cost_basis": cost_basis_total,
            "current_nav_per_share": current_nav_per_share, "market_value": market_value,
            "unrealized_gain_loss": unrealized_gain_loss, "currency": currency,
            "ta_account_id": ta_account_info["ta_account_id"], "shareholder_id": ta_account_info["shareholder_id"],
            "distributor_id": ta_account_info["distributor_id"], "representative_id": ta_account_info["representative_id"],
        })
    return pd.DataFrame(portfolio_holdings)

def generate_cash_net_activity(num_records, fund_type_filter=None, fund_info=None):
    cash_activities = []
    start_date = datetime(2023, 1, 1)
    if fund_info is None:
        fund_info = get_random_fund_info(fund_type_filter)

    for i in range(num_records):
        activity_date = start_date + timedelta(days=random.randint(1, 365))
        ta_account_info = get_random_ta_account_info()
        currency = ta_account_info["account_currency"]

        activity_type = random.choice(["Subscription Cash In", "Redemption Cash Out", "Dividend Payment", "Fee Payment", "Other Adjustment"])
        amount = round(random.uniform(100, 50000) * (1 if "In" in activity_type else -1), 2)

        cash_activities.append({
            "activity_id": f"CASHACT{i+1:05d}", "fund_id": fund_info["fund_id"], "fund_type": fund_info["fund_type"],
            "activity_date": activity_date.strftime("%Y-%m-%d"), "ta_account_id": ta_account_info["ta_account_id"],
            "shareholder_id": ta_account_info["shareholder_id"], "distributor_id": ta_account_info["distributor_id"],
            "representative_id": ta_account_info["representative_id"], "activity_type": activity_type,
            "currency": currency, "amount": amount, "description": f"{activity_type} for account {ta_account_info['ta_account_id']}"
        })
    return pd.DataFrame(cash_activities)

def generate_mifid_transaction_report(num_mifid_transactions, fund_type_filter=None, fund_info=None):
    mifid_reports = []
    start_date = datetime(2023, 1, 1)
    if fund_info is None:
        fund_info = get_random_fund_info(fund_type_filter)

    securities_for_fund = MASTER_SECURITIES_DF[MASTER_SECURITIES_DF['fund_id'] == fund_info['fund_id']]
    if securities_for_fund.empty:
        st.warning(f"No underlying securities found for {fund_info['fund_name']}. MiFID report might be empty.")
        return pd.DataFrame()

    for i in range(num_mifid_transactions):
        transaction_date = start_date + timedelta(days=random.randint(1, 365))
        security_info = securities_for_fund.sample(1).iloc[0].to_dict()
        
        transaction_id = f"MIFIDTRN{i+1:05d}"
        exec_venue = security_info["exchange_mic"] if security_info["exchange_mic"] != "N/A" else random.choice(EXCHANGE_MICS)
        
        client_entity_type = random.choice(["FUND", "SHAREHOLDER", "OTHER_INST"])
        client_id_value = ""
        client_id_type = ""
        if client_entity_type == "FUND":
            client_id_type = "LEI"
            client_id_value = fund_info["management_company_lei"]
        else:
            client_shareholder = random.choice(MASTER_ENTITIES["shareholders"])
            if client_shareholder["shareholder_type"] != "Individual" and client_shareholder["shareholder_lei"]:
                client_id_type = "LEI"
                client_id_value = client_shareholder["shareholder_lei"]
            else:
                client_id_type = "NATID"
                client_id_value = f"NATID{random.randint(100000000, 999999999)}"

        investment_firm_lei = fund_info["management_company_lei"]
        
        buy_sell_indicator = random.choice(["BUY", "SELL"])
        quantity = round(random.uniform(10, 1000), 2)
        price = round(random.uniform(50, 1000), 2)
        currency = security_info["currency"]
        notional_amount = round(quantity * price, 2)
        
        liquid_market = random.choice(["LIQM", "NLIQ"])
        waiver_indicator = random.choice(["SIZE", "ILQD", "RFPT", "OTHR", "NO_WAIVER"])
        
        mifid_reports.append({
            "report_id": transaction_id, "fund_id": fund_info["fund_id"], "fund_isin": fund_info["fund_isin"],
            "transaction_date_time": transaction_date.strftime("%Y-%m-%dT%H:%M:%S"), "security_id": security_info["security_id"],
            "isin": security_info["isin"], "security_name": security_info["security_name"],
            "asset_class_mifid": security_info["asset_class"], "execution_venue": exec_venue,
            "investment_firm_lei": investment_firm_lei, "client_id_type": client_id_type,
            "client_id_value": client_id_value, "buy_sell_indicator": buy_sell_indicator,
            "quantity": quantity, "price": price, "currency": currency, "notional_amount": notional_amount,
            "liquid_market_indicator": liquid_market, "waiver_indicator": waiver_indicator,
            "trading_capacity": random.choice(["DEAL", "MTCH", "AOTC"]), "country_of_branch": random.choice(COUNTRIES)
        })
    return pd.DataFrame(mifid_reports)

# --- New Genie Datasets (Aladdin/FundInfo Inspired) ---

def generate_genie_trade_orders(num_orders, fund_type_filter=None, fund_info=None):
    orders = []
    start_date = datetime(2023, 1, 1)
    if fund_info is None:
        fund_info = get_random_fund_info(fund_type_filter)
    
    securities_for_fund = MASTER_SECURITIES_DF[MASTER_SECURITIES_DF['fund_id'] == fund_info['fund_id']]
    if securities_for_fund.empty:
        st.warning(f"No underlying securities found for {fund_info['fund_name']}. Trade orders might be empty.")
        return pd.DataFrame()

    trading_desks = ["Equity Trading", "Fixed Income Trading", "Derivatives Desk"]
    traders = ["Trader A", "Trader B", "Trader C", "Trader D"]

    for i in range(num_orders):
        order_date = start_date + timedelta(days=random.randint(1, 365))
        security_info = securities_for_fund.sample(1).iloc[0].to_dict()

        order_type = random.choice(["BUY", "SELL"])
        quantity = random.randint(100, 5000)
        limit_price = round(random.uniform(50, 1000), 2) if random.random() > 0.3 else None
        status = random.choice(["NEW", "OPEN", "PARTIALLY_FILLED", "CANCELLED", "FILLED"])

        orders.append({
            "order_id": f"ORD{i+1:06d}", "fund_id": fund_info["fund_id"], "fund_isin": fund_info["fund_isin"],
            "order_date_time": order_date.strftime("%Y-%m-%dT%H:%M:%S"), "security_id": security_info["security_id"],
            "isin": security_info["isin"], "security_name": security_info["security_name"],
            "order_type": order_type, "quantity": quantity, "limit_price": limit_price,
            "currency": security_info["currency"], "order_status": status,
            "trading_desk": random.choice(trading_desks), "trader_id": random.choice(traders)
        })
    return pd.DataFrame(orders)

def generate_genie_executed_trades(num_trades, fund_type_filter=None, fund_info=None):
    trades = []
    start_date = datetime(2023, 1, 1)
    if fund_info is None:
        fund_info = get_random_fund_info(fund_type_filter)

    securities_for_fund = MASTER_SECURITIES_DF[MASTER_SECURITIES_DF['fund_id'] == fund_info['fund_id']]
    if securities_for_fund.empty:
        st.warning(f"No underlying securities found for {fund_info['fund_name']}. Executed trades might be empty.")
        return pd.DataFrame()

    brokers = ["Brokerage A", "Brokerage B", "Brokerage C", "Brokerage D"]

    for i in range(num_trades):
        trade_date = start_date + timedelta(days=random.randint(1, 365))
        security_info = securities_for_fund.sample(1).iloc[0].to_dict()

        trade_type = random.choice(["BUY", "SELL"])
        quantity = random.randint(50, 2500)
        execution_price = round(random.uniform(50, 1000), 2)
        trade_amount = round(quantity * execution_price, 2)
        settlement_date = (trade_date + timedelta(days=random.randint(2,3))).strftime("%Y-%m-%d") # T+2 or T+3
        settlement_status = random.choice(["SETTLED", "PENDING", "FAILED"]) # More realistic status

        trades.append({
            "trade_id": f"TRD{i+1:06d}", "order_id": f"ORD{random.randint(1, num_trades+1000):06d}",
            "fund_id": fund_info["fund_id"], "fund_isin": fund_info["fund_isin"],
            "trade_date_time": trade_date.strftime("%Y-%m-%dT%H:%M:%S"), "security_id": security_info["security_id"],
            "isin": security_info["isin"], "security_name": security_info["security_name"],
            "trade_type": trade_type, "quantity": quantity, "execution_price": execution_price,
            "trade_amount": trade_amount, "currency": security_info["currency"],
            "broker_id": random.choice(brokers), "settlement_date": settlement_date,
            "settlement_status": settlement_status # Renamed from trade_status for clarity
        })
    return pd.DataFrame(trades)

def generate_genie_daily_security_prices(num_days, fund_type_filter=None):
    prices = []
    securities_to_price = MASTER_SECURITIES_DF.copy()
    if fund_type_filter:
        fund_ids_for_type = MASTER_FUNDS_DF[MASTER_FUNDS_DF['fund_type'] == fund_type_filter]['fund_id'].tolist()
        securities_to_price = MASTER_SECURITIES_DF[MASTER_SECURITIES_DF['fund_id'].isin(fund_ids_for_type)].copy()

    if securities_to_price.empty:
        st.warning(f"No securities to price for fund type {fund_type_filter}. Prices might be empty.")
        return pd.DataFrame()

    start_date = datetime(2023, 1, 1)

    # Initialize a 'price_series' for each security for simple trend simulation
    # Using a dictionary to hold current prices for update
    current_prices_for_simulation = {row['security_id']: random.uniform(10.0, 1000.0) for index, row in securities_to_price.iterrows()}

    for i in range(num_days):
        price_date = start_date + timedelta(days=i)
        for _, security_info in securities_to_price.iterrows():
            security_id = security_info['security_id']
            current_price = current_prices_for_simulation[security_id]

            # Simulate daily price movement with a slight upward bias (trend)
            # and volatility based on fund type (if linked)
            fund_id_for_sec = security_info['fund_id']
            fund_type_of_sec = MASTER_FUNDS_DF[MASTER_FUNDS_DF['fund_id'] == fund_id_for_sec]['fund_type'].iloc[0] if not MASTER_FUNDS_DF[MASTER_FUNDS_DF['fund_id'] == fund_id_for_sec].empty else "Traditional UCITS"
            fund_config_for_vol = FUND_TYPES.get(fund_type_of_sec, {"daily_volatility_pct": 0.01, "avg_annual_return_pct": 0.05})

            daily_vol = fund_config_for_vol.get("daily_volatility_pct", 0.01)
            avg_daily_return = (1 + fund_config_for_vol.get("avg_annual_return_pct", 0.05))**(1/252) - 1

            price_change_factor = 1 + avg_daily_return + random.gauss(0, daily_vol)
            new_price = round(current_price * price_change_factor, 2)
            if new_price < 0.01: new_price = 0.01

            prices.append({
                "price_date": price_date.strftime("%Y-%m-%d"),
                "security_id": security_id,
                "isin": security_info["isin"],
                "currency": security_info["currency"],
                "closing_price": new_price,
                # Add fund_id to daily security prices
                "fund_id": security_info["fund_id"], 
            })
            current_prices_for_simulation[security_id] = new_price # Update for next day

    return pd.DataFrame(prices)

def generate_genie_fund_characteristics():
    """Returns the pre-generated MASTER_FUNDS_DF as Fund Characteristics."""
    df = MASTER_FUNDS_DF.copy()
    df['is_active'] = [random.choice([True, False]) for _ in range(len(df))]
    df['aum_current_estimate'] = [round(random.uniform(row['target_aum_min'] * 0.8, row['target_aum_max'] * 1.2), 2) for index, row in df.iterrows()]
    df['is_open_ended'] = df['legal_structure'].apply(lambda x: x in ["UCITS", "AIF", "Unit Trust"])
    return df

def generate_genie_fund_daily_nav(num_days, fund_type_filter=None):
    nav_data = []
    funds_to_price = MASTER_FUNDS_DF.copy()
    if fund_type_filter:
        funds_to_price = MASTER_FUNDS_DF[MASTER_FUNDS_DF['fund_type'] == fund_type_filter].copy()

    if funds_to_price.empty:
        st.warning(f"No funds to generate NAV for fund type {fund_type_filter}. NAV data might be empty.")
        return pd.DataFrame()

    start_date = datetime(2023, 1, 1)

    initial_navs_per_share = {row['fund_id']: random.uniform(50.0, 150.0) for index, row in funds_to_price.iterrows()}
    initial_total_shares = {row['fund_id']: random.uniform(1_000_000, 10_000_000) for index, row in funds_to_price.iterrows()}

    for i in range(num_days):
        nav_date = start_date + timedelta(days=i)
        for _, fund_info in funds_to_price.iterrows():
            fund_id = fund_info['fund_id']
            current_nav_per_share = initial_navs_per_share[fund_id]
            current_total_shares = initial_total_shares[fund_id]

            fund_config = FUND_TYPES.get(fund_info['fund_type'], {})
            daily_vol = fund_config.get("daily_volatility_pct", 0.005)
            avg_daily_return = (1 + fund_config.get("avg_annual_return_pct", 0.05))**(1/252) - 1

            nav_change_factor = 1 + avg_daily_return + random.gauss(0, daily_vol)
            new_nav_per_share = round(current_nav_per_share * nav_change_factor, 4)
            if new_nav_per_share < 0.01: new_nav_per_share = 0.01

            share_change_pct = random.uniform(-0.001, 0.001)
            new_total_shares = round(current_total_shares * (1 + share_change_pct), 0)
            if new_total_shares < 10000: new_total_shares = 10000

            total_nav = round(new_nav_per_share * new_total_shares, 2)

            nav_data.append({
                "nav_date": nav_date.strftime("%Y-%m-%d"), "fund_id": fund_id, "fund_isin": fund_info["fund_isin"],
                "nav_per_share": new_nav_per_share, "total_nav": total_nav,
                "total_shares_outstanding": new_total_shares, "currency": fund_info["base_currency"],
            })
            initial_navs_per_share[fund_id] = new_nav_per_share
            initial_total_shares[fund_id] = new_total_shares

    return pd.DataFrame(nav_data)


def generate_genie_custody_holdings(num_holdings, fund_type_filter=None, fund_info=None):
    holdings_data = []
    valuation_date = datetime.now()

    if fund_info is None:
        fund_info = get_random_fund_info(fund_type_filter)

    # Pick a random custodian
    custodian = random.choice(MASTER_CUSTODIANS)

    securities_for_fund = MASTER_SECURITIES_DF[MASTER_SECURITIES_DF['fund_id'] == fund_info['fund_id']]
    if securities_for_fund.empty:
        st.warning(f"No underlying securities found for {fund_info['fund_name']}. Custody holdings might be empty.")
        return pd.DataFrame()

    num_securities_to_sample = min(num_holdings, len(securities_for_fund))
    sampled_securities = securities_for_fund.sample(num_securities_to_sample).to_dict('records')

    for sec_info in sampled_securities:
        quantity = random.randint(100, 10000)
        # Use a dummy current price for this simulation, in a real scenario, it would come from prices table
        current_price = random.uniform(50.0, 1000.0)
        market_value = round(quantity * current_price, 2)
        cost_basis = round(market_value * random.uniform(0.8, 1.2), 2)
        unrealized_gain_loss = round(market_value - cost_basis, 2)

        holdings_data.append({
            "snapshot_date": valuation_date.strftime("%Y-%m-%d"),
            "custodian_id": custodian["custodian_id"],
            "fund_id": fund_info["fund_id"],
            "fund_isin": fund_info["fund_isin"],
            "security_id": sec_info["security_id"],
            "isin": sec_info["isin"],
            "security_name": sec_info["security_name"],
            "asset_class": sec_info["asset_class"],
            "quantity": quantity,
            "market_value": market_value,
            "currency": sec_info["currency"],
            "cost_basis": cost_basis,
            "unrealized_gain_loss": unrealized_gain_loss,
            "safekeeping_location": random.choice(["DTC", "Euroclear", "Clearstream", "Local Sub-Custodian"])
        })
    return pd.DataFrame(holdings_data)

def generate_genie_fund_accounting_trial_balance(num_records, fund_type_filter=None, fund_info=None):
    trial_balance_data = []
    start_date = datetime(2023, 1, 1)

    if fund_info is None:
        fund_info = get_random_fund_info(fund_type_filter)

    all_accounts = []
    for category in FUND_ACCOUNTING_CHART_OF_ACCOUNTS.values():
        all_accounts.extend(category)

    for i in range(num_records):
        report_date = start_date + timedelta(days=i * 30) # Monthly snapshots

        # Basic attempt to make debits/credits somewhat balanced
        total_debits = 0
        total_credits = 0

        for account_code, account_name in all_accounts:
            is_debit = random.choice([True, False]) # Simplistic
            amount = round(random.uniform(100, 1_000_000), 2) # Varying balances

            debit_balance = amount if is_debit else 0
            credit_balance = amount if not is_debit else 0

            trial_balance_data.append({
                "report_date": report_date.strftime("%Y-%m-%d"),
                "fund_id": fund_info["fund_id"],
                "fund_isin": fund_info["fund_isin"],
                "account_code": account_code,
                "account_name": account_name,
                "debit_balance": debit_balance,
                "credit_balance": credit_balance,
                "currency": fund_info["base_currency"]
            })
            total_debits += debit_balance
            total_credits += credit_balance
            
        # Simple adjustment to try to balance, could be more sophisticated
        if total_debits != total_credits:
            diff = total_debits - total_credits
            if diff > 0: # Debits exceed credits
                # Add to a random credit account or equity
                target_account = random.choice([acc for acc in all_accounts if acc[0].startswith('3') or acc[0].startswith('2')]) # Equity or Liability
                for entry in trial_balance_data:
                    if entry['account_code'] == target_account[0] and entry['report_date'] == report_date.strftime("%Y-%m-%d"):
                        entry['credit_balance'] += abs(diff)
                        break
            else: # Credits exceed debits
                # Add to a random debit account or equity
                target_account = random.choice([acc for acc in all_accounts if acc[0].startswith('3') or acc[0].startswith('1')]) # Equity or Asset
                for entry in trial_balance_data:
                    if entry['account_code'] == target_account[0] and entry['report_date'] == report_date.strftime("%Y-%m-%d"):
                        entry['debit_balance'] += abs(diff)
                        break

    return pd.DataFrame(trial_balance_data)


def generate_genie_corporate_actions(num_cas, fund_type_filter=None):
    corporate_actions = []
    start_date = datetime(2023, 1, 1)

    # Get all securities to generate CAs for
    securities_for_ca = MASTER_SECURITIES_DF.copy()
    if fund_type_filter:
        fund_ids_for_type = MASTER_FUNDS_DF[MASTER_FUNDS_DF['fund_type'] == fund_type_filter]['fund_id'].tolist()
        securities_for_ca = MASTER_SECURITIES_DF[MASTER_SECURITIES_DF['fund_id'].isin(fund_ids_for_type)].copy()

    if securities_for_ca.empty:
        st.warning(f"No securities to generate corporate actions for fund type {fund_type_filter}.")
        return pd.DataFrame()

    ca_types = ["DIVIDEND", "STOCK_SPLIT", "MERGER"]

    for i in range(num_cas):
        sec_info = securities_for_ca.sample(1).iloc[0].to_dict()
        ca_type = random.choice(ca_types)
        
        announcement_date = start_date + timedelta(days=random.randint(1, 365))
        ex_date = announcement_date + timedelta(days=random.randint(10, 30))
        record_date = ex_date + timedelta(days=random.randint(1, 5))
        pay_date = record_date + timedelta(days=random.randint(5, 20))

        ratio_or_amount = None
        currency = None

        if ca_type == "DIVIDEND":
            ratio_or_amount = round(random.uniform(0.1, 5.0), 2) # Dividend per share
            currency = sec_info["currency"]
        elif ca_type == "STOCK_SPLIT":
            split_ratio_num = random.choice([2, 3])
            split_ratio_den = 1
            ratio_or_amount = f"{split_ratio_num} FOR {split_ratio_den}" # e.g., 2 FOR 1
        elif ca_type == "MERGER":
            ratio_or_amount = f"Acquirer: {random.choice(string.ascii_uppercase * 3)} Corp." # Simplified merger info

        corporate_actions.append({
            "ca_id": f"CA{i+1:05d}",
            "fund_id": sec_info["fund_id"], # Fund that holds the security affected
            "security_id": sec_info["security_id"],
            "isin": sec_info["isin"],
            "security_name": sec_info["security_name"],
            "ca_type": ca_type,
            "announcement_date": announcement_date.strftime("%Y-%m-%d"),
            "ex_date": ex_date.strftime("%Y-%m-%d"),
            "record_date": record_date.strftime("%Y-%m-%d"),
            "pay_date": pay_date.strftime("%Y-%m-%d"),
            "ratio_or_amount": ratio_or_amount,
            "currency": currency
        })
    return pd.DataFrame(corporate_actions)

def generate_genie_fx_rates(num_days):
    fx_rates = []
    start_date = datetime(2023, 1, 1)

    # Common base and quote currency pairs
    currency_pairs = [
        ("USD", "EUR"), ("USD", "GBP"), ("USD", "JPY"), ("EUR", "GBP"),
        ("EUR", "CHF"), ("GBP", "USD"), ("JPY", "USD"), ("CAD", "USD")
    ]

    # Initialize rates
    current_rates = {pair: random.uniform(0.7, 1.5) for pair in currency_pairs}

    for i in range(num_days):
        rate_date = start_date + timedelta(days=i)
        for base, quote in currency_pairs:
            # Simulate small daily fluctuation
            daily_change_factor = 1 + random.uniform(-0.005, 0.005)
            new_rate = round(current_rates[(base, quote)] * daily_change_factor, 4)
            if new_rate <= 0: new_rate = 0.0001 # Prevent zero or negative rates

            fx_rates.append({
                "rate_date": rate_date.strftime("%Y-%m-%d"),
                "base_currency": base,
                "quote_currency": quote,
                "exchange_rate": new_rate
            })
            current_rates[(base, quote)] = new_rate # Update for next day

    return pd.DataFrame(fx_rates)


# --- Centralized Report Generation Dispatcher ---
def generate_selected_reports(fund_type_filter, num_records_per_report, reports_to_generate_list):
    generated_reports_dfs = {}

    # Ensure master entities are generated once per session
    global MASTER_ENTITIES
    if not MASTER_ENTITIES:
        st.write("Generating master entities (Shareholders, TA Accounts, Distributors, Representatives)...")
        MASTER_ENTITIES = generate_master_entities()
        st.success("Master entities generated.")

    # Ensure master funds and securities are generated once per session
    global MASTER_FUNDS_DF, MASTER_SECURITIES_DF
    if MASTER_FUNDS_DF.empty or MASTER_SECURITIES_DF.empty:
        st.write("Generating master funds and securities...")
        MASTER_FUNDS_DF, MASTER_SECURITIES_DF = generate_master_funds_and_securities()
        st.success(f"Generated {len(MASTER_FUNDS_DF)} master funds and {len(MASTER_SECURITIES_DF)} master securities.")
        # Inject an initial price into securities for realistic price generation starting point
        # This is temporary and overwritten by generate_genie_daily_security_prices if selected
        MASTER_SECURITIES_DF['current_price'] = MASTER_SECURITIES_DF['security_id'].apply(lambda x: random.uniform(10.0, 1000.0))


    # Determine which fund to generate for (if a specific fund type is selected)
    selected_fund_info = get_random_fund_info(fund_type_filter)
    st.info(f"Generating data for Fund: **{selected_fund_info['fund_name']} (ID: {selected_fund_info['fund_id']})**")


    if "Financial Statements" in reports_to_generate_list:
        generated_reports_dfs["financial_statements"] = generate_financial_statements(num_records_per_report, fund_info=selected_fund_info)

    # TA specific reports
    if "TA Securities Transactions" in reports_to_generate_list:
        generated_reports_dfs["ta_securities_transactions"] = generate_securities_transactions(num_records_per_report * 5, fund_type_filter, fund_info=selected_fund_info)
    if "TA Portfolio Holdings" in reports_to_generate_list:
        generated_reports_dfs["ta_portfolio_holdings"] = generate_portfolio_data(num_records_per_report, fund_type_filter, fund_info=selected_fund_info)
    if "TA Cash Net Activity" in reports_to_generate_list:
        generated_reports_dfs["ta_cash_net_activity"] = generate_cash_net_activity(num_records_per_report * 3, fund_type_filter, fund_info=selected_fund_info)

    # MiFID II reports
    if "MiFID II Transaction Report" in reports_to_generate_list:
        generated_reports_dfs["mifid_transaction_report"] = generate_mifid_transaction_report(num_records_per_report * 10, fund_type_filter, fund_info=selected_fund_info)

    # Genie - Aladdin Inspired Datasets (Front/Middle Office)
    if "Genie - Trade Orders" in reports_to_generate_list:
        generated_reports_dfs["genie_trade_orders"] = generate_genie_trade_orders(num_records_per_report * 8, fund_type_filter, fund_info=selected_fund_info)
    if "Genie - Executed Trades" in reports_to_generate_list:
        generated_reports_dfs["genie_executed_trades"] = generate_genie_executed_trades(num_records_per_report * 7, fund_type_filter, fund_info=selected_fund_info)
    if "Genie - Daily Security Prices" in reports_to_generate_list:
        generated_reports_dfs["genie_daily_security_prices"] = generate_genie_daily_security_prices(num_records_per_report * 5, fund_type_filter)
    if "Genie - Custody Holdings" in reports_to_generate_list:
        generated_reports_dfs["genie_custody_holdings"] = generate_genie_custody_holdings(num_records_per_report * 2, fund_type_filter, fund_info=selected_fund_info)

    # Genie - FundInfo & Accounting Inspired Datasets (Middle/Back Office)
    if "Genie - Fund Characteristics" in reports_to_generate_list:
        generated_reports_dfs["genie_fund_characteristics"] = generate_genie_fund_characteristics() # No fund_info filter needed, generates for all master funds
    if "Genie - Fund Daily NAV" in reports_to_generate_list:
        generated_reports_dfs["genie_fund_daily_nav"] = generate_genie_fund_daily_nav(num_records_per_report * 5, fund_type_filter)
    if "Genie - Fund Accounting Trial Balance" in reports_to_generate_list:
        generated_reports_dfs["genie_fund_accounting_trial_balance"] = generate_genie_fund_accounting_trial_balance(num_records_per_report, fund_type_filter, fund_info=selected_fund_info)
    if "Genie - Corporate Actions" in reports_to_generate_list:
        generated_reports_dfs["genie_corporate_actions"] = generate_genie_corporate_actions(num_records_per_report, fund_type_filter)
    if "Genie - FX Rates" in reports_to_generate_list:
        generated_reports_dfs["genie_fx_rates"] = generate_genie_fx_rates(num_records_per_report * 5)


    return generated_reports_dfs

# --- Functions for MotherDuck Interaction (No Change Needed Here) ---
@st.cache_resource
def get_motherduck_connection(token):
    if not token:
        st.error("MotherDuck API Token is required to connect.")
        return None
    try:
        conn = duckdb.connect(f"md:?motherduck_token={token}", read_only=False)
        st.success("Successfully connected to MotherDuck default environment.")
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DB_NAME}")
        conn.execute(f"USE {MOTHERDUCK_DB_NAME}")
        st.success(f"Ensured MotherDuck database '{MOTHERDUCK_DB_NAME}' exists and is in use.")
        return conn
    except Exception as e:
        st.error(f"Error connecting to MotherDuck: {e}. Please check your token and network.")
        return None

def create_table_if_not_exists(conn, table_name, df):
    try:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
        st.success(f"Table '{table_name}' ensured to exist in MotherDuck with appropriate schema.")
        return True
    except Exception as e:
        st.error(f"Error creating table '{table_name}': {e}")
        return False

def insert_data_into_motherduck(conn, table_name, df, truncate_before_insert=False):
    try:
        if truncate_before_insert:
            conn.execute(f"DELETE FROM {table_name}")
        st.warning(f"Existing data in table '{table_name}' has been cleared.")
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        st.success(f"Successfully inserted {len(df)} records into '{table_name}'.")
        return True
    except Exception as e:
        st.error(f"Error inserting data into '{table_name}': {e}")
        return False

def fetch_data_from_motherduck(conn, table_name):
    try:
        df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        st.success(f"Successfully fetched {len(df)} records from '{table_name}'.")
        return df
    except Exception as e:
        st.error(f"Error fetching data from '{table_name}': {e}")
        return pd.DataFrame()

# --- Streamlit UI ---
st.set_page_config(layout="wide", page_title="Financial Data Generator for MotherDuck")

st.title("💰 Financial Data Generator & MotherDuck Uploader (Advanced Financial Data)")
st.markdown("""
This app generates highly realistic synthetic financial data across **Front, Middle, and Back Office** functions,
incorporating **ISO 20022 Transfer Agency (TA)**, **MiFID II**, and new **"Genie" datasets**
inspired by institutional platforms like Aladdin and public fund information sites like FundInfo.
All generated data can be uploaded to your MotherDuck database.
""")

st.sidebar.header("MotherDuck Configuration")
motherduck_token = None
if "MOTHERDUCK_TOKEN" in st.secrets:
    motherduck_token = st.secrets["MOTHERDUCK_TOKEN"]
    st.sidebar.success("MotherDuck API Token loaded from secrets.toml")
else:
    motherduck_token = st.sidebar.text_input(
        "Enter your MotherDuck API Token:", type="password",
        help="Paste your token here. For production, add it to `.streamlit/secrets.toml`."
    )
    if not motherduck_token:
        st.sidebar.warning("Please enter your MotherDuck API Token or configure `secrets.toml`.")

conn = None
if motherduck_token:
    conn = get_motherduck_connection(motherduck_token)

if conn:
    st.header("Generate and Upload Data")

    selected_fund_type = st.selectbox(
        "Select Fund Type (for filtering data generation):",
        options=list(FUND_TYPES.keys()),
        index=0,
        help="Choose the type of fund for which to generate data. This filters the *underlying fund* chosen for specific reports."
    )
    st.info(FUND_TYPES[selected_fund_type]["description"])

    available_reports = [
        "Financial Statements",
        "TA Securities Transactions",
        "TA Portfolio Holdings",
        "TA Cash Net Activity",
        "MiFID II Transaction Report",
        "Genie - Trade Orders",
        "Genie - Executed Trades",
        "Genie - Daily Security Prices",
        "Genie - Fund Characteristics",
        "Genie - Fund Daily NAV",
        "Genie - Custody Holdings",
        "Genie - Fund Accounting Trial Balance",
        "Genie - Corporate Actions",
        "Genie - FX Rates"
    ]
    reports_to_generate_selected = st.multiselect(
        "Select Reports to Generate:",
        options=available_reports,
        default=[r for r in available_reports], # Select all by default
        help="Choose which financial reports to generate data for."
    )

    num_records_per_report = st.slider(
        "Base number of records per report (e.g., approx. months/items):", min_value=1, max_value=120, value=12, step=1
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
                first_report_name = list(generated_data_dfs.keys())[0]
                if not generated_data_dfs[first_report_name].empty:
                    st.dataframe(generated_data_dfs[first_report_name].head())
                    st.info(f"Showing first 5 rows of '{first_report_name}' table preview.")
                else:
                    st.info(f"'{first_report_name}' generated no data based on current selections or underlying master data.")

                st.subheader("Uploading to MotherDuck...")
                for report_table_name, df_to_upload in generated_data_dfs.items():
                    if not df_to_upload.empty:
                        st.write(f"Processing table: `{report_table_name}` ({len(df_to_upload)} records)")
                        if create_table_if_not_exists(conn, report_table_name, df_to_upload):
                            insert_data_into_motherduck(
                                conn, report_table_name, df_to_upload, truncate_all_selected_tables
                            )
                        else:
                            st.error(f"Failed to create/ensure table '{report_table_name}'. Skipping data insertion.")
                    else:
                        st.warning(f"Skipping upload for '{report_table_name}' as no data was generated.")
                st.success("All selected report data generation and upload processes completed!")
            else:
                st.warning("No data was generated. Please check your selections.")

    st.markdown("---")
    st.header("View Data in MotherDuck")

    table_options_for_view = [
        "financial_statements", "ta_securities_transactions", "ta_portfolio_holdings",
        "ta_cash_net_activity", "mifid_transaction_report", "genie_trade_orders",
        "genie_executed_trades", "genie_daily_security_prices", "genie_fund_characteristics",
        "genie_fund_daily_nav", "genie_custody_holdings", "genie_fund_accounting_trial_balance",
        "genie_corporate_actions", "genie_fx_rates"
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
