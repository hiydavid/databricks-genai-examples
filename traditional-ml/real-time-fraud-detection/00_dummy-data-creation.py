# Databricks notebook source
# MAGIC %md
# MAGIC # Dummy Data Creation

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# COMMAND ----------

# DBTITLE 1,Generate Credit Card Customer Metadata
# Number of customers
n_customers = 1000

# Generate customer IDs
customer_ids = [f"CUST{str(i).zfill(8)}" for i in range(1, n_customers + 1)]

# Generate fake customer metadata
data = {
    "customer_id": customer_ids,
    "age": np.random.randint(18, 80, n_customers),
    "gender": np.random.choice(["M", "F", "Other"], n_customers, p=[0.48, 0.50, 0.02]),
    "annual_income": np.random.lognormal(10.8, 0.6, n_customers).astype(int),
    "credit_score": np.random.normal(680, 80, n_customers).clip(300, 850).astype(int),
    "account_age_months": np.random.randint(1, 240, n_customers),
    "num_credit_cards": np.random.choice(
        [1, 2, 3, 4, 5, 6], n_customers, p=[0.3, 0.25, 0.2, 0.15, 0.07, 0.03]
    ),
    "credit_limit": np.random.choice(
        [2000, 5000, 10000, 15000, 25000, 50000],
        n_customers,
        p=[0.2, 0.3, 0.25, 0.15, 0.07, 0.03],
    ),
    "avg_monthly_spend": np.random.lognormal(7.5, 0.8, n_customers).astype(int),
    "avg_monthly_balance": np.random.lognormal(7.8, 1.0, n_customers).astype(int),
    "payment_behavior": np.random.choice(
        ["Always On Time", "Occasionally Late", "Frequently Late", "Default"],
        n_customers,
        p=[0.70, 0.20, 0.08, 0.02],
    ),
    "utilization_rate": np.random.beta(2, 5, n_customers).clip(0, 1),
    "num_transactions_last_month": np.random.poisson(25, n_customers),
    "has_mortgage": np.random.choice([True, False], n_customers, p=[0.35, 0.65]),
    "has_auto_loan": np.random.choice([True, False], n_customers, p=[0.40, 0.60]),
    "employment_status": np.random.choice(
        ["Employed", "Self-Employed", "Unemployed", "Retired"],
        n_customers,
        p=[0.65, 0.15, 0.05, 0.15],
    ),
    "education_level": np.random.choice(
        ["High School", "Bachelor", "Master", "PhD"],
        n_customers,
        p=[0.30, 0.45, 0.20, 0.05],
    ),
    "marital_status": np.random.choice(
        ["Single", "Married", "Divorced", "Widowed"],
        n_customers,
        p=[0.35, 0.45, 0.15, 0.05],
    ),
    "num_dependents": np.random.choice(
        [0, 1, 2, 3, 4], n_customers, p=[0.30, 0.25, 0.25, 0.15, 0.05]
    ),
    "state": np.random.choice(
        ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"], n_customers
    ),
    "account_status": np.random.choice(
        ["Active", "Inactive", "Closed"], n_customers, p=[0.85, 0.10, 0.05]
    ),
}

# Create DataFrame
df_customers = pd.DataFrame(data)

# Round utilization rate to 2 decimals
df_customers["utilization_rate"] = df_customers["utilization_rate"].round(2)

# Add timestamp
df_customers["created_timestamp"] = datetime.now()

print(f"Generated {len(df_customers)} credit card customer records")
print(f"\nDataset shape: {df_customers.shape}")
print(f"\nColumn names: {list(df_customers.columns)}")

display(df_customers.head(10))

# Convert pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(df_customers)

# Define table name
table_name = "main.davidhuang.jpmcpoc_customers"

# Write to Delta table
spark_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Successfully wrote {spark_df.count()} records to Delta table: {table_name}")

# COMMAND ----------

# DBTITLE 1,Generate Credit Card Merchant Dataset
# Number of merchants
n_merchants = 100

# Merchant categories
categories = [
    'Grocery', 'Restaurant', 'Gas Station', 'Department Store', 'Online Retail',
    'Electronics', 'Pharmacy', 'Hotel', 'Airline', 'Entertainment',
    'Clothing', 'Home Improvement', 'Automotive', 'Healthcare', 'Utilities'
]

# Merchant name prefixes by category
name_prefixes = {
    'Grocery': ['Fresh Market', 'Super Save', 'Green Grocer', 'Food Mart', 'Quick Shop'],
    'Restaurant': ['The Bistro', 'Cafe', 'Grill House', 'Pizza Palace', 'Sushi Bar'],
    'Gas Station': ['QuickFuel', 'Gas & Go', 'Fuel Stop', 'Express Gas', 'Station'],
    'Department Store': ['Mega Store', 'Value Shop', 'Department Plus', 'Big Box', 'Retail Hub'],
    'Online Retail': ['E-Shop', 'Web Store', 'Online Market', 'Digital Retail', 'Click & Buy'],
    'Electronics': ['Tech World', 'Gadget Store', 'Electronics Plus', 'Digital Hub', 'Tech Mart'],
    'Pharmacy': ['Health Pharmacy', 'Care Plus', 'Wellness Drug', 'Med Store', 'Rx Pharmacy'],
    'Hotel': ['Grand Hotel', 'Inn & Suites', 'Comfort Stay', 'Resort', 'Lodge'],
    'Airline': ['Sky Airlines', 'Air Travel', 'Flight Express', 'Wings Air', 'Jet Airways'],
    'Entertainment': ['Cinema', 'Theater', 'Fun Zone', 'Entertainment Hub', 'Play Center'],
    'Clothing': ['Fashion Store', 'Apparel Shop', 'Style Boutique', 'Clothing Co', 'Wardrobe'],
    'Home Improvement': ['Home Depot', 'Builder Supply', 'Hardware Store', 'DIY Center', 'Fix It'],
    'Automotive': ['Auto Parts', 'Car Service', 'Vehicle Care', 'Auto Shop', 'Garage'],
    'Healthcare': ['Medical Center', 'Health Clinic', 'Care Facility', 'Wellness Center', 'Doctor Office'],
    'Utilities': ['Power Company', 'Water Service', 'Utility Provider', 'Energy Corp', 'Service Co']
}

# Generate merchant data
merchant_ids = [f"MERCH{str(i).zfill(6)}" for i in range(1, n_merchants + 1)]
selected_categories = np.random.choice(categories, n_merchants)

# Generate merchant names based on category
merchant_names = []
for cat in selected_categories:
    prefix = random.choice(name_prefixes[cat])
    suffix = random.choice(['', ' Downtown', ' Plaza', ' Center', ' Express', f' #{random.randint(1, 999)}'])
    merchant_names.append(f"{prefix}{suffix}")

data = {
    'merchant_id': merchant_ids,
    'merchant_name': merchant_names,
    'category': selected_categories,
    'mcc_code': np.random.choice([5411, 5812, 5541, 5311, 5999, 5732, 5912, 7011, 4511, 7832, 5651, 5211, 5533, 8011, 4900], n_merchants),
    'state': np.random.choice(['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI'], n_merchants),
    'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'], n_merchants),
    'avg_transaction_amount': np.random.lognormal(3.5, 0.8, n_merchants).round(2),
    'monthly_transaction_volume': np.random.lognormal(7.0, 1.2, n_merchants).astype(int),
    'merchant_rating': np.random.choice([3.0, 3.5, 4.0, 4.5, 5.0], n_merchants, p=[0.05, 0.15, 0.30, 0.35, 0.15]),
    'years_in_business': np.random.randint(1, 50, n_merchants),
    'accepts_online': np.random.choice([True, False], n_merchants, p=[0.60, 0.40]),
    'has_loyalty_program': np.random.choice([True, False], n_merchants, p=[0.40, 0.60]),
    'fraud_risk_score': np.random.uniform(0.01, 0.99, n_merchants).round(2),
    'chargeback_rate': np.random.uniform(0.001, 0.05, n_merchants).round(4),
    'merchant_status': np.random.choice(['Active', 'Inactive', 'Suspended'], n_merchants, p=[0.90, 0.08, 0.02]),
    'created_timestamp': datetime.now()
}

# Create DataFrame
df_merchants = pd.DataFrame(data)

print(f"Generated {len(df_merchants)} merchant records")
print(f"\nDataset shape: {df_merchants.shape}")
print(f"\nColumn names: {list(df_merchants.columns)}")
print(f"\nCategories distribution:")
print(df_merchants['category'].value_counts())

display(df_merchants.head(10))

# Convert pandas DataFrame to Spark DataFrame
spark_df_merchants = spark.createDataFrame(df_merchants)

# Define table name
table_name = "main.davidhuang.jpmcpoc_merchants"

# Write to Delta table
spark_df_merchants.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"\nSuccessfully wrote {spark_df_merchants.count()} records to Delta table: {table_name}")

# COMMAND ----------

# DBTITLE 1,Generate Risk Location Dataset
# Number of risk locations
n_locations = 50

# Generate location IDs
location_ids = [f"LOC{str(i).zfill(4)}" for i in range(1, n_locations + 1)]

# Major US cities with approximate coordinates
cities_coords = [
    ('New York, NY', 40.7128, -74.0060),
    ('Los Angeles, CA', 34.0522, -118.2437),
    ('Chicago, IL', 41.8781, -87.6298),
    ('Houston, TX', 29.7604, -95.3698),
    ('Phoenix, AZ', 33.4484, -112.0740),
    ('Philadelphia, PA', 39.9526, -75.1652),
    ('San Antonio, TX', 29.4241, -98.4936),
    ('San Diego, CA', 32.7157, -117.1611),
    ('Dallas, TX', 32.7767, -96.7970),
    ('San Jose, CA', 37.3382, -121.8863),
    ('Miami, FL', 25.7617, -80.1918),
    ('Atlanta, GA', 33.7490, -84.3880),
    ('Boston, MA', 42.3601, -71.0589),
    ('Seattle, WA', 47.6062, -122.3321),
    ('Denver, CO', 39.7392, -104.9903),
    ('Las Vegas, NV', 36.1699, -115.1398),
    ('Detroit, MI', 42.3314, -83.0458),
    ('Portland, OR', 45.5152, -122.6784),
    ('Minneapolis, MN', 44.9778, -93.2650),
    ('Orlando, FL', 28.5383, -81.3792)
]

# Generate location data
locations = []
for i in range(n_locations):
    # Select a base city and add some random variation to coordinates
    base_city = random.choice(cities_coords)
    city_name = base_city[0]
    lat = base_city[1] + np.random.uniform(-0.5, 0.5)
    lon = base_city[2] + np.random.uniform(-0.5, 0.5)
    
    locations.append({
        'location_id': location_ids[i],
        'location_name': f"{city_name.split(',')[0]} - Zone {random.randint(1, 10)}",
        'city': city_name.split(',')[0],
        'state': city_name.split(',')[1].strip(),
        'latitude': round(lat, 6),
        'longitude': round(lon, 6),
        'risk_level': np.random.choice(['Low', 'Medium', 'High', 'Critical'], p=[0.30, 0.40, 0.25, 0.05]),
        'fraud_incidents_last_month': np.random.poisson(15),
        'avg_fraud_amount': round(np.random.lognormal(6.5, 0.8), 2),
        'population_density': np.random.choice(['Low', 'Medium', 'High', 'Very High'], p=[0.15, 0.30, 0.35, 0.20]),
        'crime_rate_index': round(np.random.uniform(20, 95), 2),
        'is_high_traffic_area': np.random.choice([True, False], p=[0.60, 0.40]),
        'last_incident_date': (datetime.now() - pd.Timedelta(days=np.random.randint(1, 90))),
        'created_timestamp': datetime.now()
    })

# Create DataFrame
df_risk_locations = pd.DataFrame(locations)

print(f"Generated {len(df_risk_locations)} risk location records")
print(f"\nDataset shape: {df_risk_locations.shape}")
print(f"\nColumn names: {list(df_risk_locations.columns)}")
print(f"\nRisk level distribution:")
print(df_risk_locations['risk_level'].value_counts())

display(df_risk_locations.head(10))

# Convert pandas DataFrame to Spark DataFrame
spark_df_locations = spark.createDataFrame(df_risk_locations)

# Define table name
table_name = "main.davidhuang.jpmcpoc_locations"

# Write to Delta table
spark_df_locations.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"\nSuccessfully wrote {spark_df_locations.count()} records to Delta table: {table_name}")

# COMMAND ----------

# DBTITLE 1,Generate Historical Transaction Data
# Number of transactions to generate
n_transactions = 50000

# Generate transaction IDs
transaction_ids = [f"TXN{str(i).zfill(10)}" for i in range(1, n_transactions + 1)]

# Randomly select customer and merchant IDs from existing data
selected_customer_ids = np.random.choice(df_customers['customer_id'].values, n_transactions)
selected_merchant_ids = np.random.choice(df_merchants['merchant_id'].values, n_transactions)

# Generate transaction dates (last 2 years of data)
start_date = datetime.now() - timedelta(days=730)
end_date = datetime.now()
date_range = (end_date - start_date).days
transaction_dates = [start_date + timedelta(days=np.random.randint(0, date_range)) for _ in range(n_transactions)]

# Generate transaction amounts (log-normal distribution)
transaction_amounts = np.random.lognormal(3.8, 1.0, n_transactions).round(2)

# Generate transaction data
transaction_data = {
    'transaction_id': transaction_ids,
    'customer_id': selected_customer_ids,
    'merchant_id': selected_merchant_ids,
    'transaction_date': transaction_dates,
    'transaction_amount': transaction_amounts,
    'transaction_status': np.random.choice(['Approved', 'Declined', 'Pending', 'Refunded'], 
                                          n_transactions, p=[0.85, 0.10, 0.03, 0.02]),
    'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'Digital Wallet'], 
                                      n_transactions, p=[0.60, 0.30, 0.10]),
    'is_online': np.random.choice([True, False], n_transactions, p=[0.45, 0.55]),
    'is_international': np.random.choice([True, False], n_transactions, p=[0.08, 0.92]),
    'card_present': np.random.choice([True, False], n_transactions, p=[0.55, 0.45]),
    'transaction_hour': np.random.randint(0, 24, n_transactions),
    'day_of_week': [date.strftime('%A') for date in transaction_dates],
    'is_weekend': [date.weekday() >= 5 for date in transaction_dates],
    'distance_from_home_km': np.random.exponential(20, n_transactions).round(2),
    'time_since_last_transaction_hours': np.random.exponential(24, n_transactions).round(2),
    'is_recurring': np.random.choice([True, False], n_transactions, p=[0.15, 0.85]),
    'fraud_flag': np.random.choice([True, False], n_transactions, p=[0.02, 0.98]),
    'created_timestamp': datetime.now()
}

# Create DataFrame
df_transactions = pd.DataFrame(transaction_data)

# Sort by transaction date
df_transactions = df_transactions.sort_values('transaction_date').reset_index(drop=True)

print(f"Generated {len(df_transactions)} transaction records")
print(f"\nDataset shape: {df_transactions.shape}")
print(f"\nColumn names: {list(df_transactions.columns)}")
print(f"\nDate range: {df_transactions['transaction_date'].min()} to {df_transactions['transaction_date'].max()}")
print(f"\nTransaction status distribution:")
print(df_transactions['transaction_status'].value_counts())
print(f"\nFraud flag distribution:")
print(df_transactions['fraud_flag'].value_counts())

display(df_transactions.head(10))

# Convert pandas DataFrame to Spark DataFrame
spark_df_transactions = spark.createDataFrame(df_transactions)

# Define table name
table_name = "main.davidhuang.jpmcpoc_transactions"

# Write to Delta table
spark_df_transactions.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"\nSuccessfully wrote {spark_df_transactions.count()} records to Delta table: {table_name}")

# COMMAND ----------

