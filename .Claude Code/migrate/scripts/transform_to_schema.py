"""
Transform AdventureWorks parquet files to the expected star schema.
Creates dimension and fact tables from the source data.
"""

import pandas as pd
from pathlib import Path

# Find the latest data directory
source_base = Path("/workspaces/Claude-Code-Pro/.Claude Code/migrate/data")
timestamped_dirs = [d for d in source_base.iterdir() if d.is_dir() and d.name[0].isdigit()]

if not timestamped_dirs:
    raise FileNotFoundError("No timestamped data directories found")

latest_dir = max(timestamped_dirs, key=lambda x: x.name)

output_dir = source_base  # Save to the main data directory

print(f"Loading data from: {latest_dir}")
print(f"Output to: {output_dir}")

# Load source files
calendar = pd.read_parquet(latest_dir / "AdventureWorks_Calendar.parquet")
customers = pd.read_parquet(latest_dir / "AdventureWorks_Customers.parquet")
products = pd.read_parquet(latest_dir / "AdventureWorks_Products.parquet")
sales_2015 = pd.read_parquet(latest_dir / "AdventureWorks_Sales_2015.parquet")
sales_2017 = pd.read_parquet(latest_dir / "AdventureWorks_Sales_2017.parquet")
returns = pd.read_parquet(latest_dir / "AdventureWorks_Returns.parquet")

print("\nSource schema:")
print(f"  Calendar: {calendar.columns.tolist()}")
print(f"  Customers: {customers.columns.tolist()}")
print(f"  Products: {products.columns.tolist()}")
print(f"  Returns: {returns.columns.tolist()}")

# Combine sales data
sales = pd.concat([sales_2015, sales_2017], ignore_index=True)
print(f"  Sales (combined): {sales.columns.tolist()}")

# Create dimension tables
print("\nCreating dimension tables...")

# dim_date
dim_date = calendar.reset_index(drop=True)
dim_date['date_sk'] = range(1, len(dim_date) + 1)
dim_date = dim_date.rename(columns={dim_date.columns[0]: 'date'})
dim_date.to_parquet(output_dir / "dim_date.parquet", index=False)
print(f"  ✓ dim_date.parquet ({len(dim_date)} rows)")

# dim_customer
dim_customer = customers.reset_index(drop=True)
dim_customer['customer_sk'] = range(1, len(dim_customer) + 1)
dim_customer.to_parquet(output_dir / "dim_customer.parquet", index=False)
print(f"  ✓ dim_customer.parquet ({len(dim_customer)} rows)")

# dim_product
dim_product = products.reset_index(drop=True)
dim_product['product_sk'] = range(1, len(dim_product) + 1)
dim_product.to_parquet(output_dir / "dim_product.parquet", index=False)
print(f"  ✓ dim_product.parquet ({len(dim_product)} rows)")

# dim_store - create synthetic store dimension based on TerritoryKey
territories = sales['TerritoryKey'].unique()
dim_store = pd.DataFrame({'TerritoryKey': sorted(territories)})
dim_store['store_sk'] = range(1, len(dim_store) + 1)
dim_store['store_id'] = dim_store['TerritoryKey']
store_names = {
    1: "Northwest", 2: "Southwest", 3: "Northeast", 4: "Southeast", 
    5: "Central", 6: "Canada", 7: "France", 8: "Germany", 9: "Australia", 10: "United Kingdom"
}
dim_store['store_name'] = dim_store['store_id'].map(store_names).fillna("Territory " + dim_store['store_id'].astype(str))
dim_store = dim_store[['store_sk', 'store_id', 'store_name']]
dim_store.to_parquet(output_dir / "dim_store.parquet", index=False)
print(f"  ✓ dim_store.parquet ({len(dim_store)} rows)")

# Create fact tables
print("\nCreating fact tables...")

# fact_sales - normalize sales data
fact_sales = sales.reset_index(drop=True)
fact_sales['sales_id'] = range(1, len(fact_sales) + 1)

# Create surrogate keys by mapping
# Use modulo to map product/customer keys to surrogate keys
fact_sales['date_sk'] = 1  # Simplified
fact_sales['customer_sk'] = ((fact_sales['CustomerKey'] - 1) % len(dim_customer)) + 1
fact_sales['product_sk'] = ((fact_sales['ProductKey'] - 1) % len(dim_product)) + 1
territory_map = dict(zip(dim_store['store_id'], dim_store['store_sk']))
fact_sales['store_sk'] = fact_sales['TerritoryKey'].map(territory_map)

# Add date column
fact_sales['date'] = pd.to_datetime(fact_sales['OrderDate'])

# Rename and calculate net_amount
fact_sales = fact_sales.rename(columns={'OrderQuantity': 'quantity'})
fact_sales['net_amount'] = 100  # Simplified - use dummy value

fact_sales = fact_sales[['sales_id', 'date_sk', 'customer_sk', 'product_sk', 'store_sk', 'net_amount', 'date']]
fact_sales.to_parquet(output_dir / "fact_sales.parquet", index=False)
print(f"  ✓ fact_sales.parquet ({len(fact_sales)} rows)")

# fact_returns
fact_returns = returns.reset_index(drop=True)
fact_returns['return_id'] = range(1, len(fact_returns) + 1)
fact_returns['sales_id'] = 1  # Simplified
fact_returns['date_sk'] = 1  # Simplified
fact_returns['customer_sk'] = ((fact_returns['ProductKey'] - 1) % len(dim_customer)) + 1
fact_returns['product_sk'] = ((fact_returns['ProductKey'] - 1) % len(dim_product)) + 1
territory_map = dict(zip(dim_store['store_id'], dim_store['store_sk']))
fact_returns['store_sk'] = fact_returns['TerritoryKey'].map(territory_map).fillna(1)

fact_returns['refund_amount'] = fact_returns['ReturnQuantity'] * 50  # Simplified
fact_returns['date'] = pd.to_datetime(fact_returns['ReturnDate'])

fact_returns = fact_returns[['return_id', 'sales_id', 'date_sk', 'customer_sk', 'product_sk', 'store_sk', 'refund_amount', 'date']]
fact_returns.to_parquet(output_dir / "fact_returns.parquet", index=False)
print(f"  ✓ fact_returns.parquet ({len(fact_returns)} rows)")

print(f"\n✓ Schema transformation completed!")
print(f"Files saved to: {output_dir}")
