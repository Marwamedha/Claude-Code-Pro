"""
Data visualization and KPI analysis script.

Reads parquet files and generates KPIs and visualizations.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime

# Setup
DATA_DIR = Path("/workspaces/Claude-Code-Pro/.Claude Code/migrate/data")
OUTPUT_DIR = Path("/workspaces/Claude-Code-Pro/.Claude Code/Visualize/visualizations")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

# Load data
print("Loading data...")
dim_customer = pd.read_parquet(DATA_DIR / "dim_customer.parquet")
dim_date = pd.read_parquet(DATA_DIR / "dim_date.parquet")
dim_product = pd.read_parquet(DATA_DIR / "dim_product.parquet")
dim_store = pd.read_parquet(DATA_DIR / "dim_store.parquet")
fact_sales = pd.read_parquet(DATA_DIR / "fact_sales.parquet")
fact_returns = pd.read_parquet(DATA_DIR / "fact_returns.parquet")

print("Data loaded successfully!")
print(f"Sales records: {len(fact_sales)}")
print(f"Returns records: {len(fact_returns)}")

# Merge data for analysis
sales_data = fact_sales.copy()
sales_data = sales_data.merge(dim_store, left_on="store_sk", right_on="store_sk", how="left")
sales_data = sales_data.merge(dim_product, left_on="product_sk", right_on="product_sk", how="left")
sales_data = sales_data.merge(dim_customer, left_on="customer_sk", right_on="customer_sk", how="left")

returns_data = fact_returns.copy()
returns_data = returns_data.merge(dim_store, left_on="store_sk", right_on="store_sk", how="left")
returns_data = returns_data.merge(dim_product, left_on="product_sk", right_on="product_sk", how="left")
# Get customer info from sales table
customer_info = fact_sales[['sales_id', 'customer_sk']].merge(dim_customer, left_on="customer_sk", right_on="customer_sk", how="left")
returns_data = returns_data.merge(customer_info, on="sales_id", how="left")

# Build KPIs
print("\n=== Building KPIs ===")
total_sales = fact_sales["net_amount"].sum()
total_returns = fact_returns["refund_amount"].sum()
net_sales = total_sales - total_returns
avg_sales_per_store = fact_sales.groupby("store_sk")["net_amount"].mean().mean()
avg_sales_per_product = fact_sales.groupby("product_sk")["net_amount"].mean().mean()
avg_sales_per_customer = fact_sales.groupby("customer_sk")["net_amount"].mean().mean()

print(f"Total Sales: ${total_sales:,.2f}")
print(f"Total Returns: ${total_returns:,.2f}")
print(f"Net Sales: ${net_sales:,.2f}")
print(f"Average Sales per Store: ${avg_sales_per_store:,.2f}")
print(f"Average Sales per Product: ${avg_sales_per_product:,.2f}")
print(f"Average Sales per Customer: ${avg_sales_per_customer:,.2f}")

# Visualization 1: Sales Trend Over Time
print("\nCreating Sales Trend Over Time...")
sales_by_date = sales_data.groupby("date")["net_amount"].sum().reset_index()
sales_by_date = sales_by_date.sort_values("date")

plt.figure(figsize=(14, 6))
plt.plot(sales_by_date["date"], sales_by_date["net_amount"], linewidth=2, color="#1f77b4")
plt.title("Sales Trend Over Time", fontsize=14, fontweight="bold")
plt.xlabel("Date")
plt.ylabel("Sales Amount ($)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "sales_trend_over_time.png", dpi=300, bbox_inches="tight")
plt.close()

# Visualization 2: Sales by Store
print("Creating Sales by Store...")
sales_by_store = sales_data.groupby("store_name")["net_amount"].sum().sort_values(ascending=False)

plt.figure(figsize=(12, 6))
sales_by_store.plot(kind="bar", color="#2ca02c")
plt.title("Total Sales by Store", fontsize=14, fontweight="bold")
plt.xlabel("Store")
plt.ylabel("Sales Amount ($)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "sales_by_store.png", dpi=300, bbox_inches="tight")
plt.close()

# Visualization 3: Sales by Product
print("Creating Sales by Product...")
sales_by_product = sales_data.groupby("ProductName")["net_amount"].sum().sort_values(ascending=False).head(15)

plt.figure(figsize=(12, 6))
sales_by_product.plot(kind="barh", color="#d62728")
plt.title("Top 15 Sales by Product", fontsize=14, fontweight="bold")
plt.xlabel("Sales Amount ($)")
plt.ylabel("Product")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "sales_by_product.png", dpi=300, bbox_inches="tight")
plt.close()

# Visualization 4: Sales by Customer
print("Creating Sales by Customer...")
sales_by_customer = sales_data.groupby("LastName")["net_amount"].sum().sort_values(ascending=False).head(15)

plt.figure(figsize=(12, 6))
sales_by_customer.plot(kind="barh", color="#ff7f0e")
plt.title("Top 15 Sales by Customer", fontsize=14, fontweight="bold")
plt.xlabel("Sales Amount ($)")
plt.ylabel("Customer")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "sales_by_customer.png", dpi=300, bbox_inches="tight")
plt.close()

# Visualization 5: Returns Trend Over Time
print("Creating Returns Trend Over Time...")
returns_by_date = returns_data.groupby("date")["refund_amount"].sum().reset_index()
returns_by_date = returns_by_date.sort_values("date")

plt.figure(figsize=(14, 6))
plt.plot(returns_by_date["date"], returns_by_date["refund_amount"], linewidth=2, color="#d62728")
plt.title("Returns Trend Over Time", fontsize=14, fontweight="bold")
plt.xlabel("Date")
plt.ylabel("Return Amount ($)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "returns_trend_over_time.png", dpi=300, bbox_inches="tight")
plt.close()

# Visualization 6: Returns by Store
print("Creating Returns by Store...")
returns_by_store = returns_data.groupby("store_name")["refund_amount"].sum().sort_values(ascending=False)

plt.figure(figsize=(12, 6))
returns_by_store.plot(kind="bar", color="#9467bd")
plt.title("Total Returns by Store", fontsize=14, fontweight="bold")
plt.xlabel("Store")
plt.ylabel("Return Amount ($)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "returns_by_store.png", dpi=300, bbox_inches="tight")
plt.close()

# Visualization 7: Returns by Product
print("Creating Returns by Product...")
returns_by_product = returns_data.groupby("ProductName")["refund_amount"].sum().sort_values(ascending=False).head(15)

plt.figure(figsize=(12, 6))
returns_by_product.plot(kind="barh", color="#8c564b")
plt.title("Top 15 Returns by Product", fontsize=14, fontweight="bold")
plt.xlabel("Return Amount ($)")
plt.ylabel("Product")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "returns_by_product.png", dpi=300, bbox_inches="tight")
plt.close()

# Visualization 8: Returns by Customer
print("Creating Returns by Customer...")
returns_data['customer_name'] = returns_data['LastName']
returns_by_customer = returns_data.groupby("customer_name")["refund_amount"].sum().sort_values(ascending=False).head(15)

plt.figure(figsize=(12, 6))
returns_by_customer.plot(kind="barh", color="#e377c2")
plt.title("Top 15 Returns by Customer", fontsize=14, fontweight="bold")
plt.xlabel("Return Amount ($)")
plt.ylabel("Customer")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "returns_by_customer.png", dpi=300, bbox_inches="tight")
plt.close()

print(f"\n[OK] All visualizations saved to {OUTPUT_DIR}")
print("[OK] Visualization complete!")