# E-Commerce Sales Analytics using Databricks

## Project Overview
Built an E-Commerce Sales Analytics solution using Databricks, PySpark, Delta Lake, and SQL to process sales data and generate business insights through an AI/BI dashboard.

## Technologies Used
- Databricks
- Python and PySpark
- Apache Spark SQL
- Delta Lake
- Medallion Architecture (Bronze, Silver, Gold)
- Databricks AI/BI Dashboards

## Data Pipeline
1. Bronze Layer: Ingest raw e-commerce sales data.
2. Silver Layer: Apply data-quality filters and calculate revenue.
3. Gold Layer: Create aggregated daily, product-category, and regional sales tables.
4. Dashboard: Visualize sales KPIs and business performance.

## Key Analytics
- Total revenue and orders
- Units sold and customer ratings
- Monthly revenue trends
- Revenue by product category
- Regional sales performance
- Payment-method analysis

## Project Files
- `Ecommerce_Sales_Lakehouse.py` — PySpark transformation and analytics code
- `dashboard.png` — Dashboard screenshot

## How to Run
1. Import the Python source into a Databricks workspace.
2. Upload the source CSV to the Databricks Volume path configured in the script.
3. Run the notebook cells in sequence.
4. Verify the generated Delta tables and configure the dashboard queries.

## Author
Gopinath D.

*Note: Update the CSV path to match your Databricks environment before running.*
