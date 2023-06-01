import pandas as pd
from db_connect import db_connect


def data_loader(configid, datasetid, runid, period):
    # Connect to database
    conn = db_connect()
    cursor = conn.cursor()

    # Optimizer Demand
    # Execute the query to retrieve demands
    cursor.execute(f"""
        SELECT *, CAST(price AS float) * CAST(quantity AS float) AS total_price
        FROM optimizer_demand
        WHERE datasetid = {datasetid} AND period IN {period} AND CAST(quantity AS float) >= 1
        ORDER BY total_price DESC
    """)
    demand_rows = cursor.fetchall()

    # Save the results in a DataFrame
    df_demand = pd.DataFrame(demand_rows, columns=[desc[0] for desc in cursor.description])
    df_demand[['quantity', 'price']] = df_demand[['quantity', 'price']].astype(float)
    demand_cols = ['location', 'product', 'client', 'quantity', 'price', 'total_price', 'period', 'region']
    df_demand = df_demand[demand_cols].copy()
    df_demand = df_demand.reset_index(drop=True)
    df_demand = df_demand.drop_duplicates()

    # Results Sales
    # Execute the query to retrieve sales
    cursor.execute(f"""
        SELECT *
        FROM results_sale
        WHERE configid = {configid} AND runid = {runid} AND period IN {period} AND CAST(solutionvalue AS float) >= 0
        ORDER BY CAST(solutionvalue AS float) DESC
    """)
    sale_rows = cursor.fetchall()

    # Save the results in a DataFrame
    df_results_sale = pd.DataFrame(sale_rows, columns=[desc[0] for desc in cursor.description])
    df_results_sale['solutionvalue'] = df_results_sale['solutionvalue'].astype(float)
    results_sale_cols = ['location', 'product', 'client', 'solutionvalue', 'period', 'region']
    df_results_sale = df_results_sale[results_sale_cols].copy()
    df_results_sale = df_results_sale.drop_duplicates()

    # Merge demand with sales
    df_merged = pd.merge(df_demand, df_results_sale, on=['location', 'product', 'client', 'period', 'region'])
    # TODO: What to do with the rest of demands?
    # Filtering rows where solutionvalue is equal to at least 20% of demand
    df_filtered = df_merged[df_merged['solutionvalue'] / df_merged['quantity'] >= 0.2].copy()
    # Calculate the product of solution_value and price
    df_filtered['total_value'] = df_filtered['solutionvalue'] * df_filtered['price']
    # Sort the DataFrame by total_value in descending order
    df_sorted = df_filtered.sort_values(['period', 'total_value'], ascending=[False, False])

    # Results Production
    # Query the products from the results_production table
    cursor.execute(f"""
        SELECT *
        FROM results_production
        WHERE configid = {configid} AND runid = {runid} AND period IN {period} AND CAST(solutionvalue AS float) >= 1
        ORDER BY CAST(solutionvalue AS float) DESC
    """)
    results_production_rows = cursor.fetchall()

    # Save the results in a DataFrame
    df_results_production = pd.DataFrame(results_production_rows, columns=[desc[0] for desc in cursor.description])
    df_results_production['solutionvalue'] = df_results_production['solutionvalue'].astype(float)
    df_results_production_cols = ['location', 'product', 'bomnum', 'period', 'solutionvalue']
    df_results_production = df_results_production[df_results_production_cols].copy()
    df_results_production = df_results_production.drop_duplicates()

    # Results Stock
    # Query the products from the results_production table
    cursor.execute(f"""
        SELECT *
        FROM results_stock
        WHERE configid = {configid} AND runid = {runid} AND period IN {period} AND CAST(solutionvalue AS float) >= 1
        ORDER BY CAST(solutionvalue AS float) DESC
    """)
    results_stock_rows = cursor.fetchall()

    # Save the results in a DataFrame
    df_results_stock = pd.DataFrame(results_stock_rows, columns=[desc[0] for desc in cursor.description])
    df_results_stock['solutionvalue'] = df_results_stock['solutionvalue'].astype(float)
    df_results_stock_cols = ['location', 'product', 'period', 'solutionvalue']
    df_results_stock = df_results_stock[df_results_stock_cols].copy()
    df_results_stock = df_results_stock.drop_duplicates()

    # Results Movements
    # Query the products from the results_production table
    cursor.execute(f"""
        SELECT *
        FROM results_movement
        WHERE configid = {configid} AND runid = {runid} AND period IN {period} AND CAST(solutionvalue AS float) >= 1
        ORDER BY CAST(solutionvalue AS float) DESC
    """)
    results_movement_rows = cursor.fetchall()

    # Save the results in a DataFrame
    df_results_movement = pd.DataFrame(results_movement_rows, columns=[desc[0] for desc in cursor.description])
    df_results_movement['solutionvalue'] = df_results_movement['solutionvalue'].astype(float)
    df_results_movement_cols = ['loc_from', 'loc_to', 'product', 'period', 'solutionvalue']
    df_results_movement = df_results_movement[df_results_movement_cols].copy()
    df_results_movement = df_results_movement.drop_duplicates()

    # df_results_production['leftover'] = df_results_production['solutionvalue']

    # Copy tables
    df_production = df_results_production.copy()
    df_stock = df_results_stock.copy()
    df_movement = df_results_movement.copy()

    # Close the cursor and the database connection
    conn.commit()
    cursor.close()
    conn.close()

    return df_sorted, df_production, df_stock, df_movement
