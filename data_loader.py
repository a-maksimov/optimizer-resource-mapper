import pandas as pd
from db_connect import db_connect


def data_loader(configid, datasetid, runid, period):
    # Connect to database
    conn = db_connect()
    cursor = conn.cursor()

    # Results Production
    # Query the products from the results_production table
    cursor.execute(f"""
        SELECT *
        FROM results_production
        WHERE configid = {configid} AND runid = {runid} AND period IN {period} AND CAST(solutionvalue AS float) >= 1
        ORDER BY CAST(period AS int)
    """)
    results_production_rows = cursor.fetchall()

    # Save the results in a DataFrame
    df_results_production = pd.DataFrame(results_production_rows, columns=[desc[0] for desc in cursor.description])
    df_results_production['solutionvalue'] = df_results_production['solutionvalue'].astype(float)
    df_results_production_cols = ['location', 'product', 'bomnum', 'period', 'solutionvalue']
    df_results_production = df_results_production[df_results_production_cols].copy()
    df_results_production = df_results_production.drop_duplicates()

    # Results Movements
    # Query the products from the results_movement table
    cursor.execute(f"""
        SELECT *
        FROM results_movement
        WHERE configid = {configid} AND runid = {runid} AND period IN {period} AND CAST(solutionvalue AS float) >= 1
        ORDER BY CAST(period AS int)
    """)
    results_movement_rows = cursor.fetchall()

    # Save the results in a DataFrame
    df_results_movement = pd.DataFrame(results_movement_rows, columns=[desc[0] for desc in cursor.description])
    df_results_movement['solutionvalue'] = df_results_movement['solutionvalue'].astype(float)
    df_results_movement_cols = ['loc_from', 'loc_to', 'product', 'period', 'solutionvalue', 'transport_type']
    df_results_movement = df_results_movement[df_results_movement_cols].copy()
    df_results_movement = df_results_movement.drop_duplicates()

    # Results Procurement
    # Query the products from the results_procurement table
    cursor.execute(f"""
        SELECT *
        FROM results_procurement
        WHERE configid = {configid} AND runid = {runid} AND period IN {period} AND CAST(solutionvalue AS float) >= 1
        ORDER BY CAST(period AS int)
    """)
    df_results_procurement_rows = cursor.fetchall()

    # Save the results in a DataFrame
    df_results_procurement = pd.DataFrame(df_results_procurement_rows, columns=[desc[0] for desc in cursor.description])
    df_results_procurement['solutionvalue'] = df_results_procurement['solutionvalue'].astype(float)
    df_results_procurement_cols = ['location', 'product', 'period', 'solutionvalue', 'supplier']
    df_results_procurement = df_results_procurement[df_results_procurement_cols].copy()
    df_results_procurement = df_results_procurement.drop_duplicates()

    # Results Stock
    # Query the products from the results_production table
    cursor.execute(f"""
        SELECT *
        FROM results_stock
        WHERE configid = {configid} AND runid = {runid} AND period IN {period} AND CAST(solutionvalue AS float) >= 0
        ORDER BY CAST(period AS int)
    """)
    results_stock_rows = cursor.fetchall()

    # Save the results in a DataFrame
    df_results_stock = pd.DataFrame(results_stock_rows, columns=[desc[0] for desc in cursor.description])
    df_results_stock['solutionvalue'] = df_results_stock['solutionvalue'].astype(float)
    df_results_stock_cols = ['location', 'product', 'period', 'solutionvalue']
    df_results_stock = df_results_stock[df_results_stock_cols].copy()
    df_results_stock = df_results_stock.drop_duplicates()

    # Initial Stock
    # Execute the query to retrieve demands
    cursor.execute(f"""
        SELECT *
        FROM optimizer_storage
        WHERE datasetid = {datasetid} AND period IN {period} AND CAST(initialstock AS float) >= 1
        ORDER BY CAST(period as int)
    """)
    initial_stock_rows = cursor.fetchall()

    # Save the results in a DataFrame
    df_initial_stock = pd.DataFrame(initial_stock_rows, columns=[desc[0] for desc in cursor.description])
    df_initial_stock['initialstock'] = df_initial_stock['initialstock'].astype(float)
    df_initial_stock_cols = ['location', 'product', 'initialstock', 'period']
    df_initial_stock = df_initial_stock[df_initial_stock_cols].copy()
    df_initial_stock = df_initial_stock.reset_index(drop=True)
    df_initial_stock = df_initial_stock.drop_duplicates()

    # Merge result stock with initial stock
    df_results_stock = pd.merge(df_results_stock, df_initial_stock, on=['location', 'product', 'period'],
                                how='left').fillna(0)

    df_results_stock.head(50)

    df_results_stock['value'] = df_results_stock['solutionvalue'] + df_results_stock['initialstock']

    df_results_stock = df_results_stock[df_results_stock['value'] >= 1]

    # Results Sales
    # Execute the query to retrieve sales
    cursor.execute(f"""
        SELECT *
        FROM results_sale
        WHERE configid = {configid} AND runid = {runid} AND period IN {period} AND CAST(solutionvalue AS float) >= 1
        ORDER BY CAST(period AS int)
    """)
    sale_rows = cursor.fetchall()

    # Save the results in a DataFrame
    df_results_sale = pd.DataFrame(sale_rows, columns=[desc[0] for desc in cursor.description])
    df_results_sale['solutionvalue'] = df_results_sale['solutionvalue'].astype(float)
    results_sale_cols = ['location', 'product', 'client', 'solutionvalue', 'period']
    df_results_sale = df_results_sale[results_sale_cols].copy()
    df_results_sale = df_results_sale.drop_duplicates()

    # Close the cursor and the database connection
    conn.commit()
    cursor.close()
    conn.close()

    # assign 'loc_from' and 'lock_to' for each table for consistency df_stock has a column 'value' which is
    # 'solutionvalue' + 'initialstock', so we assign 'value' column for each table
    df_results_stock = df_results_stock.assign(loc_from=df_results_stock['location'],
                                               loc_to=df_results_stock['location'],
                                               type='stock')

    df_results_sale = df_results_sale.assign(loc_from=df_results_sale['location'],
                                             loc_to=df_results_sale['location'],
                                             value=df_results_sale['solutionvalue'],
                                             type='sale')
    df_results_sale = df_results_sale.drop(['solutionvalue'], axis=1)

    df_results_production = df_results_production.assign(loc_from=df_results_production['location'],
                                                         loc_to=df_results_production['location'],
                                                         value=df_results_production['solutionvalue'],
                                                         type='production')
    df_results_production = df_results_production.drop(['solutionvalue'], axis=1)

    df_results_procurement = df_results_procurement.assign(loc_from=df_results_procurement['location'],
                                                           loc_to=df_results_procurement['location'],
                                                           value=df_results_procurement['solutionvalue'],
                                                           type='procurement')
    df_results_procurement = df_results_procurement.drop(['solutionvalue'], axis=1)

    df_results_movement = df_results_movement.assign(value=df_results_movement['solutionvalue'],
                                                     type='movement')

    return df_results_sale, df_results_production, df_results_stock, df_results_movement, df_results_procurement
