import pandas as pd
import numpy as np
import config
from db_connect import db_connect


def data_loader(configid, datasetid, runid, period, time_direction, priority, lead_time=True):
    # connect to database
    conn = db_connect()
    cursor = conn.cursor()

    # set up sorting
    if time_direction == 'forward':
        sorting = 'ASC'
    else:
        sorting = 'DESC'

    # Optimizer Production
    # Query the products from the optimizer_production table
    cursor.execute(f"""
        SELECT *
        FROM optimizer_production
        WHERE datasetid = {datasetid} AND period IN {period}
        ORDER BY CAST(period AS int) {sorting}
    """)
    production_lead_time_rows = cursor.fetchall()
    # Save the results in a DataFrame
    df_production_lead_time = pd.DataFrame(production_lead_time_rows, columns=[desc[0] for desc in cursor.description])
    # Cast integer datatypes
    df_production_lead_time['period'] = df_production_lead_time['period'].astype(int)
    df_production_lead_time['duration'] = df_production_lead_time['duration'].astype(int)
    # Drop unnecessary columns
    production_lead_time_cols = ['location', 'product', 'bomnum', 'period', 'duration']
    df_production_lead_time = df_production_lead_time[production_lead_time_cols].copy()
    # Drop duplicates
    df_production_lead_time = df_production_lead_time.drop_duplicates()

    # Results Production
    # Query the products from the results_production table
    cursor.execute(f"""
        SELECT *
        FROM results_production
        WHERE configid = {configid} AND runid = {runid} AND period IN {period}
        ORDER BY CAST(period AS int) {sorting}
    """)
    results_production_rows = cursor.fetchall()
    # Save the results in a DataFrame
    df_results_production = pd.DataFrame(results_production_rows, columns=[desc[0] for desc in cursor.description])
    # Filter out numbers close to zero
    df_results_production = df_results_production[
        abs(df_results_production['solutionvalue']) > config.threshold]
    # Drop unnecessary columns
    results_production_cols = ['location', 'product', 'bomnum', 'period', 'solutionvalue']
    df_results_production = df_results_production[results_production_cols].copy()
    # Drop duplicates
    df_results_production = df_results_production.drop_duplicates()

    # Merge Production with Lead time
    df_results_production = pd.merge(df_results_production, df_production_lead_time,
                                     on=['location', 'bomnum', 'product', 'period'], how='left')
    # Rename 'duration' to 'leadtime'
    df_results_production = df_results_production.rename(columns={'duration': 'leadtime'})

    # Optimizer Transportation
    # Query the products from the optimizer_transportation table
    cursor.execute(f"""
        SELECT *
        FROM optimizer_transportation
        WHERE datasetid = {datasetid} AND period IN {period}
    """)
    movement_lead_time_rows = cursor.fetchall()
    # Save the results in a DataFrame
    df_movement_lead_time = pd.DataFrame(movement_lead_time_rows, columns=[desc[0] for desc in cursor.description])
    # Cast integers
    df_movement_lead_time['duration'] = df_movement_lead_time['duration'].astype(int)
    # Drop unnecessary columns
    movement_lead_time_cols = ['loc_from', 'loc_to', 'product', 'period', 'transport_type', 'duration']
    df_movement_lead_time = df_movement_lead_time[movement_lead_time_cols].copy()
    # Drop duplicates
    df_movement_lead_time = df_movement_lead_time.drop_duplicates()

    # Results Movements
    # Query the products from the results_movement table
    cursor.execute(f"""
        SELECT *
        FROM results_movement
        WHERE configid = {configid} AND runid = {runid} AND period IN {period}
        ORDER BY CAST(period AS int) {sorting}
    """)
    results_movement_rows = cursor.fetchall()
    # Save the results in a DataFrame
    df_results_movement = pd.DataFrame(results_movement_rows, columns=[desc[0] for desc in cursor.description])
    # Filter out numbers close to zero
    df_results_movement = df_results_movement[
        abs(df_results_movement['solutionvalue']) > config.threshold]
    # Drop unnecessary columns
    df_results_movement_cols = ['loc_from', 'loc_to', 'product', 'period', 'solutionvalue', 'transport_type']
    df_results_movement = df_results_movement[df_results_movement_cols].copy()
    # Drop duplicates
    df_results_movement = df_results_movement.drop_duplicates()

    # Merge result movement with lead times
    df_results_movement = pd.merge(df_results_movement, df_movement_lead_time,
                                   on=['loc_from', 'loc_to', 'product', 'period', 'transport_type'], how='left')
    # Rename 'duration' to 'leadtime'
    df_results_movement = df_results_movement.rename(columns={'duration': 'leadtime'})

    # Results Procurement
    # Query the products from the results_procurement table
    cursor.execute(f"""
        SELECT *
        FROM results_procurement
        WHERE configid = {configid} AND runid = {runid} AND period IN {period}
        ORDER BY CAST(period AS int) {sorting}
    """)
    results_procurement_rows = cursor.fetchall()
    # Save the results in a DataFrame
    df_results_procurement = pd.DataFrame(results_procurement_rows, columns=[desc[0] for desc in cursor.description])
    # Filter out values close to zero
    df_results_procurement = df_results_procurement[abs(df_results_procurement['solutionvalue']) > config.threshold]
    # Drop unnecessary columns
    df_results_procurement_cols = ['location', 'product', 'period', 'solutionvalue', 'supplier']
    df_results_procurement = df_results_procurement[df_results_procurement_cols].copy()
    # Drop duplicates
    df_results_procurement = df_results_procurement.drop_duplicates()

    # Initial Stock
    # Execute the query to retrieve demands
    cursor.execute(f"""
           SELECT *
           FROM optimizer_storage
           WHERE datasetid = {datasetid} AND period IN {period}
           ORDER BY CAST(period AS int) {sorting}
       """)
    initial_stock_rows = cursor.fetchall()
    # Save the results in a DataFrame
    df_initial_stock = pd.DataFrame(initial_stock_rows, columns=[desc[0] for desc in cursor.description])
    # Filter out values close to zero
    df_initial_stock = df_initial_stock[~df_initial_stock['initialstock'].isna()]
    df_initial_stock = df_initial_stock[abs(df_initial_stock['initialstock']) > config.threshold]
    # Drop unnecessary columns
    initial_stock_cols = ['location', 'product', 'initialstock', 'period']
    df_initial_stock = df_initial_stock[initial_stock_cols].copy()
    # Drop duplicates
    df_initial_stock = df_initial_stock.drop_duplicates()

    # Results Stock
    # Query the products from the results_production table
    cursor.execute(f"""
        SELECT *
        FROM results_stock
        WHERE configid = {configid} AND runid = {runid} AND period IN {period}
        ORDER BY CAST(period AS int) {sorting}
    """)
    results_stock_rows = cursor.fetchall()
    # Save the results in a DataFrame
    df_results_stock = pd.DataFrame(results_stock_rows, columns=[desc[0] for desc in cursor.description])
    # Drop unnecessary columns
    df_results_stock_cols = ['location', 'product', 'period', 'solutionvalue']
    df_results_stock = df_results_stock[df_results_stock_cols].copy()
    # Drop duplicates
    df_results_stock = df_results_stock.drop_duplicates()

    # Merge result stock with initial stock
    df_results_stock = pd.merge(df_results_stock, df_initial_stock, on=['location', 'product', 'period'],
                                how='left').fillna(0)

    # Sort the dataframe by location, product, and period in descending order
    df_results_stock = df_results_stock.sort_values(['location', 'product', 'period'],
                                                    ascending=[True, True, False])

    # Create the 'period_spent' column
    df_results_stock['period_spent'] = df_results_stock.groupby(['location', 'product'])['solutionvalue'].diff().shift(
        -1)

    df_results_stock.loc[df_results_stock['period'] == 0, 'period_spent'] = \
        df_results_stock['initialstock'] - df_results_stock['solutionvalue']

    # Create the 'extra_res' column
    df_results_stock['extra_res'] = -np.minimum(0, df_results_stock['period_spent'])

    # Remove negative 'period_spent'
    df_results_stock['period_spent'] = np.maximum(0, df_results_stock['period_spent'])

    # # Remove rows with zero total stock
    # df_results_stock = df_results_stock[(df_results_stock['solutionvalue'] +
    #                                      df_results_stock['initialstock'] +
    #                                      abs(df_results_stock['period_spent'])) > threshold]

    # Execute the query to retrieve demands
    cursor.execute(f"""
        SELECT *
        FROM optimizer_demand
        WHERE datasetid = {datasetid} AND period IN {period}
        ORDER BY CAST(period AS int) {sorting}
    """)
    demand_rows = cursor.fetchall()
    # Save the results in a DataFrame
    df_demand = pd.DataFrame(demand_rows, columns=[desc[0] for desc in cursor.description])
    # Filter out values close to zero
    df_demand = df_demand[abs(df_demand['quantity']) > config.threshold]
    # Drop unnecessary columns
    demand_cols = ['location', 'product', 'client', 'quantity', 'price', 'period']
    df_demand = df_demand[demand_cols].copy()
    # Drop duplicates
    df_demand = df_demand.drop_duplicates()

    # Results Sales
    # Execute the query to retrieve sales
    cursor.execute(f"""
        SELECT *
        FROM results_sale
        WHERE configid = {configid} AND runid = {runid} AND period IN {period}
        ORDER BY CAST(period AS int) {sorting}
    """)
    sale_rows = cursor.fetchall()
    # Save the results in a DataFrame
    df_results_sale = pd.DataFrame(sale_rows, columns=[desc[0] for desc in cursor.description])
    # Filter out values close to zero
    df_results_sale = df_results_sale[abs(df_results_sale['solutionvalue']) > config.threshold]
    # Drop unnecessary columns
    results_sale_cols = ['location', 'product', 'client', 'solutionvalue', 'period']
    df_results_sale = df_results_sale[results_sale_cols].copy()
    # Drop duplicates and reset index
    df_results_sale = df_results_sale.drop_duplicates().reset_index(drop=True)

    # Merge sales with demand
    df_results_sale = pd.merge(df_results_sale, df_demand, on=['location', 'product', 'client', 'period'])

    # Calculate the product of solution_value and price
    df_results_sale['total_price'] = df_results_sale['solutionvalue'] * df_results_sale['price']

    # Execute the query to retrieve BOMs
    cursor.execute(f"""
        SELECT *
        FROM optimizer_bom
        WHERE datasetid = {datasetid} AND period IN {period}
        ORDER BY CAST(period AS int) {sorting}
    """)
    bom_rows = cursor.fetchall()
    # Save the results in a DataFrame
    df_bom = pd.DataFrame(bom_rows, columns=[desc[0] for desc in cursor.description])
    # Drop unnecessary columns
    df_bom_cols = ['bomnum', 'location', 'product', 'input_output', 'period']
    df_bom = df_bom[df_bom_cols].copy()
    # Drop duplicates
    df_bom = df_bom.drop_duplicates()

    # Close the cursor and the database connection
    conn.commit()
    cursor.close()
    conn.close()

    # Parameters
    # Sorting parameters
    if time_direction == 'backward' and priority == 'total_price':
        parameters_list = [False, False]
    else:
        parameters_list = [True, False]
    df_results_sale = df_results_sale.sort_values(['period', 'total_price'], ascending=parameters_list).reset_index(
        drop=True)

    # Whether to ignore lead time
    if not lead_time:
        df_results_production['leadtime'] = 0
        df_results_movement['leadtime'] = 0

    # Save input data to file
    filepath = f'input/resource_mapper_input_{config.time_direction}_{config.priority}.xlsx'
    with pd.ExcelWriter(filepath) as writer:
        df_results_sale.to_excel(writer, sheet_name='sales')
        df_results_stock.to_excel(writer, sheet_name='stock', index=False)
        df_results_production.to_excel(writer, sheet_name='production', index=False)
        df_results_movement.to_excel(writer, sheet_name='movement', index=False)
        df_results_procurement.to_excel(writer, sheet_name='procurement', index=False)

    # Assign 'loc_from' and 'lock_to' and 'type' for each input table for consistency
    df_results_stock = df_results_stock.assign(
        loc_from=df_results_stock['location'],
        loc_to=df_results_stock['location'],
        type='stock',
    )
    df_results_sale = df_results_sale.assign(
        loc_from=df_results_sale['location'],
        loc_to=df_results_sale['location'],
        type='sale'
    )
    df_results_production = df_results_production.assign(
        loc_from=df_results_production['location'],
        loc_to=df_results_production['location'],
        type='production'
    )
    df_results_procurement = df_results_procurement.assign(
        loc_from=df_results_procurement['location'],
        loc_to=df_results_procurement['location'],
        type='procurement'
    )
    df_results_movement = df_results_movement.assign(type='movement')

    # Create unique keys
    df_results_sale['keys'] = df_results_sale.apply(
        lambda row: '.'.join(row[['client', 'period', 'location', 'product']].astype(str)), axis=1)
    df_results_production['keys'] = df_results_production.apply(
        lambda row: '.'.join(row[['location', 'period', 'product', 'bomnum']].astype(str)), axis=1)
    df_results_stock['keys'] = df_results_stock.apply(
        lambda row: '.'.join(row[['location', 'period', 'product']].astype(str)), axis=1)
    df_results_movement['keys'] = df_results_movement.apply(
        lambda row: '.'.join(row[['loc_to', 'loc_from', 'product', 'period', 'transport_type']].astype(str)), axis=1)
    df_results_procurement['keys'] = df_results_procurement.apply(
        lambda row: '.'.join(row[['product', 'supplier', 'period']].astype(str)), axis=1)

    # Initialize residuals counting
    df_results_sale['residual'] = df_results_sale['solutionvalue']

    # Initialize leftovers counting for the resources
    df_results_stock['is_leftover'] = df_results_stock['initialstock']
    df_results_stock['sv_leftover'] = df_results_stock['solutionvalue']
    df_results_stock['ps_leftover'] = df_results_stock['period_spent']
    df_results_stock['er_leftover'] = df_results_stock['extra_res']
    df_results_production['leftover'] = df_results_production['solutionvalue']
    df_results_movement['leftover'] = df_results_movement['solutionvalue']
    df_results_procurement['leftover'] = df_results_procurement['solutionvalue']

    return df_results_sale, df_results_stock, df_results_production, df_results_movement, df_results_procurement, df_bom
