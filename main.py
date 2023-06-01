import pandas as pd
from tqdm import tqdm
from resource_mapper import map_resources
from config import configid, datasetid, runid, period
from data_loader import data_loader
pd.set_option('display.float_format', lambda x: '%.3f' % x)

# Load data
df_sales, df_production, df_stock, df_movement = data_loader(configid, datasetid, runid, period)

# Create empty DataFrames to store the mapped resources
mapped_production = pd.DataFrame()
mapped_stock = pd.DataFrame()
mapped_movement = pd.DataFrame()

# TODO: First sort by period, then by total_value
# TODO: Look for resources in the same period first
# TODO: Make from_loc amd to_loc in every table for consistency after parametrization
# Iterate over rows in sorted sales dataframe
for sale in tqdm(df_sales.iterrows(), total=len(df_sales)):

    # get the order_id and the row of the sale
    order_id, order = sale

    # run recursive mapping and get updated resources
    result = map_resources(order, order_id, df_production, df_stock, df_movement)

    # if the product was found in the resources in any location
    if result:
        df_production_updated, df_stock_updated, df_movement_updated, production, stock, movement = result

        # update resources dataframes
        df_production, df_stock, df_movement = df_production_updated, df_stock_updated, df_movement_updated

        # update mapped resources
        mapped_production = pd.concat([mapped_production, production], ignore_index=True)
        mapped_stock = pd.concat([mapped_stock, stock], ignore_index=True)
        mapped_movement = pd.concat([mapped_movement, movement], ignore_index=True)

pass


