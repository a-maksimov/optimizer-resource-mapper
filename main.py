import pandas as pd
from tqdm import tqdm
from resource_mapper import map_resources
from config import configid, datasetid, runid, period
from data_loader import data_loader

pd.set_option('display.float_format', lambda x: '%.3f' % x)

# load data
df_sales, df_production, df_stock, df_movement, df_procurement = data_loader(configid, datasetid, runid, period)

filepath = 'input/resource_mapper_input.xlsx'
with pd.ExcelWriter(filepath) as writer:
    df_sales.to_excel(writer, sheet_name='sales')
    df_production.to_excel(writer, sheet_name='production', index=False)
    df_stock.to_excel(writer, sheet_name='stock', index=False)
    df_movement.to_excel(writer, sheet_name='movement', index=False)
    df_procurement.to_excel(writer, sheet_name='procurement', index=False)

# Create empty DataFrames to store the mapped resources
mapped_production = pd.DataFrame()
mapped_stock = pd.DataFrame()
mapped_movement = pd.DataFrame()
mapped_sales = pd.DataFrame()
mapped_procurement = pd.DataFrame()

# Iterate over rows in sorted sales dataframe
for sale in tqdm(df_sales.iterrows(), total=len(df_sales)):

    # get the order_id and the row of the sale
    order_id, order = sale

    # name series with its index
    order.name = order_id

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
        order = order.to_frame().T
        order['order_id'] = order_id
        mapped_sales = pd.concat([mapped_sales, order], ignore_index=True)

# create an ExcelWriter object and specify the file name
filepath = 'results/resource_mapper_results.xlsx'
with pd.ExcelWriter(filepath) as writer:
    mapped_sales.to_excel(writer, sheet_name='mapped_sales')
    mapped_production.to_excel(writer, sheet_name='mapped_production', index=False)
    mapped_stock.to_excel(writer, sheet_name='mapped_stock', index=False)
    mapped_movement.to_excel(writer, sheet_name='mapped_movement', index=False)

pass
