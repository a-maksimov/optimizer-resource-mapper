import pandas as pd
from tqdm import tqdm
from resource_mapper import map_resources
import config
from data_loader import data_loader


def run_resource_mapper():
    pd.set_option('display.float_format', lambda x: '%.3f' % x)

    # load data
    df_sales, df_production, df_stock, df_movement, df_procurement, df_bom = data_loader(
        config.configid,
        config.datasetid,
        config.runid,
        config.period,
        config.time_direction,
        config.priority
    )

    filepath = f'input/resource_mapper_input_{config.time_direction}_{config.priority}.xlsx'
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

    # initialize residual counting for resources
    df_stock['residual'] = df_stock['solutionvalue']
    df_movement['residual'] = df_movement['value']

    # initialize leftover counting for resources
    df_production['leftover'] = df_production['value']
    df_stock['leftover'] = df_stock['value']
    df_movement['leftover'] = df_movement['value']
    df_procurement['leftover'] = df_procurement['value']

    # Iterate over rows in sorted sales dataframe
    for sale in tqdm(df_sales.iterrows(), total=len(df_sales)):

        # get the order_id and the row of the sale
        order_id, order = sale

        # name series with its index
        order.name = order_id

        # label
        label = '.'.join([order['client'], str(order['period']), order['location'], order['product']])

        # run recursive mapping and get updated resources
        result = map_resources(
            order, order_id, label,
            df_production, df_stock, df_movement, df_procurement,
            df_bom, config.map_bom
        )

        # if the product was found in the resources in any location
        if result:
            # unpack results
            df_production_updated, df_stock_updated, df_movement_updated, df_procurement_updated, \
                production, stock, movement, procurement = result

            # update resources dataframes
            df_production, df_stock, df_movement, df_procurement = \
                df_production_updated, df_stock_updated, df_movement_updated, df_procurement_updated

            # update mapped resources
            mapped_production = pd.concat([mapped_production, production], ignore_index=True)
            mapped_stock = pd.concat([mapped_stock, stock], ignore_index=True)
            mapped_movement = pd.concat([mapped_movement, movement], ignore_index=True)
            mapped_procurement = pd.concat([mapped_procurement, procurement], ignore_index=True)

            # update mapped sales
            order = order.drop(labels='leftover').to_frame().T
            order['label'] = label
            mapped_sales = pd.concat([mapped_sales, order])

    filepath = f'results/resource_mapped_results_{config.time_direction}_{config.priority}.xlsx'
    with pd.ExcelWriter(filepath) as writer:
        mapped_sales.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='mapped_sales')
        mapped_production.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='mapped_production', index=False)
        mapped_stock.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='mapped_stock', index=False)
        mapped_movement.to_excel(writer, sheet_name='mapped_movement', index=False)
        mapped_procurement.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='mapped_procurement', index=False)

    filepath = f'results/resource_output_resources_{config.time_direction}_{config.priority}.xlsx'
    with pd.ExcelWriter(filepath) as writer:
        df_production.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_production', index=False)
        df_stock.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_stock', index=False)
        df_movement.to_excel(writer, sheet_name='output_movement', index=False)
        df_procurement.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_procurement', index=False)


if __name__ == '__main__':
    run_resource_mapper()
