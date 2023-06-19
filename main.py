import pandas as pd
from tqdm import tqdm
from resource_mapper import map_resources
import config
from data_loader import data_loader


def run_resource_mapper():
    pd.set_option('display.float_format', lambda x: '%.3f' % x)

    # load data
    df_sales, df_stock, df_production, df_movement, df_procurement, df_bom, df_capacity = data_loader(
        config.configid,
        config.datasetid,
        config.runid,
        config.period,
        config.time_direction,
        config.priority,
        config.lead_time
    )

    # Create empty DataFrames to store the mapped resources
    mapped_stock = pd.DataFrame()
    mapped_production = pd.DataFrame()
    mapped_movement = pd.DataFrame()
    mapped_procurement = pd.DataFrame()
    mapped_capacity = pd.DataFrame()
    mapped_sales = pd.DataFrame()

    # Iterate over rows in sorted sales dataframe
    for sale in tqdm(df_sales.iterrows(), total=len(df_sales)):

        # get the order_id and the row of the sale
        order_id, order = sale

        # name series with its index
        order.name = order_id

        # label
        label = order['keys']

        # run recursive mapping and get updated resources
        result = map_resources(order, order_id, label, df_stock, df_production, df_movement, df_procurement,
                               config.map_priority, df_bom, df_capacity, config.threshold)

        # if the product was found in the resources in any location
        if result:
            # unpack the results
            df_stock_updated, df_production_updated, df_movement_updated, df_procurement_updated, df_capacity_updated, \
                stock, production, movement, procurement, capacity = result

            # update resources dataframes
            df_stock, df_production, df_movement, df_procurement = \
                df_stock_updated, df_production_updated, df_movement_updated, df_procurement_updated

            # update mapped resources
            mapped_stock = pd.concat([mapped_stock, stock], ignore_index=True)
            mapped_production = pd.concat([mapped_production, production], ignore_index=True)
            mapped_movement = pd.concat([mapped_movement, movement], ignore_index=True)
            mapped_procurement = pd.concat([mapped_procurement, procurement], ignore_index=True)
            mapped_capacity = pd.concat([mapped_capacity, capacity], ignore_index=True)

            print(f'\nOrder: {order_id} ({label}) has been mapped.')

            # update mapped sales
            order = order.to_frame().T
            mapped_sales = pd.concat([mapped_sales, order])
            mapped_sales['unsatisfied_demand'] = mapped_sales['quantity'] - mapped_sales['solutionvalue']

    # save the results
    filepath = f'results/resource_mapped_results_{config.time_direction}_{config.priority}.xlsx'
    with pd.ExcelWriter(filepath) as writer:
        mapped_sales[
            [
                'keys',
                'location',
                'product',
                'client',
                'quantity',
                'solutionvalue',
                'unsatisfied_demand',
                'period',
                'price',
                'total_price',
                'residual'
            ]
        ].to_excel(writer, sheet_name='mapped_sales')
        mapped_stock[
            [
                'order_id',
                'label',
                'keys',
                'location',
                'product',
                'period',
                'solutionvalue',
                'initialstock',
                'period_spent',
                'residual',
                'ps_leftover',
                'spend',
                'store'
            ]
        ].to_excel(writer, sheet_name='mapped_stock', index=False)
        mapped_production[
            [
                'order_id',
                'label',
                'keys',
                'location',
                'product',
                'bomnum',
                'period',
                'solutionvalue',
                'leadtime',
                'leftover',
                'spend'
            ]
        ].to_excel(writer, sheet_name='mapped_production', index=False)
        mapped_movement[
            [
                'order_id',
                'label',
                'keys',
                'loc_from',
                'loc_to',
                'product',
                'period',
                'transport_type',
                'solutionvalue',
                'leadtime',
                'residual',
                'leftover',
                'spend'
            ]
        ].to_excel(writer, sheet_name='mapped_movement', index=False)
        mapped_procurement[
            [
                'keys',
                'order_id',
                'label',
                'location',
                'product',
                'period',
                'solutionvalue',
                'supplier',
                'leftover',
                'spend'
            ]
        ].to_excel(writer, sheet_name='mapped_procurement', index=False)
        mapped_capacity[
            [
                'order_id',
                'label',
                'location',
                'product',
                'bomnum',
                'resource',
                'capacity',
                'period',
                'leftover',
                'spend'
            ]
        ].to_excel(writer, sheet_name='mapped_capacity', index=False)

    filepath = f'results/resource_output_resources_{config.time_direction}_{config.priority}.xlsx'
    with pd.ExcelWriter(filepath) as writer:
        df_production.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_production', index=False)
        df_stock.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_stock', index=False)
        df_movement.to_excel(writer, sheet_name='output_movement', index=False)
        df_procurement.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_procurement', index=False)
        df_capacity.to_excel(writer, sheet_name='output_capacity', index=False)


if __name__ == '__main__':
    run_resource_mapper()
