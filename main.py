import pandas as pd
from tqdm import tqdm
from resource_mapper import map_resources
import config
from data_loader import data_loader


def calculate_cost(mapped_resources):
    """ Calculates costs of the mapped results """
    # unpack the mapped resources
    mapped_sales, mapped_stock, mapped_movement, mapped_procurement, mapped_capacity = mapped_resources

    # calculate total costs
    mapped_stock['total_cost'] = -mapped_stock['cost'] * mapped_stock['store']
    mapped_movement['total_cost'] = -mapped_movement['cost'] * mapped_movement['spend']
    mapped_procurement['total_cost'] = -mapped_procurement['cost'] * mapped_procurement['spend'] * mapped_procurement[
        'coefficient']
    mapped_capacity['total_cost'] = -mapped_capacity['cost'] * mapped_capacity['spend']
    mapped_sales['total_cost'] = 0

    cost_stocks = mapped_stock.groupby('label')['total_cost'].sum()
    cost_movement = mapped_movement.groupby('label')['total_cost'].sum()
    cost_procurement = mapped_procurement.groupby('label')['total_cost'].sum()
    cost_capacity = mapped_capacity.groupby('label')['total_cost'].sum()

    # if the order is in the mapped table, add up its cost
    for order in pd.unique(mapped_sales['keys']):
        order_cost = 0
        if order in cost_stocks.index:
            order_cost += cost_stocks.loc[order]
        if order in cost_movement.index:
            order_cost += cost_movement.loc[order]
        if order in cost_procurement:
            order_cost += cost_procurement.loc[order]
        if order in cost_capacity:
            order_cost += cost_capacity.loc[order]
        # add up the cost
        mapped_sales.loc[mapped_sales['keys'] == order, 'total_cost'] = order_cost

    # calculate the unit cost
    mapped_sales['unit_cost'] = mapped_sales['total_cost'] / mapped_sales['solutionvalue']

    return mapped_sales, mapped_stock, mapped_movement, mapped_procurement, mapped_capacity


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

    # fill nan spend and store with 0
    mapped_stock = mapped_stock.fillna(0)

    # pack the resources
    mapped_resources = mapped_sales, mapped_stock, mapped_movement, mapped_procurement, mapped_capacity

    # calculate costs
    mapped_sales, mapped_stock, mapped_movement, mapped_procurement, mapped_capacity = calculate_cost(mapped_resources)

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
                'residual',
                'total_cost',
                'unit_cost'
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
                'store',
                'cost',
                'total_cost'
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
                'spend',
                'cost',
                'total_cost'
            ]
        ].to_excel(writer, sheet_name='mapped_movement', index=False)
        mapped_procurement[
            [
                'order_id',
                'label',
                'keys',
                'location',
                'product',
                'period',
                'solutionvalue',
                'supplier',
                'leftover',
                'spend',
                'coefficient',
                'cost',
                'total_cost'
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
                'spend',
                'coefficient',
                'cost',
                'total_cost'
            ]
        ].to_excel(writer, sheet_name='mapped_capacity', index=False)

    filepath = f'results/resource_output_resources_{config.time_direction}_{config.priority}.xlsx'
    with pd.ExcelWriter(filepath) as writer:
        df_production.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_production', index=False)
        df_stock.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_stock', index=False)
        df_movement.to_excel(writer, sheet_name='output_movement', index=False)
        df_procurement.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_procurement',
                                                                     index=False)
        df_capacity.to_excel(writer, sheet_name='output_capacity', index=False)


if __name__ == '__main__':
    run_resource_mapper()
