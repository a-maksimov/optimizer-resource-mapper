import pandas as pd
from tqdm import tqdm
from resource_mapper import map_resources
import config
import utils
from data_loader import data_loader


def process_mapped_resources(mapped_resources, df_demand):
    """ Calculates additional columns and creates the summary table """

    # unpack the mapped resources
    mapped_sales, mapped_stock, mapped_production, mapped_movement, mapped_procurement, mapped_capacity = mapped_resources

    # drop unnecessary columns
    mapped_sales = mapped_sales.drop(['loc_to', 'loc_from', 'operation_type', 'residual', 'keys'], axis=1)

    summary_table = pd.merge(df_demand, mapped_sales,
                             on=['location', 'product', 'client', 'period', 'price', 'quantity']).sort_values(
        'order_id')

    summary_table['config_id'] = config.configid
    summary_table['dataset_id'] = config.datasetid
    summary_table['run_id'] = config.runid

    summary_table = summary_table.rename(
        columns={
            'location': 'demand_location',
            'period': 'demand_period',
            'product': 'demand_product',
            'client': 'demand_client',
            'price': 'demand_price',
            'quantity': 'demand_volume',
            'solutionvalue': 'results_sale',
            'total_cost': 'cost_of_demand'
        }
    )

    # calculate the unsatisfied demand
    summary_table['unsatisfied_demand'] = summary_table['demand_volume'] - summary_table['results_sale']

    # calculate the unit cost
    summary_table['cost_per_unit_order'] = summary_table['cost_of_demand'] / summary_table['results_sale']

    # calculate margin of the unit price
    summary_table['margin_per_unit'] = summary_table['demand_price'] - summary_table['cost_per_unit_order']

    # drop unnecessary columns
    mapped_stock = mapped_stock.drop(['loc_to', 'loc_from', 'residual', 'period_spent', 'extra_res', 'is_leftover',
                                      'sv_leftover', 'ps_leftover', 'er_leftover', 'keys', 'label'], axis=1)
    mapped_production = mapped_production.drop(['loc_to', 'loc_from', 'residual', 'leftover', 'keys', 'label'], axis=1)
    mapped_movement = mapped_movement.drop(['residual', 'leftover', 'keys', 'label'], axis=1)
    mapped_procurement = mapped_procurement.drop(['loc_to', 'loc_from', 'residual', 'leftover', 'keys', 'label'],
                                                 axis=1)
    mapped_capacity = mapped_capacity.drop(['label', 'leftover'], axis=1)

    # pack the mapped resources
    mapped_resources = [mapped_stock, mapped_production, mapped_movement, mapped_procurement, mapped_capacity]

    # create a list of merged tables
    summary_tables = [summary_table.merge(mapped_resource, on=['order_id']) for mapped_resource in mapped_resources]

    # concatenate the list of merged tables
    summary_table = pd.concat(summary_tables, ignore_index=True)

    # final renames
    summary_table = summary_table.rename(
        columns={
            'solutionvalue': 'total_operation_volume',
            'var_production_cons': 'resource_consumption_per_unit'
        }
    )

    return summary_table


def calculate_cost(mapped_resources):
    """ Calculates the costs of the mapped results """

    # unpack the mapped resources
    mapped_sales, mapped_stock, mapped_movement, mapped_procurement, mapped_capacity = mapped_resources

    # fill stock nan spend and store with 0
    mapped_stock = mapped_stock.fillna(0)

    # calculate total costs
    mapped_stock['cost_of_allocated'] = -mapped_stock['cost'] * mapped_stock['store'] * mapped_stock['coefficient']
    mapped_movement['cost_of_allocated'] = -mapped_movement['cost'] * mapped_movement['oder_operation_volume'] * \
                                           mapped_procurement['coefficient']
    mapped_procurement['cost_of_allocated'] = -mapped_procurement['cost'] * mapped_procurement[
        'oder_operation_volume'] * mapped_procurement['coefficient']
    mapped_capacity['cost_of_allocated'] = -mapped_capacity['cost'] * mapped_capacity['oder_operation_volume'] * \
                                           mapped_capacity['coefficient']
    mapped_sales['cost_of_demand'] = 0

    # calculate total cost for each order in the operation
    cost_stocks = mapped_stock.groupby('label')['cost_of_allocated'].sum()
    cost_movement = mapped_movement.groupby('label')['cost_of_allocated'].sum()
    cost_procurement = mapped_procurement.groupby('label')['cost_of_allocated'].sum()
    cost_capacity = mapped_capacity.groupby('label')['cost_of_allocated'].sum()

    cost_list = [cost_stocks, cost_movement, cost_procurement, cost_capacity]

    # calculate the cost of demand for this order if the order is in the mapped table
    for order in pd.unique(mapped_sales['keys']):
        cost_of_demand = sum(cost.loc[order] for cost in cost_list if order in cost.index)
        mapped_sales.loc[mapped_sales['keys'] == order, 'cost_of_demand'] = cost_of_demand

    return mapped_sales, mapped_stock, mapped_movement, mapped_procurement, mapped_capacity


def run_resource_mapper():
    pd.set_option('display.float_format', lambda x: '%.3f' % x)

    # load data
    df_sales, df_stock, df_production, df_movement, df_procurement, df_bom, df_capacity, df_demand = data_loader(
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
        order['order_id'] = order_id
        mapped_sales = pd.concat([mapped_sales, order])

    # pack the resources
    mapped_resources = mapped_sales, mapped_stock, mapped_movement, mapped_procurement, mapped_capacity

    # calculate the costs
    mapped_sales, mapped_stock, mapped_movement, mapped_procurement, mapped_capacity = calculate_cost(mapped_resources)

    # pack the resources
    mapped_resources = mapped_sales, mapped_stock, mapped_production, mapped_movement, mapped_procurement, mapped_capacity

    # calculate the summary_table
    summary_table = process_mapped_resources(mapped_resources, df_demand)

    # export the summary table
    utils.export_summary_table(summary_table)


if __name__ == '__main__':
    run_resource_mapper()
