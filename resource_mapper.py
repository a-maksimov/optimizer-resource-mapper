import pandas as pd
import numpy as np
from decimal import Decimal


# TODO: take into account periods, procurement, costs, leftovers, parametrize, output the order with residual
def map_resources(order, order_id, label,
                  df_stock, df_production, df_movement, df_procurement, map_priority,
                  df_bom, map_bom, threshold=Decimal('0.1')):
    """
    Recursively maps the resources in df_production, df_stock and df_movement with index of sale in order
    :param order: pd.Series, sale specification
    :param order_id: index of sale
    :param label: label of sale
    :param df_production: pd.DataFrame, data related to production
    :param df_stock: pd.Dataframe, data related to storage
    :param df_movement: pd.Dataframe, data related to transportation
    :param df_procurement: pd.Dataframe, data related to procurement
    :param map_priority: dict, priority of resources
    :param df_bom: pd.Dataframe with BOMs data
    :param map_bom: bool, whether to map BOMs
    :param threshold: Decimal, threshold for comparing real numbers
    :return: tuple of pd.Dataframes: updated and mapped resources
            (df_production, df_stock, df_movement, df_procurement,
             mapped_production, mapped_stock, mapped_movement, mapped_procurement)
    """
    # create empty dataframes for mapping
    t = pd.DataFrame({'order_id': [], 'label': [], 'spend': [], 'store': []})
    mapped_stock = pd.DataFrame(columns=df_stock.columns)
    mapped_stock = pd.concat([mapped_stock, t])
    t = pd.DataFrame({'order_id': [], 'label': [], 'spend': []})
    mapped_production = pd.DataFrame(columns=df_production.columns)
    mapped_production = pd.concat([mapped_production, t])
    mapped_movement = pd.DataFrame(columns=df_movement.columns)
    mapped_movement = pd.concat([mapped_movement, t])
    mapped_procurement = pd.DataFrame(columns=df_procurement.columns)
    mapped_procurement = pd.concat([mapped_procurement, t])

    # pack the resources
    resources = df_stock, df_production, df_movement, df_procurement
    mapped_resources = mapped_stock, mapped_production, mapped_movement, mapped_procurement

    def update_mapped_residual(order, resources, mapped_resources):

        # unpack the resources
        df_stock, df_production, df_movement, df_procurement = resources
        mapped_stock, mapped_production, mapped_movement, mapped_procurement = mapped_resources

        # find the latest row with the desired 'keys' and update it's residual
        # also update the residual in the input tables
        if order['type'] == 'stock':
            last_index = mapped_stock.loc[mapped_stock['keys'] == order['keys']].index[-1]
            mapped_stock.loc[last_index, 'residual'] = order['residual']

        elif order['type'] == 'production':
            last_index = mapped_production.loc[mapped_production['keys'] == order['keys']].index[-1]
            mapped_production.loc[last_index, 'residual'] = order['residual']

        elif order['type'] == 'movement':
            last_index = mapped_movement.loc[mapped_movement['keys'] == order['keys']].index[-1]
            mapped_movement.loc[last_index, 'residual'] = order['residual']

        elif order['type'] == 'procurement':
            last_index = mapped_procurement.loc[mapped_procurement['keys'] == order['keys']].index[-1]
            mapped_procurement.loc[last_index, 'residual'] = order['residual']

        # pack the resources
        resources = df_stock, df_production, df_movement, df_procurement
        mapped_resources = mapped_stock, mapped_production, mapped_movement, mapped_procurement

        return resources, mapped_resources

    def map_stock(order, product_stock, resources, mapped_resources):

        # map the product in stock
        product_stock['order_id'] = order_id
        product_stock['label'] = label

        if order['type'] == product_stock['type']:
            order['residual'] -= product_stock['sv_leftover']
            if order['residual'] < threshold:
                product_stock['store'] = product_stock['sv_leftover'] + order['residual']
                product_stock['sv_leftover'] = -order['residual']
                order['residual'] = 0
            else:
                product_stock['store'] = product_stock['sv_leftover']
                product_stock['sv_leftover'] = 0
        else:
            order['residual'] -= product_stock['ps_leftover']
            if order['residual'] < threshold:
                product_stock['spend'] = product_stock['ps_leftover'] + order['residual']
                product_stock['ps_leftover'] = -order['residual']
                order['residual'] = 0
            else:
                product_stock['spend'] = product_stock['ps_leftover']
                product_stock['ps_leftover'] = 0

        # update residual
        resources, mapped_resources = update_mapped_residual(order, resources, mapped_resources)

        # unpack the resources
        df_stock, df_production, df_movement, df_procurement = resources
        mapped_stock, mapped_production, mapped_movement, mapped_procurement = mapped_resources

        if order['type'] == product_stock['type']:
            spend = product_stock['store']
        else:
            spend = product_stock['spend']

        if product_stock['er_leftover'] > threshold:
            # map extra resource
            product_stock['residual'] = product_stock['er_leftover'] - spend
            if product_stock['residual'] < threshold:
                product_stock['residual'] = product_stock['er_leftover']
                product_stock['er_leftover'] = 0
            else:
                product_stock['er_leftover'] = product_stock['residual']
                product_stock['residual'] = spend
        else:
            # take into account initial stock
            product_stock['residual'] = spend - product_stock['is_leftover']
            if product_stock['residual'] < threshold:
                product_stock['is_leftover'] -= spend
                product_stock['residual'] = 0
            else:
                product_stock['is_leftover'] = 0

        # append to a mapped dataframe
        mapped_stock.loc[len(mapped_stock.index)] = product_stock

        # update resource leftover for storage
        df_stock.loc[product_stock.name, 'ps_leftover'] = product_stock['ps_leftover']
        df_stock.loc[product_stock.name, 'sv_leftover'] = product_stock['sv_leftover']
        df_stock.loc[product_stock.name, 'is_leftover'] = product_stock['is_leftover']
        df_stock.loc[product_stock.name, 'er_leftover'] = product_stock['er_leftover']

        # pack the resources
        resources = df_stock, df_production, df_movement, df_procurement
        mapped_resources = mapped_stock, mapped_production, mapped_movement, mapped_procurement

        return product_stock, resources, mapped_resources

    def map_production(order, product_production, resources, mapped_resources):

        # map the product in production
        product_production['order_id'] = order_id
        product_production['label'] = label

        # subtract production leftover from order
        order['residual'] -= product_production['leftover']

        # calculate the order residual and resource spend and leftover
        if order['residual'] < threshold:
            product_production['spend'] = product_production['leftover'] + order['residual']
            product_production['leftover'] = -order['residual']
            order['residual'] = 0
        else:
            product_production['spend'] = product_production['leftover']
            product_production['leftover'] = 0

        # update residual
        resources, mapped_resources = update_mapped_residual(order, resources, mapped_resources)

        # unpack the resources
        df_stock, df_production, df_movement, df_procurement = resources
        mapped_stock, mapped_production, mapped_movement, mapped_procurement = mapped_resources

        # append to a mapped dataframe
        mapped_production.loc[len(mapped_production.index)] = product_production

        # update production leftover
        df_production.loc[product_production.name, 'leftover'] = product_production['leftover']

        # pack the resources
        resources = df_stock, df_production, df_movement, df_procurement
        mapped_resources = mapped_stock, mapped_production, mapped_movement, mapped_procurement

        return product_production, resources, mapped_resources

    def map_movement(order, product_movement, resources, mapped_resources):

        # map the product in movement
        product_movement['order_id'] = order_id
        product_movement['label'] = label

        # subtract movement value from order
        order['residual'] -= product_movement['leftover']

        # calculate the order residual and resource spend and leftover
        if order['residual'] < threshold:
            product_movement['spend'] = product_movement['leftover'] + order['residual']
            product_movement['leftover'] = -order['residual']
            order['residual'] = 0
        else:
            product_movement['spend'] = product_movement['leftover']
            product_movement['leftover'] = 0

        # update residual
        resources, mapped_resources = update_mapped_residual(order, resources, mapped_resources)

        # unpack the resources
        df_stock, df_production, df_movement, df_procurement = resources
        mapped_stock, mapped_production, mapped_movement, mapped_procurement = mapped_resources

        # update resource residual
        product_movement['residual'] = product_movement['spend']

        # append to a mapped dataframe
        mapped_movement.loc[len(mapped_movement.index)] = product_movement

        # update resource leftover
        df_movement.loc[product_movement.name, 'leftover'] = product_movement['leftover']

        # pack the resources
        resources = df_stock, df_production, df_movement, df_procurement
        mapped_resources = mapped_stock, mapped_production, mapped_movement, mapped_procurement

        return product_movement, resources, mapped_resources

    def map_procurement(order, product_procurement, resources, mapped_resources):

        # map the product in production
        product_procurement['order_id'] = order_id
        product_procurement['label'] = label

        # subtract movement value from order
        order['residual'] -= product_procurement['leftover']

        # calculate the order residual and resource spend and leftover
        if order['residual'] < threshold:
            product_procurement['spend'] = product_procurement['leftover'] + order['residual']
            product_procurement['leftover'] = -order['residual']
            order['residual'] = 0
        else:
            product_procurement['spend'] = product_procurement['leftover']
            product_procurement['leftover'] = 0

        # update residual
        resources, mapped_resources = update_mapped_residual(order, resources, mapped_resources)

        # unpack the resources
        df_stock, df_production, df_movement, df_procurement = resources
        mapped_stock, mapped_production, mapped_movement, mapped_procurement = mapped_resources

        # update resource residual
        product_procurement['residual'] = product_procurement['spend']

        # append to a mapped dataframe
        mapped_procurement.loc[len(mapped_procurement.index)] = product_procurement

        # update resource leftover
        df_procurement.loc[product_procurement.name, 'leftover'] = product_procurement['leftover']

        # pack the resources
        resources = df_stock, df_production, df_movement, df_procurement
        mapped_resources = mapped_stock, mapped_production, mapped_movement, mapped_procurement

        return product_procurement, resources, mapped_resources

    def get_bom_orders(product_production, df_bom):
        # get the BOM
        df_product_bom = df_bom[
            (df_bom['bomnum'] == product_production['bomnum']) &
            (df_bom['period'] == product_production['period']) &
            (df_bom['input_output'] < 0)
            ].copy()
        # form orders for supplies
        df_product_bom['order_id'] = product_production['order_id']
        df_product_bom['label'] = product_production['label']
        df_product_bom['loc_from'], df_product_bom['loc_to'] = df_product_bom['location'], df_product_bom['location']
        df_product_bom['solutionvalue'] = -df_product_bom['input_output'] * product_production['spend']
        df_product_bom['residual'] = df_product_bom['solutionvalue']
        df_product_bom['leftover'] = Decimal('0')
        df_product_bom['spend'] = df_product_bom['solutionvalue']
        df_product_bom['type'] = 'bom'

        return df_product_bom

    def find_resources(order, order_id, label, resources, mapped_resources, map_priority, df_bom, map_bom, threshold):

        # unpack the resources
        df_stock, df_production, df_movement, df_procurement = resources
        mapped_stock, mapped_production, mapped_movement, mapped_procurement = mapped_resources

        # base case
        if abs(order['residual']) < threshold:
            # convert order to dataframe
            result = df_stock, df_production, df_movement, df_procurement, \
                mapped_stock, mapped_production, mapped_movement, mapped_procurement
            return result

        # recursive cases
        else:
            # initialize list of branches
            df_list = []

            # check stock
            df_product_stock = df_stock[
                (df_stock['product'] == order['product']) &
                # if the order is stock, check prev period, otherwise check current stock leftovers
                (np.where(df_stock['type'] == order['type'],
                          df_stock['period'] == order['period'] - 1,
                          df_stock['period'] == order['period'])) &
                (df_stock['loc_to'] == order['loc_from']) &
                # if the order is stock, check previous period solutionvalue leftover,
                # otherwise check current period_spend leftover
                (np.where(df_stock['type'] == order['type'],
                          abs(df_stock['sv_leftover']) > threshold,
                          abs(df_stock['ps_leftover']) > threshold)) &
                # suppress selection from self leftovers
                (~df_stock.index.isin([order.name]) | (df_stock['type'] != order['type']))
                ].copy()
            if len(df_product_stock) > 0:
                df_list.append(df_product_stock)

            # find a product at the same location in production
            df_product_production = df_production[
                (df_production['product'] == order['product']) &
                (df_production['period'] == order['period'] - df_production['leadtime']) &
                (df_production['loc_to'] == order['loc_from']) &
                (abs(df_production['leftover']) > threshold) &
                # suppress selection from self
                (~df_production.index.isin([order.name]) | (df_production['type'] != order['type']))
                ].copy()
            if len(df_product_production) > 0:
                df_list.append(df_product_production)

            # check movement
            df_product_movement = df_movement[
                (df_movement['product'] == order['product']) &
                (df_movement['period'] == order['period'] - df_movement['leadtime']) &
                (df_movement['loc_to'] == order['loc_from']) &
                (abs(df_movement['leftover']) > threshold) &
                # suppress selection from self leftovers
                (~df_movement.index.isin([order.name]) | (df_movement['type'] != order['type']))
                ].copy()
            if len(df_product_movement) > 0:
                df_list.append(df_product_movement)

            # find a product at the same location and same period in procurement
            df_product_procurement = df_procurement[
                (df_procurement['product'] == order['product']) &
                (df_procurement['period'] <= order['period']) &
                (df_procurement['loc_to'] == order['loc_from']) &
                (abs(df_procurement['leftover']) > threshold) &
                # suppress selection from self leftovers
                (~df_procurement.index.isin([order.name]) | (df_procurement['type'] != order['type']))
                ].copy()
            if len(df_product_procurement):
                df_list.append(df_product_procurement)

            recursive_results = []
            # iterate over sorted list of branches
            df_list = sorted(df_list, key=lambda d: map_priority[d.iloc[0]['type']])
            for df in df_list:
                # break the loop if the order is fulfilled
                if abs(order['residual']) < threshold:
                    break

                else:
                    if df.iloc[0]['type'] == 'stock':
                        # iterate over found stocks
                        recursive_results_stock = []
                        for i in range(len(df)):
                            # break loop if there is no more residual
                            if abs(order['residual']) < threshold:
                                break
                            # get the stock row from the found stocks
                            product_stock = df.sort_values('period', ascending=False).iloc[i].copy()

                            # set the name of the Series to the label of the row
                            product_stock.name = df.index[i]

                            # pack the resources
                            resources = df_stock, df_production, df_movement, df_procurement
                            mapped_resources = mapped_stock, mapped_production, mapped_movement, mapped_procurement

                            # get mapped result
                            product_stock, resources, mapped_resources = map_stock(
                                order, product_stock,
                                resources, mapped_resources
                            )

                            # capture the recursive results
                            recursive_results_stock.append(
                                find_resources(
                                    product_stock, order_id, label,
                                    resources, mapped_resources, map_priority,
                                    df_bom, map_bom, threshold
                                )
                            )
                        # get the recursive result from the stock branch
                        recursive_results.append(recursive_results_stock[-1])

                    elif df.iloc[0]['type'] == 'production':
                        # iterate over found productions
                        recursive_results_production = []
                        for i in range(len(df)):
                            # break loop if there is no more residual
                            if abs(order['residual']) < threshold:
                                break
                            # get the stock row from the found stocks
                            product_production = df.sort_values('period', ascending=False).iloc[i].copy()

                            # set the name of the Series to the label of the row
                            product_production.name = df.index[i]

                            # pack the resources
                            resources = df_stock, df_production, df_movement, df_procurement
                            mapped_resources = mapped_stock, mapped_production, mapped_movement, mapped_procurement

                            # get mapped result
                            product_production, resources, mapped_resources = map_production(
                                order, product_production,
                                resources, mapped_resources
                            )
                            # map BOM
                            if map_bom:
                                # get BOM
                                df_bomlist = get_bom_orders(product_production, df_bom)
                                # if the product has inputs
                                if len(df_bomlist) > 0:
                                    # map BOM
                                    recursive_results_bom = []
                                    for j in range(len(df_bomlist)):
                                        # get the BOM item from found BOM list
                                        product_bom_item = df_bomlist.iloc[j].copy()

                                        # set the name of the Series to the index-label of the row
                                        product_bom_item.name = df_bomlist.index[j]

                                        # capture the recursive results
                                        recursive_results_bom.append(
                                            find_resources(
                                                product_bom_item, order_id, label,
                                                resources, mapped_resources, map_priority,
                                                df_bom, map_bom, threshold
                                            )
                                        )
                                    recursive_results_production.append(recursive_results_bom[-1])

                            # capture the recursive results
                            recursive_results_production.append(
                                find_resources(
                                    order, order_id, label,
                                    resources, mapped_resources, map_priority,
                                    df_bom, map_bom, threshold
                                )
                            )

                        # get the recursive result from the production branch
                        recursive_results.append(recursive_results_production[-1])

                    elif df.iloc[0]['type'] == 'movement':
                        # iterate over found movements
                        recursive_results_movement = []
                        for i in range(len(df)):
                            # break loop if there is no more residual
                            if abs(order['residual']) < threshold:
                                break
                            # get the stock row from the found stocks
                            product_movement = df.sort_values('period', ascending=False).iloc[i].copy()

                            # set the name of the Series to the label of the row
                            product_movement.name = df.index[i]

                            # pack the resources
                            resources = df_stock, df_production, df_movement, df_procurement
                            mapped_resources = mapped_stock, mapped_production, mapped_movement, mapped_procurement

                            # get mapped result
                            product_movement, resources, mapped_resources = map_movement(
                                order, product_movement,
                                resources, mapped_resources
                            )

                            # capture the recursive results
                            recursive_results_movement.append(
                                find_resources(
                                    product_movement, order_id, label,
                                    resources, mapped_resources, map_priority,
                                    df_bom, map_bom, threshold
                                )
                            )
                        # get the recursive result from the movement branch
                        recursive_results.append(recursive_results_movement[-1])

                    else:
                        # iterate over found procurements
                        recursive_results_procurement = []
                        for i in range(len(df)):
                            # break loop if there is no more residual
                            if abs(order['residual']) < threshold:
                                break
                            # get the stock row from the found stocks
                            product_procurement = df.sort_values('period', ascending=False).iloc[i].copy()

                            # set the name of the Series to the label of the row
                            product_procurement.name = df.index[i]

                            # pack the resources
                            resources = df_stock, df_production, df_movement, df_procurement
                            mapped_resources = mapped_stock, mapped_production, mapped_movement, mapped_procurement

                            # get mapped result
                            product_procurement, resources, mapped_resources = map_procurement(
                                order, product_procurement,
                                resources, mapped_resources
                            )

                            # capture the recursive results
                            recursive_results_procurement.append(
                                find_resources(
                                    order, order_id, label,
                                    resources, mapped_resources, map_priority,
                                    df_bom, map_bom, threshold
                                )
                            )
                        # get the recursive result from the procurement branch
                        recursive_results.append(recursive_results_procurement[-1])

            recursive_result = recursive_results[-1]

        return recursive_result

    return find_resources(order, order_id, label, resources, mapped_resources, map_priority, df_bom, map_bom, threshold)
