import pandas as pd


# TODO: take into account periods, procurement, costs, leftovers, parametrize, output the order with residual
def map_resources(order, index, label, df_production, df_stock, df_movement, df_procurement, df_bom):
    """
    Recursively maps the resources in df_production, df_stock and df_movement with index of sale in order
    :param order: pd.Series, sale specification
    :param index: index of sale
    :param label: label of sale
    :param df_production: pd.DataFrame, data related to production
    :param df_stock: pd.Dataframe, data related to storage
    :param df_movement: pd.Dataframe, data related to transportation
    :param df_procurement: pd.Dataframe, data related to procurement
    :param df_bom: pd.Dataframe with BOMs data
    :return: tuple of pd.Dataframes: updated and mapped resources
            (df_production, df_stock, df_movement, df_procurement,
             mapped_production, mapped_stock, mapped_movement, mapped_procurement)
    """
    # create empty dataframes for filling with mapped data
    mapped_production = pd.DataFrame({'order_id': [], 'label': [], 'location': [], 'product': [], 'period': [],
                                      'value': [], 'spent': [], 'loc_from': [], 'loc_to': [], 'type': [],
                                      'leftover': []})

    mapped_stock = pd.DataFrame({'order_id': [], 'label': [], 'location': [], 'product': [], 'period': [],
                                 'solutionvalue': [], 'initialstock': [], 'value': [], 'spent': [], 'loc_from': [],
                                 'loc_to': [], 'type': [], 'residual': [], 'leftover': []})

    mapped_movement = pd.DataFrame({'order_id': [], 'label': [], 'product': [], 'period': [], 'transport_type': [],
                                    'value': [], 'spent': [], 'loc_from': [], 'loc_to': [], 'type': [], 'residual': [],
                                    'leftover': []})

    mapped_procurement = pd.DataFrame({'order_id': [], 'label': [], 'product': [], 'period': [], 'transport_type': [],
                                       'value': [], 'spent': [], 'loc_from': [], 'loc_to': [], 'type': [],
                                       'leftover': []})

    # initialize order residual and leftover counting
    order['residual'] = order['value']
    order['leftover'] = order['value']

    # to suppress taking the resource from itself
    compare_cols = ['value', 'leftover']

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
        df_product_bom['value'] = -df_product_bom['input_output'] * product_production['value']
        df_product_bom['residual'] = df_product_bom['value']
        df_product_bom['leftover'] = df_product_bom['value']
        df_product_bom['type'] = 'bom'

        return df_product_bom

    def find_resources(order, index, label, df_production, df_stock, df_movement, df_procurement,
                       mapped_production, mapped_stock, mapped_movement, mapped_procurement, df_bom):

        # base case
        if order['residual'] < 1:
            # convert order to dataframe
            results = df_production, df_stock, df_movement, df_procurement, \
                mapped_production, mapped_stock, mapped_movement, mapped_procurement
            return results

        else:
            # find a product at the same location and same period in production
            df_product_production = df_production[
                (df_production['product'] == order['product']) &
                (df_production['period'] == order['period']) &
                (df_production['loc_to'] == order['loc_from']) &
                (df_production['leftover'] > 0) &
                # suppress selection from self leftovers
                (~df_production[compare_cols].eq(order[compare_cols]).all(axis=1) |
                 (df_production['type'] != order['type']))
                ].copy()

            # recursive case 1
            # check production in this location
            if len(df_product_production) > 0:
                # iterate over found productions
                recursive_results = []
                for i in range(len(df_product_production)):
                    # break loop if there is no more residual
                    if order['residual'] < 1:
                        break

                    # get production row from the found products
                    product_production = df_product_production.iloc[i].copy()

                    # set the name of the Series to the index-label of the row
                    product_production.name = df_product_production.index[i]

                    # map product in production
                    product_production['order_id'] = index
                    product_production['label'] = label

                    # subtract production leftover from order
                    order['residual'] -= product_production['leftover']

                    # calculate the order residual and resource spent and leftover
                    if order['residual'] < 1:
                        product_production['spent'] = product_production['leftover'] + order['residual']
                        product_production['leftover'] = -order['residual']
                        order['residual'] = 0
                    else:
                        product_production['spent'] = product_production['leftover']
                        product_production['leftover'] = 0

                    # update mapped residual
                    if order['type'] == 'stock':
                        mapped_stock.loc[len(mapped_stock) - 1, 'residual'] = order['residual']
                        df_stock.loc[order.name, 'residual'] = order['residual']
                    elif order['type'] == 'movement':
                        mapped_movement.loc[len(mapped_movement) - 1, 'residual'] = order['residual']
                        df_movement.loc[order.name, 'residual'] = order['residual']

                    # append to a mapped dataframe
                    mapped_production.loc[len(mapped_production.index)] = product_production

                    # update production leftover
                    df_production.loc[product_production.name, 'leftover'] = product_production['leftover']

                    # # get BOM
                    # df_bomlist = get_bom_orders(product_production, df_bom)
                    # # map BOM
                    # bom_recursive_results = []
                    # for j in range(len(df_bomlist)):
                    #     # get the BOM item from found BOM list
                    #     product_bom_item = df_bomlist.iloc[i].copy()
                    #
                    #     # set the name of the Series to the index-label of the row
                    #     product_bom_item.name = df_bomlist.index[i]
                    #
                    #     # capture the recursive results
                    #     bom_recursive_results.append(find_resources(product_bom_item, index, label,
                    #                                                 df_production, df_stock, df_movement, df_procurement,
                    #                                                 mapped_production, mapped_stock, mapped_movement,
                    #                                                 mapped_procurement, df_bom))
                    #
                    #     # capture the BOM recursive results
                    #     bom_recursive_result = bom_recursive_results[-1]
                    #     return bom_recursive_result

                    # capture the recursive results
                    recursive_results.append(find_resources(order, index, label,
                                                            df_production, df_stock, df_movement, df_procurement,
                                                            mapped_production, mapped_stock, mapped_movement,
                                                            mapped_procurement, df_bom))

                # process the list of recursive results
                recursive_result = recursive_results[-1]
                return recursive_result

            # check stock
            df_product_stock = df_stock[
                (df_stock['product'] == order['product']) &
                (df_stock['period'] <= order['period']) &
                # movement from current period - 1
                (df_stock['period'] >= order['period'] - 1) &
                (df_stock['loc_to'] == order['loc_from']) &
                (df_stock['leftover'] > 0) &
                # suppress selection from self leftovers
                (~df_stock[compare_cols].eq(order[compare_cols]).all(axis=1) |
                 (df_stock['type'] != order['type']))
                ].copy()

            # recursive case 2
            # check stock in this location
            if len(df_product_stock) > 0:
                # iterate over found stocks
                recursive_results = []
                for i in range(len(df_product_stock)):
                    # break loop if there is no more residual
                    if order['residual'] < 1:
                        break
                    # get the stock row from the found stocks
                    product_stock = df_product_stock.iloc[i].copy()

                    # set the name of the Series to the label of the row
                    product_stock.name = df_product_stock.index[i]

                    # map product in production
                    product_stock['order_id'] = index
                    product_stock['label'] = label

                    # subtract production from order
                    order['residual'] -= product_stock['leftover']

                    # calculate the order residual and resource spent and leftover
                    if order['residual'] < 1:
                        product_stock['spent'] = product_stock['leftover'] + order['residual']
                        product_stock['leftover'] = -order['residual']
                        order['residual'] = 0
                    else:
                        product_stock['spent'] = product_stock['leftover']
                        product_stock['leftover'] = 0

                    # update mapped residual
                    if order['type'] == 'stock':
                        mapped_stock.loc[len(mapped_stock) - 1, 'residual'] = order['residual']
                        df_stock.loc[order.name, 'residual'] = order['residual']
                    elif order['type'] == 'movement':
                        mapped_movement.loc[len(mapped_movement) - 1, 'residual'] = order['residual']
                        df_movement.loc[order.name, 'residual'] = order['residual']

                    # if it's only initial stock, return on top level,
                    # if it is mixed or solution stock, go down level
                    if not product_stock['initialstock'] == product_stock['value']:
                        order = product_stock

                    # append to a mapped dataframe
                    mapped_stock.loc[len(mapped_stock.index)] = product_stock

                    # update resource leftover
                    df_stock.loc[product_stock.name, 'leftover'] = product_stock['leftover']

                    # capture the recursive results
                    recursive_results.append(find_resources(order, index, label,
                                                            df_production, df_stock, df_movement, df_procurement,
                                                            mapped_production, mapped_stock, mapped_movement,
                                                            mapped_procurement, df_bom))
                # process the list of recursive results
                recursive_result = recursive_results[-1]
                return recursive_result

            # check movement
            df_product_movement = df_movement[
                (df_movement['product'] == order['product']) &
                (df_movement['period'] <= order['period']) &
                # movement from current period - 1
                (df_movement['period'] >= order['period'] - 1) &
                (df_movement['loc_to'] == order['loc_from']) &
                (df_movement['leftover'] > 0) &
                # suppress selection from self leftovers
                (~df_movement[compare_cols].eq(order[compare_cols]).all(axis=1) |
                 (df_movement['type'] != order['type']))
                ].copy()

            # recursive case 3
            # check movement to this location
            if len(df_product_movement) > 0:
                # iterate over found movements
                recursive_results = []
                for i in range(len(df_product_movement)):
                    # break loop if there is no more residual
                    if order['residual'] < 1:
                        break

                    # get the movement row from the found movements
                    product_movement = df_product_movement.iloc[i].copy()

                    # set the name of the Series to the label of the row
                    product_movement.name = df_product_movement.index[i]

                    # map movement
                    product_movement['order_id'] = index
                    product_movement['label'] = label

                    # subtract movement value from order
                    order['residual'] -= product_movement['leftover']

                    # calculate the order residual and resource spent and leftover
                    if order['residual'] < 1:
                        product_movement['spent'] = product_movement['leftover'] + order['residual']
                        product_movement['leftover'] = -order['residual']
                        order['residual'] = 0
                    else:
                        product_movement['spent'] = product_movement['leftover']
                        product_movement['leftover'] = 0

                    # update mapped residual
                    if order['type'] == 'stock':
                        mapped_stock.loc[len(mapped_stock) - 1, 'residual'] = order['residual']
                        df_stock.loc[order.name, 'residual'] = order['residual']
                    elif order['type'] == 'movement':
                        mapped_movement.loc[len(mapped_movement) - 1, 'residual'] = order['residual']
                        df_movement.loc[order.name, 'residual'] = order['residual']

                    # append to a mapped dataframe
                    mapped_movement.loc[len(mapped_movement.index)] = product_movement

                    # update resource leftover
                    df_movement.loc[product_movement.name, 'leftover'] = product_movement['leftover']

                    # capture the recursive results
                    recursive_results.append(find_resources(product_movement, index, label,
                                                            df_production, df_stock, df_movement, df_procurement,
                                                            mapped_production, mapped_stock, mapped_movement,
                                                            mapped_procurement, df_bom))

                # process the list of recursive results
                recursive_result = recursive_results[-1]
                return recursive_result

            # find a product at the same location and same period in procurement
            df_product_procurement = df_procurement[
                (df_procurement['product'] == order['product']) &
                (df_procurement['period'] == order['period']) &
                (df_procurement['loc_to'] == order['loc_from']) &
                (df_procurement['leftover'] > 0) &
                # suppress selection from self leftovers
                (~df_procurement[compare_cols].eq(order[compare_cols]).all(
                    axis=1) |
                 (df_procurement['type'] != order['type']))
                ].copy()

            # recursive case 4
            # check production in this location
            if len(df_product_procurement) > 0:
                # iterate over found productions
                recursive_results = []
                for i in range(len(df_product_procurement)):
                    # break loop if there is no more residual
                    if order['residual'] < 1:
                        break
                    # get the procurement row from the found procurement
                    product_procurement = df_product_procurement.iloc[i].copy()

                    # set the name of the Series to the index-label of the row
                    product_procurement.name = df_product_procurement.index[i]

                    # map product in procurement
                    product_procurement['order_id'] = index
                    product_procurement['label'] = label

                    # subtract procurement leftover from order
                    order['residual'] -= product_procurement['leftover']

                    # calculate the order residual and resource spent and leftover
                    if order['residual'] < 1:
                        product_procurement['spent'] = product_procurement['leftover'] + order['residual']
                        product_procurement['leftover'] = -order['residual']
                        order['residual'] = 0
                    else:
                        product_procurement['spent'] = product_procurement['leftover']
                        product_procurement['leftover'] = 0

                    # update mapped residual
                    if order['type'] == 'stock':
                        mapped_stock.loc[len(mapped_stock) - 1, 'residual'] = order['residual']
                        df_stock.loc[order.name, 'residual'] = order['residual']
                    elif order['type'] == 'movement':
                        mapped_movement.loc[len(mapped_movement) - 1, 'residual'] = order['residual']
                        df_movement.loc[order.name, 'residual'] = order['residual']

                    # append to a mapped dataframe
                    mapped_procurement.loc[len(mapped_procurement.index)] = product_procurement

                    # update procurement leftover
                    df_procurement.loc[product_procurement.name, 'leftover'] = product_procurement['leftover']

                    # capture the recursive results
                    recursive_results.append(find_resources(order, index, label,
                                                            df_production, df_stock, df_movement, df_procurement,
                                                            mapped_production, mapped_stock, mapped_movement,
                                                            mapped_procurement, df_bom))

                # process the list of recursive results
                recursive_result = recursive_results[-1]
                return recursive_result

            return df_production, df_stock, df_movement, df_procurement, \
                mapped_production, mapped_stock, mapped_movement, mapped_procurement

    return find_resources(order, index, label,
                          df_production, df_stock, df_movement, df_procurement,
                          mapped_production, mapped_stock, mapped_movement, mapped_procurement, df_bom)

