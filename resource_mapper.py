import pandas as pd


def process_recursive_results(recursive_results: list):
    """
    Processes the list of mapping results
    :param recursive_results: list of tuples of pd.Dataframes
    :return: tuple pd.Dataframes
    """
    # get lists of updated resource dataframes
    df_productions = list(map(lambda res: res[0], recursive_results))
    df_stocks = list(map(lambda res: res[1], recursive_results))
    df_movements = list(map(lambda res: res[2], recursive_results))

    # select the smallest dataframe
    df_production = min(df_productions, key=lambda x: x['leftover'].sum())
    df_stock = min(df_stocks, key=lambda x: x['leftover'].sum())
    df_movement = min(df_movements, key=lambda x: x['leftover'].sum())

    # get lists of mapped resource dataframes
    mapped_productions = list(map(lambda res: res[3], recursive_results))
    mapped_stocks = list(map(lambda res: res[4], recursive_results))
    mapped_movements = list(map(lambda res: res[5], recursive_results))

    # select the longest paths
    mapped_production = max(mapped_productions, key=len)
    mapped_stock = max(mapped_stocks, key=len)
    mapped_movement = max(mapped_movements, key=len)

    return df_production, df_stock, df_movement, mapped_production, mapped_stock, mapped_movement


# TODO: take into account periods, procurement, costs, leftovers, parametrize, output the order with residual
def map_resources(order, index, df_production, df_stock, df_movement):
    """
    Recursively maps the resources in df_production, df_stock and df_movement with index of sale in order
    :param order: pd.Series with the sale specification
    :param index: index of the sale
    :param df_production: pd.DataFrame
    :param df_stock: pd.Dataframe
    :param df_movement: pd.Dataframe
    :return: tuple of pd.Dataframes: updated and mapped resources and updated order
    (df_production, df_stock, df_movement, mapped_production, mapped_stock, mapped_movement, updated_order)
    """
    # create empty DataFrames to store the mapped resources
    mapped_production = pd.DataFrame()
    mapped_stock = pd.DataFrame()
    mapped_movement = pd.DataFrame()

    # initialize leftover counting for resources
    df_production['leftover'] = df_production['value']
    df_stock['leftover'] = df_stock['value']
    df_movement['leftover'] = df_movement['value']

    # initialize tree
    order['residual'] = order['value']
    order['leftover'] = order['value']

    # to suppress taking resource from the same stock twice
    compare_cols = ['value', 'leftover']

    def find_resources(order, index, df_production, df_stock, df_movement, mapped_production,
                       mapped_stock, mapped_movement):

        # base case
        if order['residual'] <= 0:
            # convert order to dataframe
            results = df_production, df_stock, df_movement, mapped_production, mapped_stock, mapped_movement
            return results

        else:
            # check stock
            df_product_stock = df_stock[(df_stock['product'] == order['product']) &
                                        (df_stock['period'] <= order['period']) &
                                        # movement from current period - 1
                                        (df_stock['period'] >= order['period'] - 1) &
                                        (df_stock['loc_to'] == order['loc_from']) &
                                        # suppress selection from self leftovers
                                        (~df_stock[compare_cols].eq(order[compare_cols]).all(axis=1) |
                                         (df_stock['type'] != order['type']))].copy()

            # recursive case 1
            # check stock in this location
            if len(df_product_stock) > 0:
                # iterate over found stocks
                recursive_results = []
                for i in range(len(df_product_stock)):
                    # break loop if there is no more residual
                    if order['residual'] == 0:
                        break
                    # get the first stock row from the found stocks
                    product_stock = df_product_stock.iloc[i].copy()

                    # set the name of the Series to the label of the row
                    product_stock.name = df_product_stock.index[i]

                    # map product in production
                    product_stock['order_id'] = index

                    # subtract production from order
                    order['residual'] -= product_stock['value']

                    # calculate the order residual and resource leftover
                    if order['residual'] < 0:
                        product_stock['leftover'] = -order['residual']
                        order['residual'] = 0
                    else:
                        product_stock['leftover'] = 0

                    # if it's only initial stock, return on top level
                    if product_stock['period'] == 0 and product_stock['initialstock'] == product_stock['value']:
                        product_stock['residual'] = 0
                    # else go down the level and take only solutionvalue as residual (without initial stock)
                    else:
                        product_stock['residual'] = product_stock['solutionvalue']
                        order = product_stock

                    # convert series to df and append as a row
                    mapped_stock = pd.concat([mapped_stock, product_stock.to_frame().T], axis=0)

                    # update resource leftover or drop
                    if product_stock['leftover'] > 0:
                        df_stock.loc[product_stock.name, 'leftover'] = product_stock['leftover']
                    else:
                        df_stock = df_stock.drop(product_stock.name)

                    # capture the recursive results
                    recursive_results.append(find_resources(order, index, df_production, df_stock, df_movement,
                                                            mapped_production, mapped_stock, mapped_movement))
                # process the list of recursive results
                recursive_results = process_recursive_results(recursive_results)
                return recursive_results

            # find a product at the same location and same period in production
            df_product_production = df_production[(df_production['product'] == order['product']) &
                                                  (df_production['period'] == order['period']) &
                                                  (df_production['loc_to'] == order['loc_from']) &
                                                  # suppress selection from self leftovers
                                                  (~df_production[compare_cols].eq(order[compare_cols]).all(axis=1) |
                                                   (df_production['type'] != order['type']))].copy()
            # recursive case 2
            # check production in this location
            if len(df_product_production) > 0:
                # iterate over found productions
                recursive_results = []
                for i in range(len(df_product_production)):
                    # break loop if there is no more residual
                    if order['residual'] == 0:
                        break
                    # get the first production row from the found products
                    product_production = df_product_production.iloc[i].copy()

                    # set the name of the Series to the index-label of the row
                    product_production.name = df_product_production.index[i]

                    # map product in production
                    product_production['order_id'] = index

                    # subtract production from order
                    order['residual'] -= product_production['value']

                    # calculate the order residual and resource leftover
                    if order['residual'] < 0:
                        product_production['leftover'] = -order['residual']
                        order['residual'] = 0
                    else:
                        product_production['leftover'] = 0

                    # convert series to df and append as a row
                    mapped_production = pd.concat([mapped_production, product_production.to_frame().T], axis=0)

                    # update resource leftover or drop
                    if product_production['leftover'] > 0:
                        df_production.loc[product_production.name, 'leftover'] = product_production['leftover']
                    else:
                        df_production = df_production.drop(product_production.name)

                    # capture the recursive results
                    recursive_results.append(find_resources(order, index, df_production, df_stock, df_movement,
                                                            mapped_production, mapped_stock, mapped_movement))

                # process the list of recursive results
                recursive_results = process_recursive_results(recursive_results)
                return recursive_results

            # check movement
            df_product_movement = df_movement[(df_movement['product'] == order['product']) &
                                              (df_movement['period'] <= order['period']) &
                                              # movement from current period - 1
                                              (df_movement['period'] >= order['period'] - 1) &
                                              (df_movement['loc_to'] == order['loc_from']) &
                                              # suppress selection from self leftovers
                                              (~df_movement[compare_cols].eq(order[compare_cols]).all(axis=1) |
                                               (df_movement['type'] != order['type']))].copy()
            # recursive case 3
            # check movement to this location
            if len(df_product_movement) > 0:
                # iterate over found movements
                recursive_results = []
                for i in range(len(df_product_movement)):
                    # break loop if there is no more residual
                    if order['residual'] == 0:
                        break

                    # get the first movement row from the found movements
                    product_movement = df_product_movement.iloc[i].copy()

                    # set the name of the Series to the label of the row
                    product_movement.name = df_product_movement.index[i]

                    # map movement
                    product_movement['order_id'] = index

                    # subtract movement value from order
                    order['residual'] -= product_movement['value']

                    # calculate the order residual and resource leftover
                    if order['residual'] < 0:
                        product_movement['leftover'] = -order['residual']
                        order['residual'] = 0
                    else:
                        product_movement['leftover'] = 0

                    # initialize movement residual tracking
                    product_movement['residual'] = product_movement['value']

                    # convert series to df and append as a row
                    mapped_movement = pd.concat([mapped_movement, product_movement.to_frame().T], axis=0)

                    # update resource leftover or drop
                    if product_movement['leftover'] > 0:
                        df_movement.loc[product_movement.name, 'leftover'] = product_movement['leftover']
                    else:
                        df_movement = df_movement.drop(product_movement.name)

                    # capture the recursive results
                    recursive_results.append(find_resources(product_movement, index, df_production, df_stock,
                                                            df_movement, mapped_production, mapped_stock,
                                                            mapped_movement))

                # process the list of recursive results
                recursive_results = process_recursive_results(recursive_results)
                return recursive_results

            return df_production, df_stock, df_movement, mapped_production, mapped_stock, mapped_movement

    return find_resources(order, index, df_production, df_stock, df_movement, mapped_production, mapped_stock,
                          mapped_movement)
