import pandas as pd


# TODO: take into account periods, procurement, costs, leftovers, parametrize
def map_resources(order, index, df_production, df_stock, df_movement):
    """
    Recursively maps the resources in df_production, df_stock and df_movement with index of sale in order
    :param order: pd.Series with the sale specification
    :param index: index of the sale
    :param df_production: pd.DataFrame
    :param df_stock: pd.Dataframe
    :param df_movement: pd.Dataframe
    :return: tuple of updated pd.Dataframes: (df_production, df_stock, df_movement,
     mapped_production, mapped_stock, mapped_movement)
    """
    # create empty DataFrames to store the mapped resources
    mapped_production = pd.DataFrame()
    mapped_stock = pd.DataFrame()
    mapped_movement = pd.DataFrame()

    order['residual'] = order['solutionvalue']

    # initialize residual counting
    mapped_sales = order.to_frame().T
    mapped_sales['residual'] = mapped_sales['solutionvalue']

    # initialize leftover counting
    df_production['leftover'] = df_production['solutionvalue']
    df_stock['leftover'] = df_stock['solutionvalue']
    df_movement['leftover'] = df_movement['solutionvalue']

    def find_resources(order, index, df_production, df_stock, df_movement,
                       mapped_production, mapped_stock, mapped_movement, mapped_sales):

        # if the order hasn't been fulfilled
        if order['residual'] > 0:

            # find a product with the same location in production
            df_product_production = df_production[(df_production['product'] == order['product']) &
                                                  (df_production['period'] == order['period']) &
                                                  (df_production['location'] == order['location'])].copy()

            # find a product with the same location in stock
            df_product_stock = df_stock[(df_stock['product'] == order['product']) &
                                        (df_stock['period'] == order['period']) &
                                        (df_stock['location'] == order['location'])].copy()

            # check product movement
            df_product_movement = df_movement[(df_movement['product'] == order['product']) &
                                              (df_movement['period'] == order['period']) &
                                              (df_movement['loc_to'] == order['location'])].copy()

            # recursive case 1
            # check production in this location
            if len(df_product_production) > 0:
                # TODO: consider the case when multiple resources are available

                # iterate over found productions
                for i in range(len(df_product_production)):
                    # get the first production row from the found products
                    product_production = df_product_production.iloc[i].copy()

                    # set the name of the Series to the label of the row
                    product_production.name = df_product_production.index[0]

                    # map product in production
                    product_production['order_id'] = index

                    # subtract production from order
                    order['residual'] -= product_production['solutionvalue']

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
                        df_production.loc[product_production.name]['leftover'] = product_production['leftover']
                    else:
                        df_production = df_production.drop(product_production.name)

                    # capture the recursive results
                    recursive_results = find_resources(order, index, df_production, df_stock, df_movement,
                                                       mapped_production, mapped_stock, mapped_movement, mapped_sales)

                    return recursive_results

            # recursive case 2
            # check stock in this location
            elif len(df_product_stock) > 0:

                # iterate over found stock
                for i in range(len(df_product_stock)):
                    # get the first stock row from the found stocks
                    product_stock = df_product_stock.iloc[i].copy()

                    # set the name of the Series to the label of the row
                    product_stock.name = df_product_stock.index[0]

                    # map product in production
                    product_stock['order_id'] = index

                    # subtract production from order
                    order['residual'] -= product_stock['solutionvalue']

                    # calculate the order residual and resource leftover
                    if order['residual'] < 0:
                        product_stock['leftover'] = -order['residual']
                        order['residual'] = 0
                    else:
                        product_stock['leftover'] = 0

                    # convert series to df and append as a row
                    mapped_stock = pd.concat([mapped_production, product_stock.to_frame().T], axis=0)

                    # update resource leftover or drop
                    if product_stock['leftover'] > 0:
                        df_stock.loc[product_stock.name]['leftover'] = product_stock['leftover']
                    else:
                        df_stock = df_stock.drop(product_stock.name)

                    # capture the recursive results
                    recursive_results = find_resources(product_stock, index, df_production, df_stock, df_movement,
                                                       mapped_production, mapped_stock, mapped_movement, mapped_sales)

                    return recursive_results

            # recursive case 3
            # check movement to this location
            elif len(df_product_movement) > 0:
                # iterate over found movements
                for i in range(len(df_product_movement)):
                    # get the first movement row from the found movements
                    product_movement = df_product_movement.iloc[i].copy()

                    # set the name of the Series to the label of the row
                    product_movement.name = df_product_movement.index[0]

                    # map movement
                    product_movement['order_id'] = index

                    # subtract movement solutionvalue from order
                    order['residual'] -= product_movement['solutionvalue']

                    # calculate the order residual and resource leftover
                    if order['residual'] < 0:
                        product_movement['leftover'] = -order['residual']
                        order['residual'] = 0
                    else:
                        product_movement['leftover'] = 0

                    # convert series to df and append as a row
                    mapped_movement = pd.concat([mapped_movement, product_movement.to_frame().T], axis=0)

                    # update resource leftover or drop
                    if product_movement['leftover'] > 0:
                        df_movement.loc[product_movement.name]['leftover'] = product_movement['leftover']
                    else:
                        df_movement = df_movement.drop(product_movement.name)

                    # rename to be able to pass as order in recursive cases
                    product_movement = product_movement.rename({'loc_from': 'location'})

                    # initialize movement residual tracking
                    product_movement['residual'] = product_movement['solutionvalue']

                    # capture the recursive results
                    recursive_results = find_resources(product_movement, index, df_production, df_stock, df_movement,
                                                       mapped_production, mapped_stock, mapped_movement, mapped_sales)

                    return recursive_results

            else:
                print(f'\nProduct {order["product"]} for order_id {index} was not found')
                return []
        # if order residual <= 0
        else:
            print(f'\nOrder_id {index} for product {order["product"]} in location {order["location"]} was fulfilled.')

    return find_resources(order, index, df_production, df_stock, df_movement,
                          mapped_production, mapped_stock, mapped_movement, mapped_sales)
