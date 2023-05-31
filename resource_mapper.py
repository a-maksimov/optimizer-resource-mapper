import pandas as pd


# TODO: take into account periods, procurement, costs, calculate residuals, leftovers, parametrize
def map_resources(df_row, index, df_production, df_stock, df_movement):
    """
    Recursively maps the resources in df_production, df_stock and df_movement with index of sale in df_row
    :param df_row: pd.Series with the sale specification
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

    def find_resources(df_row, index, df_production, df_stock, df_movement,
                       mapped_production, mapped_stock, mapped_movement):

        # find a product with the same location in production
        df_product_production = df_production[(df_production['product'] == df_row['product']) &
                                              (df_production['location'] == df_row['location'])].copy()

        # find a product with the same location in stock
        df_product_stock = df_stock[(df_stock['product'] == df_row['product']) &
                                    (df_stock['location'] == df_row['location'])].copy()

        # check product movement
        df_product_movement = df_movement[(df_movement['product'] == df_row['product']) &
                                          (df_movement['loc_to'] == df_row['location'])].copy()

        # base case
        # check production in this location
        if len(df_product_production) > 0:
            # df_required_product should be a dataframe with 1 row
            # TODO: consider the case when multiple resources are available
            # map product in production
            df_product_production['order_id'] = index

            # append mapped production table
            mapped_production = pd.concat([mapped_production, df_product_production], ignore_index=True)

            # remove production row
            df_production = df_production.drop(df_product_production.index[0])

            # capture the results
            results = df_production, df_stock, df_movement, mapped_production, mapped_stock, mapped_movement

            return results

        # recursive case
        # check stock in this location
        elif len(df_product_stock) > 0:
            # map product in stock
            df_product_stock['order_id'] = index

            # append mapped stock table
            mapped_stock = pd.concat([mapped_stock, df_product_stock], ignore_index=True)

            # get product stock row
            product_stock = df_stock.loc[df_product_stock.index[0]]

            # remove stock row
            df_stock = df_stock.drop(df_product_stock.index[0])

            # capture the recursive results
            recursive_results = find_resources(product_stock, index, df_production, df_stock, df_movement,
                                               mapped_production, mapped_stock, mapped_movement)

            return recursive_results

        # recursive case
        # check movement to this location
        elif len(df_product_movement) > 0:
            # map movement
            df_product_movement['order_id'] = index

            # append mapped movement table
            mapped_movement = pd.concat([mapped_movement, df_product_movement], ignore_index=True)

            # get product movement row
            product_movement = df_movement.loc[df_product_movement.index[0]]

            # rename to be able to pass as df_row in recursive cases
            product_movement = product_movement.rename({'loc_from': 'location'})

            # remove movement row
            df_movement = df_movement.drop(df_product_movement.index[0])

            # capture the recursive results
            recursive_results = find_resources(product_movement, index, df_production, df_stock, df_movement,
                                               mapped_production, mapped_stock, mapped_movement)

            return recursive_results

        else:
            return print(f'product {df_row["product"]} for order_id {index} was not found')

    return find_resources(df_row, index, df_production, df_stock, df_movement,
                          mapped_production, mapped_stock, mapped_movement)
