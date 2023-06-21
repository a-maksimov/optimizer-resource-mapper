import pandas as pd
import config


def export_input_resources(df_results_sale, df_results_stock, df_results_production, df_results_movement,
                           df_results_procurement):
    """ Exports input resources before mapping into Excel spreadsheet """

    filepath = f'input/resource_mapper_input_{config.time_direction}_{config.priority}.xlsx'

    with pd.ExcelWriter(filepath) as writer:
        df_results_sale.to_excel(writer, sheet_name='sales')
        df_results_stock.to_excel(writer, sheet_name='stock', index=False)
        df_results_production.to_excel(writer, sheet_name='production', index=False)
        df_results_movement.to_excel(writer, sheet_name='movement', index=False)
        df_results_procurement.to_excel(writer, sheet_name='procurement', index=False)


def export_mapped_resources(mapped_sales, mapped_stock, mapped_production, mapped_movement, mapped_procurement,
                            mapped_capacity):
    """ Exports mapped resources into Excel spreadsheet """

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
                'oder_operation_volume',
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
                'oder_operation_volume'
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
                'oder_operation_volume',
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
                'oder_operation_volume',
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
                'prod_quantity',
                'var_production_cons',
                'period',
                'leftover',
                'oder_operation_volume',
                'coefficient',
                'cost',
                'total_cost'
            ]
        ].to_excel(writer, sheet_name='mapped_capacity', index=False)


def export_output_resources(df_stock, df_production, df_movement, df_procurement, df_capacity):
    """ Exports resources after mapping into Excel file """

    filepath = f'results/resource_output_resources_{config.time_direction}_{config.priority}.xlsx'

    with pd.ExcelWriter(filepath) as writer:
        df_stock.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_stock', index=False)
        df_production.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_production', index=False)
        df_movement.to_excel(writer, sheet_name='output_movement', index=False)
        df_procurement.drop(['loc_from', 'loc_to'], axis=1).to_excel(writer, sheet_name='output_procurement',
                                                                     index=False)
        df_capacity.to_excel(writer, sheet_name='output_capacity', index=False)


def export_summary_table(summary_table):
    """ Exports the summary table into Excel spreadsheet """
    filepath = f'results/marking_demand.xlsx'

    with pd.ExcelWriter(filepath) as writer:
        summary_table.to_excel(writer, sheet_name='summary_table', index=False)
