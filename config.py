from decimal import Decimal

configid = -77
datasetid = -770
runid = 28
lead_time = True  # False makes lead times 0. Use with duration switched off in optimizer config
period = 0, 1, 2
threshold = Decimal('0.10')
time_direction = 'backward'
priority = 'revenue'
map_priority = {
    'stock': 1,
    'production': 0,
    'movement': 2,
    'procurement': 3
}