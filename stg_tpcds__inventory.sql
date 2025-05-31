select 
    INV_DATE_SK, 
    INV_ITEM_SK,
    INV_QUANTITY_ON_HAND AS QUANTITY_ON_HAND, 
    INV_WAREHOUSE_SK,
    current_timestamp as DATETIMESTAMP
from {{source('tpcds','inventory')}}