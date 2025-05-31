{{ 
    config(
        materialized= 'incremental',
        unique_key= ['warehouse_sk', 'item_sk', 'sold_date_sk']
    )
}}

with incremental_sales as (
SELECT 
        WAREHOUSE_SK,
        ITEM_SK,
        SOLD_DATE_SK,
        QUANTITY,
        sales_price * quantity as sales_amt,
        NET_PROFIT
    from {{ref('stg_tpcds__catalog_sales')}}
    WHERE 
    quantity is not null and sales_amt is not null -- to filter out bad data. business specified
    
    union --in order to get sales from both catalog and web

    SELECT 
            WAREHOUSE_SK,
            ITEM_SK,
            SOLD_DATE_SK,
            QUANTITY,
            sales_price * quantity as sales_amt,
            NET_PROFIT
    from {{ref('stg_tpcds__web_sales')}}
    WHERE 
    quantity is not null and sales_amt is not null
), 

aggregating_records_to_daily_sales as
(
select 
    warehouse_sk,
    item_sk,
    sold_date_sk, 
    sum(quantity) as daily_qty,
    sum(sales_amt) as daily_sales_amt,
    sum(net_profit) as daily_profit 
from incremental_sales
group by 1, 2, 3

),

adding_week_number_and_yr_number as
(
select 
    *,
    date.wk_num as sold_wk_num,
    date.yr_num as sold_yr_num
from aggregating_records_to_daily_sales 
LEFT JOIN {{ref('stg_tpcds__date_dim')}} date
    ON sold_date_sk = date.date_sk
)

SELECT 
	warehouse_sk,
    item_sk,
    sold_date_sk,
    max(sold_wk_num) as sold_wk_num,
    max(sold_yr_num) as sold_yr_num,
    sum(daily_qty) as daily_qty,
    sum(daily_sales_amt) as daily_sales_amt,
    sum(daily_profit) as daily_profit 
FROM adding_week_number_and_yr_number
{% if is_incremental() %}
WHERE sold_date_sk >= (select max(sold_date_sk) from {{this}})
{% endif %} 
--only grabbing records after max sold date, nvl zero if null 
GROUP BY warehouse_sk,item_sk,sold_date_sk
ORDER BY warehouse_sk,item_sk,sold_date_sk

-- incremental filter only applies if data exists in table