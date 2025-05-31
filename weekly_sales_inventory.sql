{{ 
    config(
        materialized= 'incremental',
        unique_key= ['warehouse_sk', 'item_sk', 'sold_wk_sk']
    )
}}

with aggregating_daily_sales_to_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK,
    MIN(SOLD_DATE_SK) AS SOLD_WK_SK, --beginning of the week
    SOLD_WK_NUM,
    SOLD_YR_NUM, 
    SUM(DAILY_QTY) AS SUM_QTY_WK, 
    SUM(DAILY_SALES_AMT) AS SUM_AMT_WK, 
    SUM(DAILY_PROFIT) AS SUM_PROFIT_WK
FROM
    {{ref('int_sales__daily_aggregated_sales')}}
GROUP BY
    warehouse_sk,item_sk,sold_wk_num,sold_yr_num
),

finding_first_date_of_the_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    date.date_sk AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK
FROM
    aggregating_daily_sales_to_week as daily_sales
INNER JOIN {{ref('stg_tpcds__date_dim')}} as date --inner join to 'normalize' weeks 
-- that dont have sales on first date
on daily_sales.SOLD_WK_NUM=date.wk_num
and daily_sales.sold_yr_num=date.yr_num
and date.day_of_wk_num=0
),

populating_friday_date_for_the_week as (
SELECT 
    *,
    date.date_sk as friday_sk
FROM
    finding_first_date_of_the_week as ffw
INNER JOIN {{ref('stg_tpcds__date_dim')}} as date
on ffw.SOLD_WK_NUM=date.wk_num
and ffw.sold_yr_num=date.yr_num
and date.day_of_wk_num=5
)

select 
       warehouse_sk, 
       item_sk, 
       max(SOLD_WK_SK) as sold_wk_sk,
       sold_wk_num,
       sold_yr_num,
       sum(sum_qty_wk) as sum_qty_wk,
       sum(sum_amt_wk) as sum_amt_wk,
       sum(sum_profit_wk) as sum_profit_wk,
       sum(sum_qty_wk)/7 as avg_qty_dy,
       sum(nvl(inv.quantity_on_hand, 0)) as inv_qty_wk, 
       sum(nvl(inv.quantity_on_hand, 0)) / sum(sum_qty_wk) as wks_sply,
       iff(avg_qty_dy>0 and avg_qty_dy>inv_qty_wk, true , false) as low_stock_flg_wk
from populating_friday_date_for_the_week
left join {{ref('stg_tpcds__inventory')}} inv 
    on inv.inv_date_sk = friday_sk and item_sk = inv.inv_item_sk and inv.inv_warehouse_sk = warehouse_sk
group by warehouse_sk,item_sk,sold_wk_num,sold_yr_num,sold_wk_sk
-- extra precaution because to avoid negative or zero quantities in our final model
having sum(sum_qty_wk) > 0
{%- if is_incremental() %}
and sold_wk_sk >= (select max(nvl(sold_wk_sk,0)) from {{this}})
{%- endif %} 

--  inventory table gets populated every friday thus join with friday_sk