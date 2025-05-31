{{ 
    config(
        materialized= 'incremental',
        unique_key= ['item_sk', 'ship_date_sk','ship_addr_sk','ship_mode_sk']
    ) 
}}

with initial_sales_cte as (
    select 
        item_sk,
        ship_date_sk,
        ship_addr_sk,
        ship_mode_sk,
        sum(ext_ship_cost) as ship_cost,
        sum(quantity) as quantity,
        sum(net_profit) as net_profit
    from {{ref('stg_tpcds__web_sales')}}
    WHERE QUANTITY IS NOT null
    group by item_sk,ship_date_sk,ship_addr_sk,ship_mode_sk
),

secondcte_aggregation as (
    select 
        *,
        date.yr_num as shipped_year,
        date.mnth_num as shipped_month,
        date.yr_mnth_num as shipped_year_month
    from initial_sales_cte
    left join {{ref('stg_tpcds__date_dim')}} date
    on date.date_sk = ship_date_sk
)
select 
        item_sk,
        ship_date_sk,
        ship_addr_sk,
        ship_mode_sk,
        sum(ship_cost) as ship_cost,
        sum(quantity) as quantity,
        sum(net_profit) as net_profit,
        max(shipped_year) as shipped_year,
        max(shipped_month) as shipped_month,
        max(shipped_year_month) as shipped_year_month
from secondcte_aggregation
{% if is_incremental() %}
WHERE ship_date_sk >= (select max(ship_date_sk) from {{this}})
{% endif %} 
group by item_sk,ship_date_sk,ship_addr_sk,ship_mode_sk
order by item_sk,ship_date_sk,ship_addr_sk,ship_mode_sk