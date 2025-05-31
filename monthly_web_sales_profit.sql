{{ 
    config(
        materialized= 'incremental',
        unique_key= ['item_sk','ship_mnth_sk','ship_addr_sk','ship_mode_sk']
    )
}}

with aggregating_daily_sales_to_month as (
    select 
        item_sk,
        ship_addr_sk,
        ship_mode_sk,
        min(ship_date_sk) as ship_date_sk,
        sum(ship_cost) as ship_cost,
        sum(quantity) as quantity,
        sum(net_profit) as net_profit,
        max(shipped_year) as shipped_year,
        max(shipped_month) as shipped_month,
        shipped_year_month as ship_mnth_sk
    from {{ref('int_sales__daily_shipped_web_sales')}}
    group by item_sk,ship_mnth_sk,ship_addr_sk,ship_mode_sk
    ),

normalize_for_month as (
    select 
        *,
        date.date_sk as ship_date_sk
    from aggregating_daily_sales_to_month as monthly_sales
    inner join {{ref('stg_tpcds__date_dim')}} as date
    on monthly_sales.ship_mnth_sk = date.yr_mnth_num
)
select 
    item_sk,
    ship_addr_sk,
    ship_mode_sk,
    sum(ship_cost) as ship_cost,
    sum(quantity) as quantity,
    sum(net_profit) as net_profit,
    max(shipped_year) as shipped_year,
    max(shipped_month) as shipped_month,
    ship_mnth_sk
from normalize_for_month
{% if is_incremental() %}
    where ship_mnth_sk >= (select max(coalesce(ship_mnth_sk,0)) from {{this}})
{% endif %}
group by ship_mnth_sk,ship_addr_sk,ship_mode_sk,item_sk