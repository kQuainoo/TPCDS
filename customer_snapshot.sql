{% snapshot customer_snapshot %}
    {{
        config(
            target_database='dbt_tpcds',
            target_schema='intermediate',
            unique_key='CUSTOMER_SK',
            strategy='timestamp',
            updated_at='DATETIMESTAMP'
        )
    }}
    select * from {{ ref('stg_tpcds__customer') }} 
    -- ref function to reference a 'previous model'
{% endsnapshot %}
