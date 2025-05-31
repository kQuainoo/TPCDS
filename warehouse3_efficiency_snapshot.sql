{% snapshot warehouse3_efficiency_snapshot %}

    {{
        config(
          target_schema='marts',
          strategy='check',
          unique_key='warehouse_id',
          check_cols=['product_name','item_sk','pick_method','efficiency'],
        )
    }}

    select * from {{ source('dbt_tpcds', 'field_eng_data') }}

{% endsnapshot %}