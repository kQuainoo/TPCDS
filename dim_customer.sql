{{ 
    config(
        materialized= 'incremental',
        unique_key= ['CUSTOMER_SK']
    )
}}

select cs.*, 
    ca.STREET_NAME, ca.SUITE_NUMBER, ca.STATE, ca.LOCATION_TYPE, ca.COUNTRY, 
    ca.ADDRESS_ID, ca.COUNTY, ca.STREET_NUMBER, ca.ZIP, ca.CITY, ca.STREET_TYPE, ca.GMT_OFFSET, 
    cd.DEP_EMPLOYED_COUNT, cd.CREDIT_RATING, cd.EDUCATION_STATUS, 
    cd.PURCHASE_ESTIMATE, cd.MARITAL_STATUS, cd.DEP_COLLEGE_COUNT, cd.GENDER,
    hd.BUY_POTENTIAL, hd.DEP_COUNT, hd.VEHICLE_COUNT, ib.LOWER_BOUND, ib.INCOME_BAND_SK, ib.UPPER_BOUND 
from {{ref('customer_snapshot')}} as cs
LEFT join {{ref('stg_tpcds__customer_address')}} as ca on ca.ADDRESS_SK = cs.CURRENT_ADDR_SK 
LEFT join {{ref('stg_tpcds__customer_demographics')}} as cd ON cd.DEMO_SK = cs.CURRENT_CDEMO_SK
LEFT join {{ref('stg_tpcds__household_demographics')}} as hd ON hd.DEMO_SK = cs.CURRENT_HDEMO
LEFT join {{ref('stg_tpcds__income_band')}} as ib ON ib.INCOME_BAND_SK = hd.INCOME_BAND_SK 
WHERE cs.dbt_valid_from is null --comment out where clause for initial load