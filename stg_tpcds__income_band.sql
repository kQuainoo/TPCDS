select 
    IB_LOWER_BOUND AS LOWER_BOUND, 
    IB_INCOME_BAND_SK AS INCOME_BAND_SK, 
    IB_UPPER_BOUND AS UPPER_BOUND, 
    _AIRBYTE_NORMALIZED_AT AS DATETIMESTAMP
from {{source('tpcds','income_band')}}