select 
    WS_ORDER_NUMBER AS ORDER_NUMBER, 
    WS_SHIP_MODE_SK AS SHIP_MODE_SK, 
    WS_SOLD_TIME_SK AS SOLD_TIME_SK, 
    WS_SHIP_ADDR_SK AS SHIP_ADDR_SK, 
    WS_NET_PAID_INC_TAX AS NET_PAID_INC_TAX, 
    WS_PROMO_SK AS PROMO_SK, 
    WS_NET_PAID_INC_SHIP AS NET_PAID_INC_SHIP, 
    WS_QUANTITY AS QUANTITY, 
    WS_EXT_SALES_PRICE AS EXT_SALES_PRICE, 
    WS_WEB_SITE_SK AS WEB_SITE_SK, 
    WS_SHIP_DATE_SK AS SHIP_DATE_SK, 
    WS_BILL_CDEMO_SK AS BILL_CDEMO, 
    WS_NET_PROFIT AS NET_PROFIT, 
    WS_NET_PAID AS NET_PAID, 
    WS_EXT_SHIP_COST AS EXT_SHIP_COST, 
    WS_BILL_CUSTOMER_SK AS BILL_CUSTOMER_SK, 
    WS_COUPON_AMT AS COUPON_AMT, 
    WS_WHOLESALE_COST AS WHOLESALE_COST, 
    WS_SHIP_CUSTOMER_SK AS SHIP_CUSTOMER_SK, 
    WS_BILL_HDEMO_SK AS BILL_HDEMO_SK, 
    WS_SOLD_DATE_SK AS SOLD_DATE_SK, 
    WS_SHIP_CDEMO_SK AS SHIP_CDEMO_SK, 
    WS_WAREHOUSE_SK AS WAREHOUSE_SK, 
    WS_EXT_TAX AS EXT_TAX, 
    WS_ITEM_SK AS ITEM_SK, 
    WS_SHIP_HDEMO_SK AS SHIP_HDEMO_SK, 
    WS_EXT_WHOLESALE_COST AS EXT_WHOLESALE_COST, 
    WS_NET_PAID_INC_SHIP_TAX AS NET_PAID_INC_SHIP_TAX, 
    WS_EXT_DISCOUNT_AMT AS EXT_DISCOUNT_AMT, 
    WS_SALES_PRICE AS SALES_PRICE, 
    WS_WEB_PAGE_SK AS WEB_PAGE_SK, 
    WS_EXT_LIST_PRICE AS EXT_LIST_PRICE, 
    WS_LIST_PRICE AS LIST_PRICE, 
    WS_BILL_ADDR_SK AS BILL_ADDR_SK, 
    _AIRBYTE_NORMALIZED_AT AS DATETIMESTAMP 
from {{source('tpcds','web_sales')}}