{{
    config(
        materialized="table",
        unique_key=["state_id"]
    )
}}

select
STATE_ID,
STATE_ABBR,
STATE_NAME,
STATE_REGION
    
from {{ source('northwind', 'us_states') }}



