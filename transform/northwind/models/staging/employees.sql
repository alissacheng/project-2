{{
    config(
        materialized="incremental",
        unique_key=["EMPLOYEE_ID"],
        incremental_strategy="delete+insert"
    )
}}

select
CITY,
NOTES,
PHOTO,
TITLE,
REGION,
ADDRESS,
COUNTRY,
EXTENSION,
HIRE_DATE,
LAST_NAME,
BIRTH_DATE,
FIRST_NAME,
HOME_PHONE,
PHOTO_PATH,
REPORTS_TO,
EMPLOYEE_ID,
POSTAL_CODE,
TITLE_OF_COURTESY,
_airbyte_extracted_at as LAST_UPDATE
    
from {{ source('northwind', 'employees') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}