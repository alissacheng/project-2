{{
    config(
        materialized="table",
        unique_key=["EMPLOYEE_ID"]
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
TITLE_OF_COURTESY
    
from {{ source('northwind', 'employees') }}
