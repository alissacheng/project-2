-- Employees Incremental Model
-- This dbt model represents an incremental view for the 'employees' source in the 'northwind' database.

-- Configuration:
-- - Materialized: Incremental
-- - Unique Key: ['EMPLOYEE_ID']
-- - Incremental Strategy: Delete + Insert

-- Columns:
-- - CITY: The city where the employee is located.
-- - NOTES: Additional notes or comments about the employee.
-- - PHOTO: The photo of the employee.
-- - TITLE: The job title of the employee.
-- - REGION: The region where the employee is located.
-- - ADDRESS: The address of the employee.
-- - COUNTRY: The country where the employee is located.
-- - EXTENSION: The phone extension of the employee.
-- - HIRE_DATE: The date when the employee was hired.
-- - LAST_NAME: The last name of the employee.
-- - BIRTH_DATE: The birth date of the employee.
-- - FIRST_NAME: The first name of the employee.
-- - HOME_PHONE: The home phone number of the employee.
-- - PHOTO_PATH: The file path of the employee's photo.
-- - REPORTS_TO: The supervisor or manager to whom the employee reports.
-- - EMPLOYEE_ID: The unique identifier for each employee.
-- - POSTAL_CODE: The postal code of the employee's address.
-- - TITLE_OF_COURTESY: The courtesy title of the employee.
-- - LAST_UPDATE: The timestamp of the last data extraction from the source.

-- Usage:
-- Include this code in your dbt project to create an incremental view of 'employees'.
-- Adjust the unique key and incremental strategy based on your specific requirements.


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