WITH sf_leads AS (
    SELECT
    CASE 
            WHEN REGEXP_REPLACE(phone, '[^0-9]', '') LIKE '1%' 
            THEN SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', ''), 2) 
            ELSE REGEXP_REPLACE(phone, '[^0-9]', '') 
        END AS phone,
    IS_DELETED,
    LOWER(TRIM(LAST_NAME)) AS last_name,
    LOWER(TRIM(FIRST_NAME)) AS first_name,
    LOWER(TRIM(TITLE)) AS title,
    COMPANY,
    STREET,
    CITY,
    STATE,
    POSTAL_CODE,
    COUNTRY,
    CASE 
            WHEN REGEXP_REPLACE(mobile_phone, '[^0-9]', '') LIKE '1%' 
            THEN SUBSTRING(REGEXP_REPLACE(mobile_phone, '[^0-9]', ''), 2) 
            ELSE REGEXP_REPLACE(mobile_phone, '[^0-9]', '') 
        END AS mobile_phone,
    LOWER(EMAIL) AS email,
    LOWER(WEBSITE) AS website,
    LOWER(LEAD_SOURCE) AS lead_source,
    LOWER(STATUS) AS status,
    IS_CONVERTED,
    CREATED_DATE,
    LAST_MODIFIED_DATE,
    LAST_ACTIVITY_DATE,
    EMAIL_BOUNCED_REASON,
    EMAIL_BOUNCED_DATE,
    OUTREACH_STAGE_C,
    CURRENT_ENROLLMENT_C,
    CAPACITY_C,
    LEAD_SOURCE_LAST_UPDATED_C,
    BRIGHTWHEEL_SCHOOL_UUID_C
    FROM {{ source('raw', 'sf_leads') }}
)

select *
from sf_leads