WITH source AS (
    SELECT
    REGEXP_REPLACE(PHONE, '[^0-9]', '') AS phone,
    OPERATION,
    AGENCY_NUMBER,
    LOWER(TRIM(OPERATION_NAME)) AS operation_name,
    LOWER(TRIM(ADDRESS)) AS address,
    LOWER(TRIM(CITY)) AS city,
    LOWER(STATE) AS state,
    ZIP,
    LOWER(TRIM(COUNTY)) AS county,
    LOWER(TRIM(TYPE)) AS type,
    LOWER(STATUS) AS status,
    ISSUE_DATE,
    CAPACITY,
    LOWER(TRIM(EMAIL_ADDRESS)) AS email_address,
    FACILITY_ID,
    MONITORING_FREQUENCY,
    INFANT,
    TODDLER,
    PRESCHOOL,
    SCHOOL
    FROM {{ source('raw', 'source3') }}
)

SELECT * FROM source
