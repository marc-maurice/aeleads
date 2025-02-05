WITH source AS (
    SELECT
    REGEXP_REPLACE(PHONE, '[^0-9]', '') AS phone,
    LOWER(TYPE_LICENSE) AS type_license,
    LOWER(TRIM(COMPANY)) AS company,
    ACCEPTS_SUBSIDY,
    YEAR_ROUND,
    DAYTIME_HOURS,
    STAR_LEVEL,
    MON,
    TUES,
    WED,
    THURS,
    FRIDAY,
    SATURDAY,
    SUNDAY,
    LOWER(TRIM(PRIMARY_CAREGIVER)) AS primary_caregiver,
    LOWER(TRIM(EMAIL)) AS email,
    LOWER(TRIM(ADDRESS1)) AS address,
    LOWER(TRIM(ADDRESS2)) AS address2,
    LOWER(TRIM(CITY)) AS city,
    LOWER(STATE) AS state,
    ZIP,
    SUBSIDY_CONTRACT_NUMBER,
    TOTAL_CAP,
    AGES_ACCEPTED_1,
    AA2,
    AA3,
    AA4,
    LICENSE_MONITORING_SINCE,
    SCHOOL_YEAR_ONLY,
    EVENING_HOURS
    FROM {{ source('raw', 'source2') }}
)

SELECT * FROM source
