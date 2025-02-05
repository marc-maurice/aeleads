WITH leads_union AS (
    SELECT
        'salesforce' AS source,
        id AS lead_id,
        CASE 
            WHEN REGEXP_REPLACE(phone, '[^0-9]', '') LIKE '1%' 
            THEN SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', ''), 2) 
            ELSE REGEXP_REPLACE(phone, '[^0-9]', '') 
        END AS phone,
        LOWER(TRIM(first_name)) AS first_name,
        LOWER(TRIM(last_name)) AS last_name,
        LOWER(TRIM(email)) AS email,
        LOWER(TRIM(company)) AS company,
        LOWER(TRIM(street)) AS street,
        LOWER(TRIM(city)) AS city,
        LOWER(TRIM(state)) AS state,
        LEFT(LOWER(TRIM(postal_code)), 5) AS postal_code,
        LOWER(TRIM(country)) AS country,
        created_date,
        last_modified_date
    FROM {{ source('stg', 'stg_sf_leads') }}

    UNION ALL

    SELECT
        'salesforce_mobile' AS source,
        id AS lead_id,
        CASE 
            WHEN REGEXP_REPLACE(mobile_phone, '[^0-9]', '') LIKE '1%' 
            THEN SUBSTRING(REGEXP_REPLACE(mobile_phone, '[^0-9]', ''), 2) 
            ELSE REGEXP_REPLACE(mobile_phone, '[^0-9]', '') 
        END AS phone,
        LOWER(TRIM(first_name)) AS first_name,
        LOWER(TRIM(last_name)) AS last_name,
        LOWER(TRIM(email)) AS email,
        LOWER(TRIM(company)) AS company,
        LOWER(TRIM(street)) AS street,
        LOWER(TRIM(city)) AS city,
        LOWER(TRIM(state)) AS state,
        LEFT(LOWER(TRIM(postal_code)), 5) AS postal_code,
        LOWER(TRIM(country)) AS country,
        created_date,
        last_modified_date
    FROM {{ source('stg', 'stg_sf_leads') }}

    UNION ALL

    SELECT
        'source1' AS source,
        NULL AS lead_id,
        CASE 
            WHEN REGEXP_REPLACE(phone, '[^0-9]', '') LIKE '1%' 
            THEN SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', ''), 2) 
            ELSE REGEXP_REPLACE(phone, '[^0-9]', '') 
        END AS phone,
        LOWER(TRIM(name)) AS first_name,
        NULL AS last_name,
        NULL AS email,
        NULL AS company,
        LOWER(TRIM(address)) AS street,
        NULL AS city,
        LOWER(TRIM(state)) AS state,
        NULL AS postal_code,
        NULL AS country,
        NULL AS created_date,
        NULL AS last_modified_date
    FROM {{ source('stg', 'stg_source1') }}

    UNION ALL

    SELECT
        'source2' AS source,
        NULL AS lead_id,
        CASE 
            WHEN REGEXP_REPLACE(phone, '[^0-9]', '') LIKE '1%' 
            THEN SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', ''), 2) 
            ELSE REGEXP_REPLACE(phone, '[^0-9]', '') 
        END AS phone,
        NULL AS first_name,
        NULL AS last_name,
        LOWER(TRIM(email)) AS email,
        LOWER(TRIM(company)) AS company,
        LOWER(TRIM(address)) AS street,
        LOWER(TRIM(city)) AS city,
        LOWER(TRIM(state)) AS state,
        NULL AS postal_code,
        NULL AS country,
        NULL AS created_date,
        NULL AS last_modified_date
    FROM {{ source('stg', 'stg_source2') }}

    UNION ALL

    SELECT
        'source3' AS source,
        NULL AS lead_id,
        CASE 
            WHEN REGEXP_REPLACE(phone, '[^0-9]', '') LIKE '1%' 
            THEN SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', ''), 2) 
            ELSE REGEXP_REPLACE(phone, '[^0-9]', '') 
        END AS phone,
        NULL AS first_name,
        NULL AS last_name,
        LOWER(TRIM(email_address)) AS email,
        NULL AS company,
        LOWER(TRIM(address)) AS street,
        LOWER(TRIM(city)) AS city,
        LOWER(TRIM(state)) AS state,
        NULL AS postal_code,
        NULL AS country,
        NULL AS created_date,
        NULL AS last_modified_date
    FROM {{ source('stg', 'stg_source3') }}
)

SELECT *
FROM leads_union
