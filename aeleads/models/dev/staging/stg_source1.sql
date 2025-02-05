WITH source AS (
    SELECT
    REGEXP_REPLACE(PHONE, '[^0-9]', '') AS phone,
    LOWER(TRIM(NAME)) AS name,
    LOWER(CREDENTIAL_TYPW) AS credential_type,
    CREDENTIAL_NUMBER,
    LOWER(STATUS) AS status,
    EXPIRATION_DATE,
    DISCIPLINARY_ACTION,
    LOWER(TRIM(ADDRESS)) AS address,
    LOWER(STATE) AS state,
    LOWER(COUNTY) AS county,
    LOWER(PRIMARY_CONTACT_NAME) AS primary_contact_name,
    LOWER(PRIMARY_CONTACT_ROLE) AS primary_contact_role
    FROM {{ source('raw', 'source1') }}
)

SELECT * FROM source