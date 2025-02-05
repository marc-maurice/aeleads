WITH merged_leads AS (
    SELECT 
        d.source AS original_source,
        d.lead_id,
        d.phone,
        d.first_name,
        d.last_name,
        d.email,
        d.company,
        d.street,
        d.city,
        d.state,
        d.postal_code,
        d.country,
        d.created_date,
        d.last_modified_date,

        -- Assign Salesforce Status
        CASE 
            WHEN sf.phone IS NOT NULL OR sf.mobile_phone IS NOT NULL THEN 'exists_in_salesforce'
            ELSE 'not_in_salesforce'
        END AS salesforce_status,

        -- Determine Duplicate Status
        CASE 
            WHEN sf.phone IS NOT NULL AND (COALESCE(d.phone_match, 0) > 1 OR COALESCE(d.email_match, 0) > 1) 
            THEN 'potential_duplicate'
            WHEN sf.phone IS NOT NULL THEN 'unique'
            ELSE 'new_lead'
        END AS dup_status,

        -- Salesforce Data (Aliased)
        sf.phone AS sf_phone,
        sf.mobile_phone AS sf_mobile_phone,
        sf.email AS sf_email,
        sf.company AS sf_company,
        sf.street AS sf_street,
        sf.city AS sf_city,
        sf.state AS sf_state,
        sf.postal_code AS sf_postal_code,
        sf.country AS sf_country,
        sf.lead_source AS sf_lead_source,
        sf.status AS sf_status,
        sf.is_converted,
        sf.created_date AS sf_created_date,
        sf.last_modified_date AS sf_last_modified_date,

        -- Generate a Unique Hash for Deduplication
        MD5(CONCAT_WS('|', d.phone, d.email, d.first_name, d.last_name, d.street, d.city, d.state, d.postal_code)) AS dedup_hash

    FROM {{ source('int', 'int_leads_duped') }} d
    LEFT OUTER JOIN {{ source('int', 'int_leads_sf') }} sf
        ON d.phone = sf.phone OR d.phone = sf.mobile_phone
),

ranked_leads AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY phone ORDER BY created_date ASC) AS row_num
    FROM merged_leads
)

SELECT *
FROM ranked_leads
ORDER BY phone, row_num
