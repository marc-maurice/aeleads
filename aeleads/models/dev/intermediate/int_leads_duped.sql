WITH ranked_leads AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY phone
               ORDER BY last_modified_date DESC NULLS LAST
           ) AS phone_match,
           ROW_NUMBER() OVER (
               PARTITION BY email
               ORDER BY last_modified_date DESC NULLS LAST
           ) AS email_match
    FROM {{ source('int', 'int_leads_base') }}
)

SELECT *
FROM ranked_leads
