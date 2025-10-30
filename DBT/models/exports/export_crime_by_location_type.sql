{{ config(
    materialized='incremental',
    unique_key='CRIME_ID'
) }}

SELECT
    CRIME_ID,
    CRIME_DATE,
    REPORTED_BY,
    CRIME_TYPE,
    UPDATED_LOCATION AS LOCATION,
    COUNT(*) AS CRIME_COUNT
FROM 
    {{ ref('int_crime_data') }}
WHERE 
    {% if is_incremental() %}
        DATE(LAST_UPDATED) = CURRENT_DATE
    {% else %}
        1=1
    {% endif %}
GROUP BY 
    CRIME_ID,
    CRIME_DATE,
    REPORTED_BY,
    CRIME_TYPE,
    UPDATED_LOCATION
ORDER BY 
    REPORTED_BY,
    CRIME_COUNT DESC