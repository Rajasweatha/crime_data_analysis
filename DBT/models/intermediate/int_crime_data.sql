{{ 
  config(materialized = 'table')
}}

WITH RAW AS (
  SELECT 
    * 
  FROM {{ ref('stage_crime_data') }}
),
TRANSFORMED AS (
  SELECT
    CRIME_ID,
    TRY_TO_DATE(MONTH || '-01') AS CRIME_DATE,
    REPORTED_BY,
    FALLS_WITHIN,
    LONGITUDE,
    LATITUDE,
    REGEXP_REPLACE(LOCATION, 'On or near ', '') AS UPDATED_LOCATION,
    LSOA_CODE,
    LSOA_NAME,
    CRIME_TYPE,
    LAST_OUTCOME,
    NULLIF(CONTEXT, '') AS CONTEXT
  FROM RAW
  WHERE
   UPDATED_LOCATION != 'No Location'
)
SELECT * FROM TRANSFORMED