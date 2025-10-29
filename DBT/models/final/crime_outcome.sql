SELECT
  REPORTED_BY,
  CASE
    WHEN 
      LAST_OUTCOME ILIKE '%investigation complete%' 
    THEN 'Investigation Complete'
    WHEN 
      LAST_OUTCOME ILIKE '%public interest%' 
    THEN 'No Further Action - Public Interest'
    WHEN 
      LAST_OUTCOME ILIKE '%local resolution%' OR 
      LAST_OUTCOME ILIKE '%another organisation%' 
    THEN 'Local/Alternative Resolution'
    WHEN 
      LAST_OUTCOME ILIKE '%caution%' OR 
      LAST_OUTCOME ILIKE '%penalty%' 
    THEN 'Caution or Penalty Issued'
    WHEN 
      LAST_OUTCOME ILIKE '%awaiting court%' 
    THEN 'Awaiting Legal Outcome'
    WHEN 
      LAST_OUTCOME ILIKE '%under investigation%' 
    THEN 'Under Investigation'
    WHEN 
      LAST_OUTCOME ILIKE '%unable to prosecute%' 
    THEN 'Unable to Prosecute'
    ELSE 'Other'
  END AS OUTCOME_CATEGORY,
  COUNT(*) AS CASE_COUNT
FROM 
  {{ ref('int_crime_data') }}
GROUP BY 
  REPORTED_BY, OUTCOME_CATEGORY
ORDER BY 
  CASE_COUNT DESC