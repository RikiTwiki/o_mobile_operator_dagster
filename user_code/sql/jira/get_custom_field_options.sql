SELECT
  id,
  customvalue AS title
FROM customfieldoption
WHERE customfield = %(customfield)s
  AND disabled = 'N'
ORDER BY sequence;
