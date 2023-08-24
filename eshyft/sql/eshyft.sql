SELECT
  _id AS user,
  '2050-10-08' AS expiresDate,
  TIMESTAMP('2050-10-08') AS expiresTime,
  REPLACE(JSON_EXTRACT(a, '$.type'),"\"","") AS license,
  REPLACE(JSON_EXTRACT(a, '$.type'),"\"","") AS type,
  REPLACE(JSON_EXTRACT(a, '$.number'),"\"","") AS number,
  REPLACE(JSON_EXTRACT(a, '$.state'),"\"","") AS state,
  REPLACE(JSON_EXTRACT(a, '$.expires'),"\"","") as expireTest
FROM
  `mongodb_eshyft.users`
LEFT JOIN
  UNNEST(JSON_EXTRACT_ARRAY(licenses)) AS a
WHERE
  licenses <> '[]'
  AND licenses IS NOT NULL
  AND _id NOT IN (
  SELECT
    user
  FROM
    `eshyft_dw.test_users_list`)