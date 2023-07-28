-- note, rating table is ratings.csv, NOT rating.csv
-- reader,writer locations are ./examples/{table_name}.csv


SELECT * FROM (
  SELECT
    restaurant,
    CASE
      WHEN region = 'Alabama' THEN 'AL'
      WHEN region = 'Alaska' THEN 'AK'
      WHEN region = 'Arizona' THEN 'AZ'
      WHEN region = 'Arkansas' THEN 'AR'
      WHEN region = 'California' THEN 'CA'
      WHEN region = 'Colorado' THEN 'CO'
      WHEN region = 'Connecticut' THEN 'CT'
      WHEN region = 'Delaware' THEN 'DE'
      WHEN region = 'Florida' THEN 'FL'
      WHEN region = 'Georgia' THEN 'GA'
      WHEN region = 'Hawaii' THEN 'HI'
      WHEN region = 'Idaho' THEN 'ID'
      WHEN region = 'Illinois' THEN 'IL'
      WHEN region = 'Indiana' THEN 'IN'
      WHEN region = 'Iowa' THEN 'IA'
      WHEN region = 'Kansas' THEN 'KS'
      WHEN region = 'Kentucky' THEN 'KY'
      WHEN region = 'Louisiana' THEN 'LA'
      WHEN region = 'Maine' THEN 'ME'
      WHEN region = 'Maryland' THEN 'MD'
      WHEN region = 'Massachusetts' THEN 'MA'
      WHEN region = 'Michigan' THEN 'MI'
      WHEN region = 'Minnesota' THEN 'MN'
      WHEN region = 'Mississippi' THEN 'MS'
      WHEN region = 'Missouri' THEN 'MO'
      WHEN region = 'Montana' THEN 'MT'
      WHEN region = 'Nebraska' THEN 'NE'
      WHEN region = 'Nevada' THEN 'NV'
      WHEN region = 'New Hampshire' THEN 'NH'
      WHEN region = 'New Jersey' THEN 'NJ'
      WHEN region = 'New Mexico' THEN 'NM'
      WHEN region = 'New York' THEN 'NY'
      WHEN region = 'North Carolina' THEN 'NC'
      WHEN region = 'North Dakota' THEN 'ND'
      WHEN region = 'Ohio' THEN 'OH'
      WHEN region = 'Oklahoma' THEN 'OK'
      WHEN region = 'Oregon' THEN 'OR'
      WHEN region = 'Pennsylvania' THEN 'PA'
      WHEN region = 'Rhode Island' THEN 'RI'
      WHEN region = 'South Carolina' THEN 'SC'
      WHEN region = 'South Dakota' THEN 'SD'
      WHEN region = 'Tennessee' THEN 'TN'
      WHEN region = 'Texas' THEN 'TX'
      WHEN region = 'Utah' THEN 'UT'
      WHEN region = 'Vermont' THEN 'VT'
      WHEN region = 'Virginia' THEN 'VA'
      WHEN region = 'Washington' THEN 'WA'
      WHEN region = 'West Virginia' THEN 'WV'
      WHEN region = 'Wisconsin' THEN 'WI'
      WHEN region = 'Wyoming' THEN 'WY'
    END AS region_abbr,
    country,
    false AS is_test  -- New boolean column with default value false
FROM (
    SELECT
        s.restaurant,
        s.region,
        s.country,
        r.rating
    FROM
        sales s
        INNER JOIN rating r ON s.id = r.id
    GROUP BY
        s.restaurant,
        s.region,
        s.country,
        r.rating
)
)






