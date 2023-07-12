import boto3
import json

#_________Read Json from S3_____________
def read_json_s3(file):
    s3 = boto3.resource('s3')
    content_object = s3.Object('serrademo', file)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)

json_content = read_json_s3('aws.json')

config = {'tables': ['t1','t2']}

json_content
#_________Update Json w Table Dependencies_____________
json_content['upstream_tables'] = config.get('tables')
json_test = {}
#_________Write Json to S3_____________
def write_json_s3(obj, file):
    s3 = boto3.resource('s3')
    json_dump_s3 = lambda obj, f: s3.Object('serrademo', key=f).put(Body=json.dumps(obj))
    json_dump_s3(obj, file)

write_json_s3({},'aws.json')



from serra.readers.s3_reader import S3Reader
from serra.transformers.join_transformer import Join
from serra.transformers.map_transformer import Map
from serra.transformers.pivot_transformer import Pivot
from pyspark.sql import functions as F


location_map = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}

join_config = {'join_type': 'inner', 'matching_col': ['id','id'], 'path': 's3a://serrademo/rating_df.csv'}
map_config = {'name':'region_abbr', 'map_dict': location_map, 'col_key':'region'}
pivot_config = {'row_level': 'restaurant', 'column_level': 'region_abbr', 'sum_col': 'rating'}

restaurant_config = {'path':'s3a://serrademo/restaurants.csv', 'format':'csv'}
restaurants_df = S3Reader(restaurant_config).read()
df1 = Join(join_config).transform(restaurants_df)
df2 = Map(map_config).transform(df1)
df2 = df2.withColumn('rating', F.col('rating').cast('double'))
Pivot(pivot_config).transform(df2).show()

country_config = {'path':'s3a://serrademo/country.csv', 'format':'csv'}
customers_config = {'path':'s3a://serrademo/customers.csv', 'format':'csv'}
join_config = {'join_type': 'inner', 'matching_col': ['id','id'], 'path': 's3a://serrademo/country.csv' }
countries = S3Reader(country_config).read()
customers = S3Reader(customers_config).read()

restaurants = Join(join_config).transform(customers)
restaurants.show()
# restaurants.write.option("header","true").mode('overwrite').format('csv').save('s3a://serrademo/restaurants.csv')
