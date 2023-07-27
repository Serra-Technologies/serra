"""
Wrapper around chatgpt api
"""
import openai
import json
from serra.utils import get_path_to_user_configs_folder
import os
import yaml
# API_KEY = os.environ.get("OPENAI_API_KEY")

openai.api_key = "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"

EXAMPLE_PROMPT = """
SELECT
    id,
    is_family
    subscription_type,
    location,
    CASE location
        WHEN 'AL' THEN 'Alabama'
        WHEN 'AK' THEN 'Alaska'
        WHEN 'AZ' THEN 'Arizona'
        WHEN 'AR' THEN 'Arkansas'
        WHEN 'CA' THEN 'California'
        WHEN 'CO' THEN 'Colorado'
        WHEN 'CT' THEN 'Connecticut'
        WHEN 'DE' THEN 'Delaware'
        WHEN 'FL' THEN 'Florida'
        WHEN 'GA' THEN 'Georgia'
        WHEN 'HI' THEN 'Hawaii'
        WHEN 'ID' THEN 'Idaho'
        WHEN 'IL' THEN 'Illinois'
        WHEN 'IN' THEN 'Indiana'
        WHEN 'IA' THEN 'Iowa'
        WHEN 'KS' THEN 'Kansas'
        WHEN 'KY' THEN 'Kentucky'
        WHEN 'LA' THEN 'Louisiana'
        WHEN 'ME' THEN 'Maine'
        WHEN 'MD' THEN 'Maryland'
        WHEN 'MA' THEN 'Massachusetts'
        WHEN 'MI' THEN 'Michigan'
        WHEN 'MN' THEN 'Minnesota'
        WHEN 'MS' THEN 'Mississippi'
        WHEN 'MO' THEN 'Missouri'
        WHEN 'MT' THEN 'Montana'
        WHEN 'NE' THEN 'Nebraska'
        WHEN 'NV' THEN 'Nevada'
        WHEN 'NH' THEN 'New Hampshire'
        WHEN 'NJ' THEN 'New Jersey'
        WHEN 'NM' THEN 'New Mexico'
        WHEN 'NY' THEN 'New York'
        WHEN 'NC' THEN 'North Carolina'
        WHEN 'ND' THEN 'North Dakota'
        WHEN 'OH' THEN 'Ohio'
        WHEN 'OK' THEN 'Oklahoma'
        WHEN 'OR' THEN 'Oregon'
        WHEN 'PA' THEN 'Pennsylvania'
        WHEN 'RI' THEN 'Rhode Island'
        WHEN 'SC' THEN 'South Carolina'
        WHEN 'SD' THEN 'South Dakota'
        WHEN 'TN' THEN 'Tennessee'
        WHEN 'TX' THEN 'Texas'
        WHEN 'UT' THEN 'Utah'
        WHEN 'VT' THEN 'Vermont'
        WHEN 'VA' THEN 'Virginia'
        WHEN 'WA' THEN 'Washington'
        WHEN 'WV' THEN 'West Virginia'
        WHEN 'WI' THEN 'Wisconsin'
        WHEN 'WY' THEN 'Wyoming'
        ELSE ''
    END AS location_full,
    cast(is_family AS INTEGER) as is_family_int,

from serra_dev.serra_subscriptions;
"""

assistant_context = """
Here is an example of the conversion we want you to do.
We supplied the initial sql script, our framework code, and then finally the config file that we translated the sql script to.


-- SQL SCRIPT
-- Example SQL Script for ETL Job
-- Goal: Get the average rating by state for each fast food franchise.






-- Join, Map, Cast, Pivot




SELECT *
FROM (
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
   CAST(avg_rating AS DOUBLE) as avg_rating
 FROM (
   SELECT
     s.restaurant,
     s.region,
     s.country,
     r.rating AS avg_rating
   FROM
     sales s
     INNER JOIN rating r ON s.id = r.id
   GROUP BY
     s.restaurant,
     s.region,
     s.country,
     r.rating
 )
) AS SourceTable
PIVOT (
 AVG(avg_rating)
 FOR region_abbr IN (
     'AL', 'AK', 'AZ', 'AR', 'CA',
     'CO', 'CT', 'DE', 'FL', 'GA',
     'HI', 'ID', 'IL', 'IN', 'IA',
     'KS', 'KY', 'LA', 'ME', 'MD',
     'MA', 'MI', 'MN', 'MS', 'MO',
     'MT', 'NE', 'NV', 'NH', 'NJ',
     'NM', 'NY', 'NC', 'ND', 'OH',
     'OK', 'OR', 'PA', 'RI', 'SC',
     'SD', 'TN', 'TX', 'UT', 'VT',
     'VA', 'WA', 'WV', 'WI', 'WY')
) AS PivotTable;




____ SERRA FRAMEWORK CODE (READERS, TRANSFORMERS, WRITERS)
-- TRANSFORMERS & Descriptions/params
AddColumnTransformer
  Description: Transformer to add a column to dataframe of a specified type
  Params: name — column name to add, value - value to add to new column, column_type - the type of the new column


CastColumnTransformer
  Description: Transformer to convert a column to a given type
  Params: cast_columns — dictionary where the key is the new column name, the value is a list of source column and target data type


DropColumnTransformer:  
  Description: Transformer to convert a column to a given type
  Params: drop_names — list of columns to be dropped

SelectTransformer:
  Description: Transformer to perform a SELECT operation on a DataFrame.
  Params: columns — Holds the list of columns to select from the DataFrame.
GetCountTransformer:
Description: Transformer to get count 
Params: cast_columns — dictionary where the key is the new column name, the value is 
GetCountTransformer to group by specified columns and add a new column with the count of rows for each group.
Params: config - Holds the configuration parameters such as group_by (list of column names for grouping) and count_col (column name to count rows).
JoinTransformer
Description: Transformer to perform an inner join between two DataFrames on specified columns.
Params: config - Holds the configuration parameters such as join_type (type of join, currently only supports "inner") and join_on (dictionary where the keys are table names and the values are the corresponding columns to join on).
MapTransformer
Description: Transformer to create a new column based on a mapping dictionary or file for a given input column.
Params: config - Holds the configuration parameters such as name (name of the new column), map_dict (dictionary with keys as input values and values as output values), map_dict_path (path to a file containing the mapping dictionary), and col_key (the input column to apply the mapping).
PivotTransformer
Description: Transformer to pivot a DataFrame based on row and column levels and aggregate the values.
Params: config - Holds the configuration parameters such as row_level (column used for row levels), column_level (column used for column levels), sum_col (column to summarize values), and aggregate_type (type of aggregation, currently supports "avg" or "sum").
RenameColumnTransformer
Description: Transformer to rename a column in a DataFrame.
Params: config - Holds the configuration parameters such as old_name (name of the column to be renamed) and new_name (new name for the column).
Readers:
AmazonS3Reader
Description: Reader to read data from a CSV file stored in an Amazon S3 bucket.
Params: config - Holds the configuration parameters such as aws_access_key_id (AWS access key ID), aws_secret_access_key (AWS secret access key), bucket_name (name of the S3 bucket), file_path (path to the CSV file in the bucket), and file_type (type of the file, currently assumes CSV).
DatabricksReader
Description: Reader to read data from a table in Databricks Delta format.
Params: config - Holds the configuration parameters such as database (name of the database), and table (name of the table).
LocalReader
Description: Reader to read data from a local CSV file.
Params: config - Holds the configuration parameters such as file_path (path to the local CSV file).
Writers:
AmazonS3Writer
Description: Writer to write data from a DataFrame to a CSV file stored in an Amazon S3 bucket.
Params: config - Holds the configuration parameters such as aws_access_key_id (AWS access key ID), aws_secret_access_key (AWS secret access key), bucket_name (name of the S3 bucket), file_path (path to the CSV file in the bucket), and file_type (type of the file, currently assumes CSV).
DatabricksWriter
Description: Writer to write data from a DataFrame to a Databricks Delta table.
Params: config - Holds the configuration parameters such as database (name of the database), table (name of the table), format (format of the data, e.g., "parquet"), and mode (write mode, e.g., "overwrite").
LocalWriter
Description: Writer to write data from a DataFrame to a local CSV file.
Params: config - Holds the configuration parameters such as file_path (path to the local CSV file).






-- SERRA CONFIGURATION FILE that you must model
debug: true


read_sales:
 class_name: AmazonS3Reader
 config:
   aws_access_key_id: AKIA3TOV3GZZHAH4MPAE
   aws_secret_access_key: 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
   bucket_name: serrademo
   file_path: sales.csv
   file_type: csv


read_ratings:
 class_name: AmazonS3Reader
 config:
   aws_access_key_id: AKIA3TOV3GZZHAH4MPAE
   aws_secret_access_key: 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
   bucket_name: serrademo
   file_path: rating_df.csv
   file_type: csv


join_tables:
 class_name: JoinTransformer
 config:
   join_type: 'inner'
   join_on:
     read_sales: id
     read_ratings: id


map_state_names:
 class_name: MapTransformer
 config:
   input_block: join_tables
   name: 'region_abbr'
   map_dict:
       Alabama: 'AL'
       Alaska: 'AK'
       Arizona: 'AZ'
       Arkansas: 'AR'
       California: 'CA'
       Colorado: 'CO'
       Connecticut: 'CT'
       Delaware: 'DE'
       Florida: 'FL'
       Georgia: 'GA'
       Hawaii: 'HI'
       Idaho: 'ID'
       Illinois: 'IL'
       Indiana: 'IN'
       Iowa: 'IA'
       Kansas: 'KS'
       Kentucky: 'KY'
       Louisiana: 'LA'
       Maine: 'ME'
       Maryland: 'MD'
       Massachusetts: 'MA'
       Michigan: 'MI'
       Minnesota: 'MN'
       Mississippi: 'MS'
       Missouri: 'MO'
       Montana: 'MT'
       Nebraska: 'NE'
       Nevada: 'NV'
       New Hampshire: 'NH'
       New Jersey: 'NJ'
       New Mexico: 'NM'
       New York: 'NY'
       North Carolina: 'NC'
       North Dakota: 'ND'
       Ohio: 'OH'
       Oklahoma: 'OK'
       Oregon: 'OR'
       Pennsylvania: 'PA'
       Rhode Island: 'RI'
       South Carolina: 'SC'
       South Dakota: 'SD'
       Tennessee: 'TN'
       Texas: 'TX'
       Utah: 'UT'
       Vermont: 'VT'
       Virginia: 'VA'
       Washington: 'WA'
       West Virginia: 'WV'
       Wisconsin: 'WI'
       Wyoming: 'WY'
   col_key: 'region'
 cast_ratings:
 class_name: CastColumnTransformer
 config:
   input_block: map_state_names
   cast_columns:
     rating: ['rating', 'double']


pivot:
 class_name: PivotTransformer
 config:
   input_block: cast_ratings
   row_level: 'restaurant'
   column_level: "region"
   aggregate_type: "avg"
   sum_col: "customers"


step_write:
 class_name: LocalWriter
 config:
   input_block: pivot
   file_path: "./examples/outputalan.csv"



The general structure of config file you must follow is (the config should contain the necessary unique parameters and the exact block structure of this (no higher nested levels)):

step_name: 
  class_name: The necessary class ()  --- the key must be class_name
  config: --- the key must be class_name
    input_block: -- this is the prior step and the key must be input_block, every writer and transformer NEEDS THIS except the JoinTransformer and Readers
"""


class Translator:
    def __init__(self, sql_path, is_file=True, verbose=False):
        self.sql_path = sql_path
        self.is_file = is_file
        self.verbose = verbose

    def prompt_gpt(self) -> dict:
        """
        Return the result of the prompt
        Follow the format of EXAMPLE_RESULT above

        :param prompt_or_file: Either a string prompt or the path to the SQL file.
        :param is_file: Set to True if prompt_or_file is a file path, otherwise False.
        :param verbose: Whether to print verbose output.

        :return: The generated content as a string or None if failed.
        """
        if self.is_file:
            # If prompt_or_file is a file path, read the SQL script from the file.
            if not os.path.exists(self.sql_path):
                print("Error: File not found.")
                return None

            with open(self.sql_path, 'r') as file:
                prompt = file.read()
        else:
            prompt = self.sql_path
        if self.verbose:
            print("Running following prompt")
            print(prompt)
    
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Don't provide any explanation for your answers. You are a Serra Data Engineer developer. Translate SQL scripts to our framework and output the configuration file exactly like our assistant example. Default to local reader and writer. DO not put ```yaml in your output. Specify all local paths under the './examples' folder. Use only the available framework." },
                {"role": "user", "content": prompt},
                {"role": "assistant", "content": assistant_context}
            ]
        )
        try:
            response_json_string = json.dumps(response)
            response_json = json.loads(response_json_string)
            content = response_json["choices"][0]["message"]["content"]
            print("Translated Serra Configuration: \n\n\n\n", content)
            return content
        except:
            print("Failed to decode chatgpt response")
            return None
        
    def save_as_yaml(self, content: str, file_path: str) -> None:
        """
        Save the content as a YAML file.

        :param content: The content to be saved as YAML.
        :param file_path: The path to the output YAML file.
        """
        try:
            yaml_content = yaml.safe_load(content)
            with open(file_path, 'w') as file:
                yaml.dump(yaml_content, file)
            print(f"Content saved as YAML file: {file_path}")
        except Exception as e:
            print(f"Error saving content as YAML file: {str(e)}")
