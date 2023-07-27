from serra.readers.amazon_reader import AmazonS3Reader
from serra.transformers.select_transformer import SelectTransformer

read_config = {
  "file_type": "csv", 
  "bucket_name": "serrademo", 
  "aws_access_key_id": "AKIA3TOV3GZZHAH4MPAE", 
  "aws_secret_access_key": "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", 
  "file_path": "sales.csv"
}

select_config = {
    "columns": ['id']
}

df = AmazonS3Reader(read_config).read()
df = SelectTransformer(select_config).transform(df)

print(df)