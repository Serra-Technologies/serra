import setuptools
from setuptools import setup

setup(name='serra',
      version='0.7.184',
      description='Simplified Data Pipelines',
      url='http://github.com',
      author='Serra Technologies',
      author_email='founders@serra.io',
      packages=setuptools.find_packages(),
      package_data={"serra": ["data/workspace_example/*",
                              "data/workspace_example/*/*",
                              "data/autocomplete/*",
                              "data/autocomplete/*/*"]},
      install_requires=[
          "click",
          "pyspark",
          "pyyaml", 
          "pandas", 
          "boto3", 
          "databricks-sdk",
          "wheel",
          "loguru",
          "snowflake-connector-python",
          "google-cloud-bigquery",
          "geopy",
          "db-dtypes"
      ],
      zip_safe=False,
      entry_points={
        'console_scripts': [
            'serra=serra.cli:main',
            'serra_databricks=serra.cli:serra_databricks'
        ]
    })
