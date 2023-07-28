import setuptools
from setuptools import setup

setup(name='serra',
      version='0.3',
      description='Simplified Data Pipelines',
      url='http://github.com',
      author='Alan Wang',
      author_email='alan@serra.io',
      license='tbd',
      packages=setuptools.find_packages(),
      package_data={"serra": ["data/*.yml"]},
      install_requires=[
          "click","pyspark", "pyyaml", "pandas", "boto3", "databricks-sdk", "loguru", 'flask'
      ],
      zip_safe=False,
      entry_points={
        'console_scripts': [
            'serra=serra.cli:main',
            'serra_databricks=serra.cli:serra_databricks'
        ]
    })