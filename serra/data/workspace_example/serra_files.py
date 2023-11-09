from pyspark.sql import SparkSession
from abc import ABC, abstractmethod
from pyspark.sql.functions import col, when
import yaml
import json
import os
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import lit,max,min
from pyspark.sql import Window
from pyspark.ml.feature import Imputer
from pyspark.sql import DataFrame

class SerraProfile:

    def __init__(self, config):
        self.config = config

    @staticmethod
    def from_string(config_string):
        config = yaml.safe_load(config_string)
        return SerraProfile(config)

    @staticmethod
    def from_yaml_path(config_path):
        with open(config_path, 'r') as stream:
            config = yaml.safe_load(stream)
        return SerraProfile(config)

    @property
    def aws_access_key_id(self):
        return self.config.get("AWS_ACCESS_KEY_ID")
    
    @property
    def aws_secret_access_key(self):
        return self.config.get("AWS_SECRET_ACCESS_KEY")
    
    @property
    def aws_config_bucket(self):
        return self.config.get("AWS_CONFIG_BUCKET")
    
    @property
    def databricks_host(self):
        return self.config.get("DATABRICKS_HOST")
    
    @property
    def databricks_token(self):
        return self.config.get("DATABRICKS_TOKEN")
    
    @property
    def databricks_cluster_id(self):
        return self.config.get("DATABRICKS_CLUSTER_ID")
    
    @property
    def snowflake_account(self):
        return self.config.get("SNOWFLAKE")

class Step(ABC):
    # @abstractmethod
    # def execute(self, **args):
    #     pass

    def add_serra_profile(self, serra_profile: SerraProfile):
        self.serra_profile = serra_profile

    def add_spark_session(self, spark):
        self.spark = spark

class Reader(Step):
    """
    Reader base class for ingesting data
    Enforce read method
    """
    @abstractmethod
    def read(self, fmt, path, predicate=None):
        pass

    @property
    def dependencies(self):
        return []

class Transformer(Step):
    """
    Transformer base class for data transformations
    Enforce transform method
    """
    @abstractmethod
    def transform(self, df):
        pass

    @property
    def dependencies(self):
        return [self.input_block]

class Writer(Step):
    """
    Writer base class for loading data
    Enforce write method
    """
    @abstractmethod
    def write(self, df):
        pass

    @property
    def dependencies(self):
        return [self.input_block]

class SerraRunException(Exception):
    "Raised when error is detected when running a Serra job"
    pass

class LocalReader(Reader):
    """
    A reader to read data from a local file into a Spark DataFrame.

    :param config: A dictionary containing the configuration for the reader.
                   It should have the following key:
                   - 'file_path': The path to the local file to be read.
    """


    def __init__(self, file_path):
        self.file_path = file_path

    @classmethod
    def from_config(cls, config):
        file_path = config.get("file_path")
        return cls(file_path)
        
    def read(self):
        """
        Read data from a local file and return a Spark DataFrame.

        :return: A Spark DataFrame containing the data read from the local file.
        """
        #TODO: check all files supports
        df = self.spark.read.format("csv").option("header",True).load(self.file_path)
        return df

    def read_with_spark(self, spark):
        self.spark = spark
        return self.read()
    
class DatabricksReader(Reader):
    """
    A reader to read data from a Databricks Delta Lake table into a Spark DataFrame.

    :param config: A dictionary containing the configuration for the reader.
                   It should have the following keys:
                   - 'database': The name of the database containing the table.
                   - 'table': The name of the table to be read.
    """

    def __init__(self, database, table):
        self.database = database
        self.table = table
        
    @classmethod
    def from_config(cls, config):
        database = config.get('database')
        table = config.get('table')

        obj = cls(database, table)
        return obj

    def read(self):
        """
        Read data from a Databricks Delta Lake table and return a Spark DataFrame.

        :return: A Spark DataFrame containing the data read from the specified table.
        :raises: SerraRunException if an error occurs during the data reading process.
        """
        try:
            df = self.spark.read.table(f'{self.database}.{self.table}')
        except Exception as e:
            raise SerraRunException(e)
        return df

    def read_with_spark(self, spark):
        self.spark = spark
        return self.read()


class JoinTransformer(Transformer):
    """
    A transformer to join two DataFrames together based on a specified join condition.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'join_type': The type of join to perform. Currently only 'inner' join is supported.
                   - 'join_on': A dictionary where the keys are the table names (or DataFrame aliases)
                                and the values are the column names to join on for each table.
                   Example: {'table1': 'column1', 'table2': 'column2'}
    """


    def __init__(self, join_type, join_on):
        self.join_type = join_type
        self.join_on = join_on

    @classmethod
    def from_config(cls, config):
        join_type = config.get("join_type")
        join_on = config.get("join_on")
        obj = cls(join_type, join_on)
        obj.input_block = config.get("input_block")
        return obj
    
    @property
    def dependencies(self):
        return self.input_block

    def transform(self, df1, df2):
        """
        Join two DataFrames together based on the specified join condition.

        :param df1: The first DataFrame to be joined.
        :param df2: The second DataFrame to be joined.
        :return: A new DataFrame resulting from the join operation.
        :raises: SerraRunException if the join condition columns do not match between the two DataFrames.
        """
        # assert self.join_type in "inner"

        join_keys = []

        
        for table in self.join_on:
            join_keys.append(self.join_on.get(table))

        assert len(join_keys) == 2

        matching_col = join_keys

        df1 = df1.join(df2, df1[matching_col[0]] == df2[matching_col[1]], self.join_type).drop(df2[matching_col[1]])
        if df1.isEmpty():
            raise SerraRunException(f"""Joiner - Join Key Error: {matching_col[0]}, {matching_col[1]} columns do not match.""")
        return df1 
    
class AddColumnTransformer(Transformer):
    """
    A transformer to add a new column to the DataFrame with a specified value.

    :param new_column_name: The name of the new column to be added.
    :param value: The value to be set for the new column.
    :param new_column_type: The data type of the new column. Must be a valid PySpark data type string.
    """

    def __init__(self, new_column_name, value, new_column_type):
        self.new_column_name = new_column_name
        self.value = value
        self.new_column_type = new_column_type

    @classmethod
    def from_config(cls, config):
        new_column_name = config.get("new_column_name")
        value = config.get("value")
        new_column_type = config.get("new_column_type")

        obj = cls(new_column_name, value, new_column_type)
        obj.input_block = config.get('input_block')
        return obj
        
    def transform(self, df):
        """
        Add a new column to the DataFrame with the specified name, value, and data type.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the new column added.
        :raises: SerraRunException if the column with the specified name already exists in the DataFrame.
        """
        if self.new_column_name in df.columns:
            raise SerraRunException(f"Column '{self.new_column_name}' already exists in the DataFrame. Choose a different name.")
        
        return df.withColumn(
            self.new_column_name, F.lit(self.value).cast(self.new_column_type)  
        )

class AggregateTransformer(Transformer):
    """
    A transformer to aggregate data based on specified columns and aggregation type.

    :param group_by_columns: A list of column names to group the DataFrame by.
    :param aggregation_type: The type of aggregation to be performed. Supported values: 'sum', 'avg', 'count'.
    """

    def __init__(self, group_by_columns, aggregation_type):
        self.group_by_columns = group_by_columns
        self.aggregation_type = aggregation_type

    @classmethod
    def from_config(cls, config):
        group_by_columns = config.get('group_by_columns')
        aggregation_type = config.get('aggregation_type')

        obj = cls(group_by_columns, aggregation_type)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Transform the DataFrame by aggregating data based on the specified configuration.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the aggregated data.
        """
        df = df.groupBy(*self.group_by_columns)

        if self.aggregation_type == 'sum':
            df = df.sum()
        if self.type == 'avg':
            df = df.mean()
        if self.type == 'count':
            df = df.count()

        return df

class CaseWhenTransformer(Transformer):
    """
    Test transformer to add a column to dataframe
    :param output_column: The name of the new output column to be added.
    :param input_column: The name of the input column.
    :param conditions: A list of conditions to evaluate. Each condition should be a tuple of (condition_column, operator, value).
    :param comparison_type: The type of comparison to use (e.g., 'equals', 'greater_than').
    :param is_column_condition: Boolean indicating whether the condition is based on a column value.
    :param otherwise_value: The value to use when none of the conditions are met.
    """

    def __init__(self, output_column, input_column, conditions, comparison_type, is_column_condition, otherwise_value):
        self.output_column = output_column
        self.input_column = input_column
        self.conditions = conditions
        self.comparison_type = comparison_type
        self.is_column_condition = is_column_condition
        self.otherwise_value = otherwise_value

    @classmethod
    def from_config(cls, config):
        output_column = config.get("output_column")
        input_column = config.get("input_column")
        conditions = config.get('conditions')
        comparison_type = config.get('comparison_type')
        is_column_condition = config.get('is_column_condition')
        otherwise_value = config.get('otherwise_value')

        obj = cls(output_column, input_column, conditions, comparison_type, is_column_condition, otherwise_value)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """
        print("Conditions:", self.conditions[0][0])
        # Mappings to indicate which comparison function to use
        type_dict = {
            '<=': lambda col, val: col <= val,
            '>=': lambda col, val: col >= val,
            '==': lambda col, val: col == val,
            'like': lambda col, val: col.like(val)
        }

        # Get the particular condition function based on the comparison type
        comparison_func = type_dict.get(self.comparison_type)

        if comparison_func is None:
            raise ValueError(f"Unsupported comparison type: {self.comparison_type}")
        print('###########', self.conditions[0][0])
        # Create the 'when' expression based on the provided conditions and type
        if self.is_column_condition is None:
            case_expr = when(comparison_func(df[self.input_column], self.conditions[0][0]), self.parse_result_value(self.conditions[0][1]))
            for cond_val, result_val in self.conditions[1:]:
                case_expr = case_expr.when(comparison_func(df[self.input_column], cond_val), self.parse_result_value(result_val))

            # Apply the 'otherwise' function to specify the default value (None for the last condition)
            case_expr = case_expr.otherwise(None)

            return df.withColumn(self.output_column, case_expr)
        else:
            case_expr = when(comparison_func(df[self.input_column], df[self.conditions[0][0]]), self.parse_result_value(self.conditions[0][1]))
            for cond_val, result_val in self.conditions[1:]:
                case_expr = case_expr.when(comparison_func(df[self.input_column], cond_val), self.parse_result_value(result_val))

            # Apply the 'otherwise' function to specify the default value (None for the last condition)
            case_expr = case_expr.otherwise(self.otherwise_value)

            return df.withColumn(self.output_column, case_expr)
    
    def parse_result_value(self, result_value):
        if isinstance(result_value,str) and 'col:' in result_value:
            return col(result_value[4:])
        else:
            return(result_value)

class CastColumnsTransformer(Transformer):
    """
    A transformer to cast columns in a DataFrame to specified data types.

    :param columns_to_cast: A dictionary where the keys are the target column names
                            and the values are lists containing the source column name
                            and the target data type to which the source column will be cast.
                            Example: {'target_column': ['source_column', 'target_data_type']}
    """

    def __init__(self, columns_to_cast):
        self.columns_to_cast = columns_to_cast

    @classmethod
    def from_config(cls, config):
        columns_to_cast = config.get("columns_to_cast")
        obj = cls(columns_to_cast)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Cast columns in the DataFrame to specified data types.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the specified columns cast to the target data types.
        """
        for target, [source, target_data_type] in self.columns_to_cast.items():
            df = df.withColumn(target, F.col(source).cast(target_data_type))

        return df

class CoalesceTransformer(Transformer):
    """
    A transformer to create a new column by coalescing multiple columns.

    :param input_columns: A list of column names to coalesce.
    :param output_column: The name of the new column to create with the coalesced values.
    """

    def __init__(self, input_columns, output_column):
        self.input_columns = input_columns
        self.output_column = output_column

    @classmethod
    def from_config(cls, config):
        input_columns = config.get('input_columns')
        output_column = config.get('output_column')

        obj = cls(input_columns, output_column)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Create a new column by coalescing multiple columns.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the coalesced column.
        """

        return df.withColumn(self.output_column, F.coalesce(*self.input_columns))

class CrossJoinTransformer(Transformer):
    """
    A transformer to join two DataFrames together based on a specified join condition.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'join_type': The type of join to perform. Currently only 'inner' join is supported.
                   - 'join_on': A dictionary where the keys are the table names (or DataFrame aliases)
                                and the values are the column names to join on for each table.
                   Example: {'table1': 'column1', 'table2': 'column2'}
    """

    def __init__(self):
        self = self

    @classmethod
    def from_config(cls, config):
        obj = cls()
        obj.input_block = config.get('input_block')
        return obj

    # @property
    # def dependencies(self):
    #     """
    #     Get the list of table names that this transformer depends on.

    #     :return: A list of table names (keys from the 'join_on' dictionary).
    #     """
    #     return [key for key in self.config.get("join_on").keys()]
    @property
    def dependencies(self):
        return self.config.get('input_block')

    def transform(self, df1, df2):
        """
        Join two DataFrames together based on the specified join condition.

        :param df1: The first DataFrame to be joined.
        :param df2: The second DataFrame to be joined.
        :return: A new DataFrame resulting from the join operation.
        :raises: SerraRunException if the join condition columns do not match between the two DataFrames.
        """
        
        return df1.alias('a').crossJoin(df2.alias('b'))

class DateTruncTransformer(Transformer):
    """
    A transformer to truncate a timestamp column to a specified unit.

    :param timestamp_column: The name of the timestamp column to be truncated.
    :param trunc_unit: The unit for truncating the timestamp (e.g., 'day', 'month', 'year').
    :param output_column: The name of the new column to create with the truncated timestamps.
    """

    def __init__(self, timestamp_column, trunc_unit, output_column):
        self.timestamp_column = timestamp_column
        self.trunc_unit = trunc_unit
        self.output_column = output_column

    @classmethod
    def from_config(cls, config):
        timestamp_column = config.get("timestamp_column")
        trunc_unit = config.get("trunc_unit")
        output_column = config.get('output_column')

        obj = cls(timestamp_column, trunc_unit, output_column)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Truncate the specified timestamp column to the specified unit.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the truncated timestamp column.
        """
        dt = F.to_timestamp(self.timestamp_column, "yyyy-MM-dd HH:mm:ss")
        
        if self.trunc_unit == "day":
            truncated_time = F.date_trunc("day", dt)
        elif self.trunc_unit == "month":
            truncated_time = F.date_trunc("month", dt)
        elif self.trunc_unit == "year":
            truncated_time = F.date_trunc("year", dt)
        else:
            truncated_time = None
        
        return df.withColumn(self.output_column, truncated_time)

class DropColumnsTransformer(Transformer):
    """
    A transformer to drop specified columns from the DataFrame.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following key:
                   - 'columns_to_drop': A list of column names to be dropped from the DataFrame.
    """

    def __init__(self, columns_to_drop):
        self.columns_to_drop = columns_to_drop

    @classmethod
    def from_config(cls, config):
        columns_to_drop = config.get("columns_to_drop")
        obj = cls(columns_to_drop)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Drop specified columns from the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the specified columns dropped.
        """
        return df.select([c for c in df.columns if c not in self.columns_to_drop])

class FilterTransformer(Transformer):
    """
    A transformer to filter the DataFrame based on a specified condition.

    :param filter_values: A list of values to filter the DataFrame by.
    :param filter_column: The name of the column to apply the filter on.
    :param is_expression: Whether the filter is an expression (boolean condition).
    """

    def __init__(self, filter_values, filter_column, is_expression=False):
        self.filter_values = filter_values
        self.filter_column = filter_column
        self.is_expression = is_expression

    @classmethod
    def from_config(cls, config):
        filter_values = config.get("filter_values")
        filter_column = config.get("filter_column")
        is_expression = config.get("is_expression", False)
        obj = cls(filter_values, filter_column, is_expression)
        obj.input_block = config.get('input_block')
        return obj
        

    def transform(self, df):
        """
        Filter the DataFrame based on the specified condition.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with rows filtered based on the specified condition.
        """
        if self.is_expression:
            return df.filter(F.expr(self.filter_values))

        return df.filter(df[self.filter_column].isin(self.filter_values))

class GetCountTransformer(Transformer):
    """
    A transformer to get the count of occurrences of a column in the DataFrame.

    :param group_by_columns: A list of column names to group the DataFrame by.
    :param count_column: The name of the column for which the count is to be calculated.
    """

    def __init__(self, group_by_columns, count_column):
        self.group_by_columns = group_by_columns
        self.count_column = count_column

    @classmethod
    def from_config(cls, config):
        group_by_columns = config.get("group_by_columns")
        count_column = config.get("count_column")

        obj = cls(group_by_columns, count_column)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Calculate the count of occurrences of the specified column in the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the count of occurrences of the specified column
                 for each group defined by the 'group_by' column(s).
        """

        return df.groupBy(*self.group_by_columns).agg(F.count(self.count_column))

class GetDistinctTransformer(Transformer):
    """
    A transformer to drop duplicate rows from the DataFrame based on specified column(s).

    :param columns_to_check: A list of column names to identify rows for duplicates removal.
    """

    def __init__(self, columns_to_check):
        self.columns_to_check = columns_to_check

    @classmethod
    def from_config(cls, config):
        columns_to_check = config.get('columns_to_check')

        obj = cls(columns_to_check)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Drop duplicate rows from the DataFrame based on specified columns.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with duplicate rows removed based on the specified columns.
        """

        return df.dropDuplicates(self.columns_to_check)

class GetMaxOrMinTransformer(Transformer):
    """
    Test transformer to add a column to the dataframe with the maximum or minimum value from another column.

    :param columns_and_operations: A dictionary specifying the column to be transformed and the operation to be performed.
                                   The dictionary format should be {'input_column': 'aggregation_type'}.
                                   The output_column will contain the result of the aggregation operation.
    :param new_column_names: A dictionary specifying the names of the new columns to create for each aggregation operation.
                             The dictionary format should be {'aggregation_type': 'new_column_name'}.
    :param group_by_columns: A list of column names to group the DataFrame by.
    """

    def __init__(self, columns_and_operations, new_column_names, group_by_columns):
        self.columns_and_operations = columns_and_operations
        self.new_column_names = new_column_names
        self.group_by_columns = group_by_columns

    @classmethod
    def from_config(cls, config):
        columns_and_operations = config.get('columns_and_operations')
        new_column_names = config.get('new_column_names')
        group_by_columns = config.get("group_by_columns")

        obj = cls(columns_and_operations, new_column_names, group_by_columns)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Add a column with the maximum or minimum value to the DataFrame.

        :param df: The input DataFrame.
        :return: A new DataFrame with an additional column containing the maximum or minimum value.
        """
        window_spec = Window.partitionBy(self.group_by_columns)

        i = 0
        for input_col_name, agg_type in self.columns_and_operations.items():
            if agg_type == 'max':
                result_col = max(input_col_name).over(window_spec)
            elif agg_type == 'min':
                result_col = min(input_col_name).over(window_spec)
            
            df = df.withColumn(self.new_column_names[i], result_col)
            i+=1
        
        return df

class ImputeTransformer(Transformer):
    """
    A transformer to clean/impute missing values in specified columns.

    :param columns_to_impute: A list of column names to impute missing values.
    :param imputation_strategy: The imputation strategy. Supported values: 'mean', 'median', 'mode'.
    """

    def __init__(self, columns_to_impute, imputation_strategy):
        self.columns_to_impute = columns_to_impute
        self.imputation_strategy = imputation_strategy

    @classmethod
    def from_config(cls, config):
        columns_to_impute = config.get("columns_to_impute")
        imputation_strategy = config.get('imputation_strategy')

        obj = cls(columns_to_impute, imputation_strategy)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Add column with col_value to dataframe
        :return; Dataframe w/ new column containing col_value
        """

        imputer = Imputer(inputCols=self.columns_to_impute,
                        outputCols=["{}_imputed".format(c) for c in self.columns_to_impute]
                        ).setStrategy(self.imputation_strategy)

        df_imputed = imputer.fit(df).transform(df)
        df_imputed = df_imputed.drop(*self.columns_to_impute)
        
        # Rename back to original columns
        for c in self.columns_to_impute:
            df_imputed = df_imputed.withColumnRenamed("{}_imputed".format(c), c)

        return df_imputed

class JoinWithConditionTransformer(Transformer):
    """
    A transformer to join two DataFrames together based on a specified join condition.

    :param join_type: The type of join to perform. Currently only 'inner' join is supported.
    :param condition: The condition for the join.
    :param join_on: A dictionary where the keys are the table names (or DataFrame aliases)
                    and the values are the column names to join on for each table.
                    Example: {'table1': 'column1', 'table2': 'column2'}
    """

    def __init__(self, join_type, condition, join_on):
        self.join_type = join_type
        self.condition = condition
        self.join_on = join_on

    @classmethod
    def from_config(cls, config):
        join_type = config.get("join_type")
        condition = config.get('condition')
        join_on = config.get("join_on")

        obj = cls(join_type, condition, join_on)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df1, df2):
        """
        Join two DataFrames together based on the specified join condition.

        :param df1: The first DataFrame to be joined.
        :param df2: The second DataFrame to be joined.
        :return: A new DataFrame resulting from the join operation.
        :raises: SerraRunException if the join condition columns do not match between the two DataFrames.
        """
        
        df1 = df1.alias('a').join(df2.alias('b'), F.expr(self.condition), self.join_type)

        return df1

class MapTransformer(Transformer):
    """
    A transformer to map values in a DataFrame column to new values based on a given mapping dictionary.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'output_column': The name of the new column to be added after mapping.
                   - 'mapping_dictionary': A dictionary containing the mapping of old values to new values.
                                 If 'mapping_dictionary' is not provided, 'mapping_dict_path' should be specified.
                   - 'mapping_dict_path': The path to a JSON file containing the mapping dictionary.
                   - 'input_column': The name of the DataFrame column to be used as the key for mapping.
    """
    def __init__(self, output_column, input_column, mapping_dictionary=None, mapping_dict_path=None):
        self.output_column = output_column
        self.mapping_dictionary = mapping_dictionary
        self.mapping_dict_path = mapping_dict_path
        self.input_column = input_column

    @classmethod
    def from_config(cls, config): 
        print(config)
        output_col = config.get("output_column")
        map_dict = config.get("mapping_dictionary")
        map_dict_path = config.get("mapping_dict_path")
        input_col = config.get('input_column')
        obj = cls(output_col, map_dict, map_dict_path, input_col)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Map values in the DataFrame column to new values based on the specified mapping.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the new column containing the mapped values.
        :raises: SerraRunException if any required config parameter is missing or if column specified
                 as 'input_column' does not exist in the DataFrame.
        """
        if not self.output_column or not self.input_column:
            raise SerraRunException("Both 'output_column' and 'input_column' must be provided in the config.")
        
        if not self.mapping_dictionary and not self.mapping_dict_path:
            raise SerraRunException("Either 'mapping_dictionary' or 'mapping_dict_path' must be provided in the config.")

        if self.input_column not in df.columns:
            raise SerraRunException(f"Column '{self.input_column}' specified as input_column does not exist in the DataFrame.")

        if self.mapping_dictionary is None:
            with open(self.mapping_dict_path) as f:
                self.mapping_dictionary = json.load(f)

        try:
            for key, value in self.mapping_dictionary.items():
                df = df.withColumn(f'{self.output_column}_{key}', F.when(F.col(self.input_column) == key, value))

            # Select the first non-null value from the generated columns
            # create list, then unpack *
            df = df.withColumn(self.output_column, F.coalesce(*[F.col(f'{self.output_column}_{key}') for key in self.mapping_dictionary]))
            df = df.drop(*[f'{self.output_column}_{key}' for key in self.mapping_dictionary])
        except Exception as e:
            raise SerraRunException(f"Error transforming DataFrame: {str(e)}")

        return df

class MultiJoinTransformer(Transformer):
    """
    A transformer to join multiple DataFrames together based on specified join conditions.

    :param join_type: The type of join to perform. Currently only 'inner' join is supported.
    :param join_on: A dictionary where the keys are the table names (or DataFrame aliases)
                    and the values are the column names to join on for each table.
                    Example: {'table1': 'column1', 'table2': 'column2'}
    """

    def __init__(self, join_type, join_on):
        self.join_type = join_type
        self.join_on = join_on

    @classmethod
    def from_config(cls, config):
        join_type = config.get("join_type")
        join_on = config.get("join_on")

        obj = cls(join_type, join_on)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, *dfs):
        """
        Join multiple DataFrames together based on the specified join conditions.

        :param dfs: A variable number of DataFrames to be joined.
        :return: A new DataFrame resulting from the join operations.
        :raises: SerraRunException if the join condition columns do not match between the DataFrames.
        """  

        join_keys = []
        for table in self.join_on:
            join_keys.append(self.join_on.get(table))
            

        matching_col = join_keys

        joined_df = dfs[0]
        for i in range(1,len(dfs)):
            if matching_col[i-1] != matching_col[i]:
                joined_df = joined_df.join(dfs[i], joined_df[matching_col[i-1]] == dfs[i][matching_col[i]], self.join_type[i-1])
            else:
                joined_df = joined_df.join(dfs[i], matching_col[i], self.join_type[i-1])

            non_null_counts = [joined_df.where(col(c).isNotNull()).count() for c in dfs[i].columns]
            if joined_df.isEmpty() or all(count == 0 for count in non_null_counts):
                raise SerraRunException(f"""Joiner - Join Key Error: {matching_col[0]}, {matching_col[1]} columns do not match.""")

        return joined_df

class MultipleCaseWhenTransformer(Transformer):
    """
    A transformer that adds a new column to the DataFrame based on conditional rules (similar to SQL's CASE WHEN).

    :param columns_and_conditions: A list of tuples representing the conditions and their corresponding results.
                                   Each tuple should be in the format (condition_value, result_value).
                                   The condition_value can be a specific value or a pattern for LIKE comparisons.
                                   The result_value will be assigned to the output_col if the condition is met.
    :param input_column: The name of the column containing the values to be evaluated.
    :param output_col: The name of the new column to be added with the results of the conditions.
    :param type: The type of comparison to be used. It can be either '==' for equality comparison or 'like' for pattern matching using the LIKE operator.
    """

    def __init__(self, columns_and_conditions, input_column, output_col, type):
        self.columns_and_conditions = columns_and_conditions
        self.input_column = input_column
        self.output_col = output_col
        self.type = type

    @classmethod
    def from_config(cls, config):
        columns_and_conditions = config.get('columns_and_conditions')
        input_column = config.get("input_column")
        output_col = config.get('output_col')
        type = config.get('type')

        obj = cls(columns_and_conditions, input_column, output_col, type)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Add a new column with the results of the conditions to the DataFrame.

        :param df: The input DataFrame.
        :return: A new DataFrame with an additional column containing the results of the conditions.
        """
        # Mappings to indicate which comparison function to use
        type_dict = {
            '<=': lambda col, val: col <= val,
            '>=': lambda col, val: col >= val,
            '==': lambda col, val: col == val,
            'like': lambda col, val: col.like(val)
        }

        # Get the particular condition function based on the comparison type
        comparison_func = type_dict.get(self.type)
        output_cols = list(self.columns_and_conditions.keys())
        conditions = list(self.columns_and_conditions.values())

        if comparison_func is None:
            raise ValueError(f"Unsupported comparison type: {self.type}")
        
        for i in range(len(output_cols)):
            if len(conditions[i]) == 2:
                case_expr = when(comparison_func(df[self.input_column[0]], conditions[i][0]), self.parse_result_value(conditions[i][1]))
            else: 
                case_expr = when(comparison_func(df[self.input_column[0]], conditions[i][0]) | comparison_func(df[self.input_column[0]], conditions[i][2]), self.parse_result_value(conditions[i][1]))
            case_expr = case_expr.otherwise(None)
            df = df.withColumn(output_cols[i], case_expr)

        return df
    
    def parse_result_value(self, result_value):
        if 'col:' in result_value:
            return col(result_value[4:])
        else:
            return(result_value)

class OrderByTransformer(Transformer):
    """
    A transformer to sort the DataFrame based on specified columns in ascending or descending order.

    :param columns: A list of column names to sort the DataFrame by.
    :param ascending: Optional. If True (default), sort in ascending order. If False, sort in descending order.
    """

    def __init__(self, columns, ascending=True):
        self.columns = columns
        self.ascending = ascending

    @classmethod
    def from_config(cls, config):
        columns = config.get("columns")
        ascending = config.get("ascending", True)

        obj = cls(columns, ascending)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Transform the DataFrame by sorting it based on specified columns.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the rows sorted based on the specified columns.
        """

        return df.orderBy(*self.columns, ascending = self.ascending)

class PivotTransformer(Transformer):
    """
    A transformer to pivot a DataFrame based on specified row and column levels, and perform aggregation.

    :param row_level_column: The column used for row levels during pivoting.
    :param column_level_column: The column used for column levels during pivoting.
    :param value_column: The column to be summarized (values) during pivoting.
    :param aggregate_type: The type of aggregation to perform after pivoting.
                           Should be one of 'avg' (average) or 'sum' (sum).
    """

    def __init__(self, row_level_column, column_level_column, value_column, aggregate_type):
        self.row_level_column = row_level_column
        self.column_level_column = column_level_column
        self.value_column = value_column
        self.aggregate_type = aggregate_type

    @classmethod
    def from_config(cls, config):
        row_level_column = config.get("row_level_column")
        column_level_column = config.get("column_level_column")
        value_column = config.get("value_column")
        aggregate_type = config.get("aggregate_type")

        obj = cls(row_level_column, column_level_column, value_column, aggregate_type)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Pivot the DataFrame based on the specified row and column levels, and perform aggregation.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame resulting from the pivot operation with the specified aggregation.
        :raises: SerraRunException if the specified aggregation type is invalid.
        """
        
        df = df.withColumn(self.value_column, F.col(self.value_column).cast("double"))
        df = df.groupBy(self.row_level_column).pivot(self.column_level_column)

        # Perform aggregation
        if self.aggregate_type == "avg":
            df = df.avg(self.value_column)
        elif self.aggregate_type == "sum":
            df = df.sum(self.value_column)
        else:
            raise SerraRunException("Invalid Pivot Aggregation type")

        return df

class RenameColumnTransformer(Transformer):
    """
    A transformer to rename a column in a DataFrame.

    :param old_name: The name of the column to be renamed.
    :param new_name: The new name to be assigned to the column.
    """

    def __init__(self, old_name, new_name):
        self.old_name = old_name
        self.new_name = new_name

    @classmethod
    def from_config(cls, config):
        old_name = config.get("old_name")
        new_name = config.get("new_name")

        obj = cls(old_name, new_name)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df):
        """
        Rename a column in the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with the specified column renamed.
        """

        df = df.withColumn(
            self.new_name, F.col(self.old_name)
        )
        return df.drop(F.col(self.old_name))

class SelectTransformer(Transformer):
    """
    A transformer to perform a SELECT operation on a DataFrame.

    :param columns: A list of column names to select from the DataFrame.
    :param distinct_column: Optional. The column for which distinct values should be retained.
    :param filter_expression: Optional. A filter expression to apply to the DataFrame.
    """

    def __init__(self, columns=None, distinct_column=None, filter_expression=None):
        self.columns = columns or []
        self.distinct_column = distinct_column
        self.filter_expression = filter_expression

    @classmethod
    def from_config(cls, config):
        columns = config.get("columns", [])
        distinct_column = config.get('distinct_column')
        filter_expression = config.get('filter_expression')

        obj = cls(columns, distinct_column, filter_expression)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Perform the SELECT operation on the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame containing only the selected columns.
        :raises: SerraRunException if no columns are specified in the configuration
                 or if none of the specified columns exist in the DataFrame.
        """
        if not self.columns:
            raise SerraRunException("No columns specified in the configuration.")

        selected_columns = [F.col(col) for col in self.columns if col in df.columns]

        if not selected_columns:
            raise SerraRunException("None of the specified columns exist in the DataFrame.")

        df = df.select(*selected_columns)
        
        if self.distinct_column is not None:
            df = df.dropDuplicates(self.distinct_column)

        if self.filter_expression is not None:
            df = df.filter(F.expr(self.filter_expression))
        return df

class SQLTransformer(Transformer):
    """
    A transformer to perform a SQL SELECT operation on a DataFrame.

    :param sql_expression: A SQL expression string representing the SELECT operation.
    """

    def __init__(self, sql_expression):
        self.sql_expression = sql_expression

    @classmethod
    def from_config(cls, config):
        sql_expression = config.get('sql_expression')

        obj = cls(sql_expression)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Perform the SELECT operation on the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame containing only the selected columns.
        :raises: SerraRunException if no columns are specified in the configuration
                 or if none of the specified columns exist in the DataFrame.
        """
        df = df.filter(F.expr(self.sql_expression))
        return df

class WindowTransformer(Transformer):
    """
    A transformer to set a window for all other steps after it.

    :param partition_by: A list of column names for partitioning the window.
    :param order_by: A list of column names for ordering within the window.
    :param window_name: The name of the window column to be added.
    """

    def __init__(self, partition_by=None, order_by=None, window_name=None):
        self.partition_by = partition_by or []
        self.order_by = order_by or []
        self.window_name = window_name

    @classmethod
    def from_config(cls, config):
        partition_by = config.get("partition_by", [])
        order_by = config.get("order_by", [])
        window_name = config.get("window_name")

        obj = cls(partition_by, order_by, window_name)
        obj.input_block = config.get('input_block')
        return obj

    def transform(self, df: DataFrame) -> DataFrame:
        window_spec = Window().partitionBy(*self.partition_by).orderBy(*self.partition_by)
        df_with_window = df.withColumn(f"window_{self.partition_by[0]}", F.row_number().over(window_spec))
        return df_with_window

class LocalWriter(Writer):
    """
    A writer to write data from a Spark DataFrame to a local file.

    :param file_path: The path of the local file to write to.
    """

    def __init__(self, file_path):
        self.file_path = file_path

    @classmethod
    def from_config(cls, config):
        file_path = config.get("file_path")
        obj = cls(file_path)
        obj.input_block = config.get('input_block')
        return obj
    
    def write(self, df):
        """
        Write data from a Spark DataFrame to a local file.

        :param df: The Spark DataFrame to be written to the local file.
        """
        # Convert PySpark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()

        # Write the Pandas DataFrame to a local file
        pandas_df.to_csv(self.file_path, index=False)

class DatabricksWriter(Writer):
    """
    A writer to write data from a Spark DataFrame to a Databricks Delta table.

    :param config: A dictionary containing the configuration for the writer.
                   It should have the following keys:
                   - 'database': The name of the Databricks database to write to.
                   - 'table': The name of the table in the Databricks database.
                   - 'format': The file format to use for the Delta table.
                   - 'mode': The write mode to use, such as 'overwrite', 'append', etc.
    """

    def __init__(self, database, table, format, mode):
        self.database = database
        self.table = table
        self.format = format
        self.mode = mode

    @classmethod
    def from_config(cls, config):
        database = config.get('database')
        table = config.get('table')
        format = config.get('format')
        mode = config.get('mode')

        obj = cls(database, table, format, mode)
        obj.input_block = config.get('input_block')
        return obj
        
    def write(self, df: DataFrame):
        """
        Write data from a Spark DataFrame to a Databricks Delta table.

        :param df: The Spark DataFrame to be written to the Delta table.
        """
        # Currently forces overwrite if csv already exists
        df.write.format(self.format).mode(self.mode).saveAsTable(f'{self.database}.{self.table}')
        return None

