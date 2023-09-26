from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from geopy.distance import geodesic
from serra.transformers.transformer import Transformer

class GeoDistanceTransformer(Transformer):
    """
    A transformer to calculate distances between pairs of geographical coordinates.

    :param start_column: The name of the column containing user coordinates.
    :param end_column: The name of the column containing facility coordinates.
    :param distance_km_col: The name of the column to store the calculated distance in kilometers.
    :param distance_mi_col: The name of the column to store the calculated distance in miles.
    """

    def __init__(self, start_column, end_column, distance_km_col, distance_mi_col):
        self.start_column = start_column
        self.end_column = end_column
        self.distance_km_col = distance_km_col
        self.distance_mi_col = distance_mi_col

    @classmethod
    def from_config(cls, config):
        start_column = config.get("start_column")
        end_column = config.get("end_column")
        distance_km_col = config.get("distance_km_col")
        distance_mi_col = config.get("distance_mi_col")

        obj = cls(start_column, end_column, distance_km_col, distance_mi_col)
        obj.input_block = config.get('input_block')
        return obj
        
    def transform(self, df):
        """
        Calculate distances between pairs of coordinates and add them to the DataFrame.

        :param df: The input DataFrame to be transformed.
        :return: A new DataFrame with calculated distances added as new columns.
        """
        # UDF to calculate distance in kilometers
        def calculate_distance_km(user_coords, facility_coords):
            user_lat, user_lon = map(float, user_coords.split(','))
            facility_lat, facility_lon = map(float, facility_coords.split(','))
            return geodesic((user_lat, user_lon), (facility_lat, facility_lon)).kilometers

        calculate_distance_km_udf = F.udf(calculate_distance_km, FloatType())

        # UDF to calculate distance in miles
        def calculate_distance_mi(user_coords, facility_coords):
            user_lat, user_lon = map(float, user_coords.split(','))
            facility_lat, facility_lon = map(float, facility_coords.split(','))
            return geodesic((user_lat, user_lon), (facility_lat, facility_lon)).miles

        calculate_distance_mi_udf = F.udf(calculate_distance_mi, FloatType())

        # Calculate distances and add them to the DataFrame
        df_with_distances = df.withColumn(
            'distance_km_col',
            calculate_distance_km_udf(df[self.start_column], df[self.end_column])
        ).withColumn(
            'distance_mi_col',
            calculate_distance_mi_udf(df[self.start_column], df[self.end_column])
        )

        return df_with_distances