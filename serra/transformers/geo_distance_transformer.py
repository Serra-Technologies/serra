from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from geopy.distance import geodesic
from serra.transformers.transformer import Transformer

class GeoDistanceTransformer(Transformer):
    """
    A transformer to calculate distances between pairs of geographical coordinates.

    :param config: A dictionary containing the configuration for the transformer.
                   It should have the following keys:
                   - 'user_coordinates_col': The name of the column containing user coordinates.
                   - 'facility_coordinates_col': The name of the column containing facility coordinates.
                   - 'distance_km_col': The name of the column to store the calculated distance in kilometers.
                   - 'distance_mi_col': The name of the column to store the calculated distance in miles.
    """

    def __init__(self, config):
        self.config = config
        self.user_coordinates_col = config.get("first_col")
        self.facility_coordinates_col = config.get("second_col")
        
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
            calculate_distance_km_udf(df[self.user_coordinates_col], df[self.facility_coordinates_col])
        ).withColumn(
            'distance_mi_col',
            calculate_distance_mi_udf(df[self.user_coordinates_col], df[self.facility_coordinates_col])
        )

        return df_with_distances