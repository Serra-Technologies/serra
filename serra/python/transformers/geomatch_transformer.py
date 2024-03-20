from serra.python.base import PythonTransformer
import string
import re
import pandas as pd

class GeoMatchTransformer(PythonTransformer):
    """
    Clean whitespaces, cases, punctuation.

    """

    def __init__():
        pass
        
    def transform(self, df):
        """
        Get mapping dictionaries. Keys are whatever use case you want, Values are dictionaries with keys as column name and values as the functions that do that specific use case transform.
        Loop through key-value pairs for the specified dictionaries, assign df[key] = value.

        """

        # Assuming facilities_list is a list of dictionaries where each dictionary represents a facility
        facilities_list = [
            {'Facility_ID': 1, 'Facility_Name': 'Facility_1', 'Revenue': 31952.54, 'Latitude': 48.850072, 'Longitude': -104.010627, 'Zip_Code': 25127},
            {'Facility_ID': 2, 'Facility_Name': 'Facility_2', 'Revenue': 38607.57, 'Latitude': 44.365722, 'Longitude': -99.517059, 'Zip_Code': 47237},
            {'Facility_ID': 3, 'Facility_Name': 'Facility_3', 'Revenue': 34110.54, 'Latitude': 35.927777, 'Longitude': -84.411807, 'Zip_Code': 89701},
            {'Facility_ID': 4, 'Facility_Name': 'Facility_4', 'Revenue': 31795.33, 'Latitude': 43.900210, 'Longitude': -121.358093, 'Zip_Code': 18752},
            {'Facility_ID': 5, 'Facility_Name': 'Facility_5', 'Revenue': 26946.19, 'Latitude': 27.351755, 'Longitude': -86.200821, 'Zip_Code': 90041},
            # Add the rest of the facilities in a similar manner
        ]

        # Converting the list of dictionaries to a DataFrame
        facilities_df = pd.DataFrame(facilities_list)
        return(facilities_df)



