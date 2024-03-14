import requests
from serra.python.base import PythonReader
import pandas as pd

class CmsReader(PythonReader):

    def __init__(self):
        pass
    
    def read(self):
        url = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
        response = requests.get(url)
        if response.status_code != 200:
            return False
        cms_files = response.json()
        
        # Sort files by date
        sorted_files = sorted(cms_files, key=lambda x: x['issued'], reverse=True)
        # Remove files we are not interested in
        filtered_files = [file for file in sorted_files if file['title'] == 'Provider Information']
        sorted_files = filtered_files
        # get most recent file
        most_recent_file_info = sorted_files[0]
        file_id = most_recent_file_info['identifier']

        get_file_url = f"https://data.cms.gov/provider-data/api/1/datastore/query/{file_id}/0?offset=0&count=true&results=true&schema=true&keys=true&format=json&rowIds=false"
        file_specific_response = requests.get(get_file_url)
        if file_specific_response.status_code != 200:
            return False
        file_output = file_specific_response.json()
        list_of_rows = file_output['results']

        # Convert to pandas 
        df = pd.DataFrame(list_of_rows)
        return df

