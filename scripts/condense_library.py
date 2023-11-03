"""
This scripts will look at all the code in the following directories:
- ../serra/readers
- ../serra/writers
- ../serra/transformers

In the end we want to create a json file in the format
[
    {
    "block_type": The name of the class (for example S3Reader)
    "code_text": The code for that class ( for example we would copy all the text in ../serra/readers/s3_reader.py and place it here)
    },
    ...
]
"""

OUTPUT_FILE = "output.json"

import os
import json
import string

# Directories to search for code
directories = [
    "../serra/readers",
    "../serra/writers",
    "../serra/transformers"
]

# List to store the code snippets
code_snippets = []

# Iterate over the directories
for directory in directories:
    # Iterate over the files in the directory
    for filename in os.listdir(directory):
        # Read the code from each file
        if not os.path.isfile(os.path.join(directory, filename)):
            continue

        if (filename in ["reader.py", "transformer.py", "writer.py"]):
            # ignore the base classes
            continue

        with open(os.path.join(directory, filename), 'r') as file:
            code_text = file.read()
        
        # Create a dictionary with the block type and code text
        code_snippet = {
            "block_type": string.capwords(filename.split(".")[0].replace("_", " ")).replace(" ",""),
            "code_text": code_text
        }
        
        # Append the code snippet to the list
        code_snippets.append(code_snippet)

# Write the code snippets to the output file in a pretty format
with open(OUTPUT_FILE, 'w') as file:
    json.dump(code_snippets, file, indent=4)