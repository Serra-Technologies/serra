import serra # testing that this works
from flask import Flask, request
import yaml
from werkzeug.utils import secure_filename
import os
import subprocess

# Create an instance of the Flask class
app = Flask(__name__)

@app.route('/run', methods=['POST'])
def upload_yaml():
    # Check if a file is present in the request
    if 'file' not in request.files:
        return 'No file uploaded.', 400

    file = request.files['file']
    # Check if the file has a YAML extension
    if file.filename.endswith('.yaml') or file.filename.endswith('.yml'):
        try:
            data = yaml.safe_load(file)
            # Process the YAML data here
            # You can access the data as a dictionary
            # For example, print the content of the YAML file
            print(data)

            filename = secure_filename(file.filename)
            file_path = os.path.join('./serra/jobs', filename)

            with open(file_path, 'w') as f:
                yaml.safe_dump(data, f)

                        # Verify that the file was saved correctly
            if os.path.isfile(file_path):
                # Run command serra run_locally {config_name} and capture output
                bashCommand = "serra run_locally testing"
                process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
                output, error = process.communicate()

                # Send the output back in the response
                return output.decode('utf-8')

            else:
                return 'Error saving the file.', 500

            # bashCommand = "serra run_locally hi"
            # process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
            # output, error = process.communicate()
            # print(output)
            # return output
            # return 'YAML file uploaded and processed successfully.'
        except yaml.YAMLError:
            return 'Invalid YAML file.', 400
        
    else:
        return 'Invalid file format. Please upload a YAML file.', 400

# Define a route and its corresponding function
@app.route('/')
def hello_world():
    return 'Hello, World!'

# Run the application if the script is executed directly
if __name__ == '__main__':
    app.run()




"""
inside /upload endpoint ( which we should probable rename to /run)
* write the file data to a file in the jobs folder
* run command serra run_locally {config_name} and capture output
* send this output back in response (search or gpt how to send data for response back
in flask endpoint)
"""
