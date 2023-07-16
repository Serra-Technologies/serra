import serra # testing that this works
from flask import Flask, request
import yaml

# Create an instance of the Flask class
app = Flask(__name__)

@app.route('/upload', methods=['POST'])
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
            return 'YAML file uploaded and processed successfully.'
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