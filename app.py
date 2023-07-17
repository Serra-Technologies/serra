import serra # testing that this works
from flask import Flask, request, session, redirect, url_for
import yaml
from werkzeug.utils import secure_filename
import os
import subprocess
from serra.cli import run_locally_with_function
from loguru import logger
from serra.logger import get_io_buffer

logger.add(get_io_buffer(), format="<green>{time}</green> - <level>{level}</level> - <cyan>{message}</cyan>", colorize=True)

# Create an instance of the Flask class
app = Flask(__name__)
app.secret_key = "here's some secret key"

@app.route('/run', methods=['POST'])
def upload_yaml():
    # Check if a file is present in the request
    if 'file' not in request.files:
        return 'No file uploaded.', 400

    # Get session info
    session_id = request.form['session_id']
    
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
            new_file_name = f"{session_id}_{filename}"
            file_base_name = new_file_name.split(".")[0]

            file_path = os.path.join('./serra/jobs', new_file_name)

            with open(file_path, 'w') as f:
                yaml.safe_dump(data, f)

                        # Verify that the file was saved correctly
            if os.path.isfile(file_path):
                # Run command serra run_locally {config_name} and capture output
                # bashCommand = f"serra run_locally {file_base_name}"
                # process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
                # output, error = process.communicate()

                # # Send the output back in the response
                # return output.decode('utf-8')
                run_locally_with_function(file_base_name)
                logs = get_io_buffer().getvalue() # Get the logs
                get_io_buffer().truncate(0)
                return logs

            else:
                return 'Error saving the file.', 500
        except yaml.YAMLError:
            return 'Invalid YAML file.', 400
        
    else:
        return 'Invalid file format. Please upload a YAML file.', 400

@app.route('/api', methods=['POST'])
def api():
    # Get the session ID from the request
    session_id = request.json['session_id']
    
    # Retrieve the session data based on the session ID
    session_data = session.get(session_id, {})
    
    # Access and modify session data as needed
    session_data['counter'] = session_data.get('counter', 0) + 1
    
    # Store the updated session data
    session[session_id] = session_data
    
    # Return a response with the updated session data or any other information
    return {'counter': session_data['counter']}

# Define a route and its corresponding function
@app.route('/')
def hello_world():
    return 'Hello, World!'

# Run the application if the script is executed directly
if __name__ == '__main__':
    app.run(port=8000)




"""
inside /upload endpoint ( which we should probable rename to /run)
* write the file data to a file in the jobs folder
* run command serra run_locally {config_name} and capture output
* send this output back in response (search or gpt how to send data for response back
in flask endpoint)
"""
