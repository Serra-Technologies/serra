import requests
import os

# Constants
BASE_URL = "********"
TOKEN = '******'
HEADERS = {
    "Authorization": TOKEN
}
SCRIPT = "py_script.py"
DBFS_PATH = "dbfs:/scripts/t.py"

def upload_script_to_dbfs(script_path, dbfs_path):
    """Uploads a script to DBFS."""
    with open(script_path, "rb") as f:
        script_content = f.read()

    response = requests.post(
        f"{BASE_URL}/api/2.0/dbfs/put",
        headers=HEADERS,
        files={
            'path': (None, dbfs_path),
            'content': (os.path.basename(script_path), script_content),
            'overwrite': (None, 'true')
        }
    )

    if response.status_code == 200:
        print(f"Script uploaded successfully to {dbfs_path}")
    else:
        print(f"Error uploading script: {response.content}")
        return False

    return True

def create_and_run_job(dbfs_path):
    """Creates and runs a Databricks job."""
    # Create a new job
    response = requests.post(
        f"{BASE_URL}/api/2.0/jobs/create",
        headers=HEADERS,
        json={
            "name": "MyJob3",
            "existing_cluster_id": "0922-180514-m6fcc4kt",
            "spark_python_task": {
                "python_file": dbfs_path
            }
        }
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    # Run the job
    response = requests.post(
        f"{BASE_URL}/api/2.0/jobs/run-now",
        headers=HEADERS,
        json={"job_id": job_id}
    )
    assert response.status_code == 200

if __name__ == "__main__":
    if upload_script_to_dbfs(SCRIPT, DBFS_PATH):
        create_and_run_job(DBFS_PATH)
