#flask code

from flask import Flask, request, jsonify
import requests
import json
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

@app.route('/ask', methods=['POST'])
def ask():
    question = request.json.get('question')
    if not question:
        return jsonify({"error": "No question provided"}), 400

    base_url = os.getenv("DATABRICKS_BASE_URL")
    token = os.getenv("DATABRICKS_TOKEN")
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Start the job run
    run_now_url = f"{base_url}/jobs/run-now"
    payload = {
        "job_id": 1065737057597852,
        "creator_user_name": "shachak@apac.corpdir.net",
        "run_as_user_name": "shachak@apac.corpdir.net",
        "run_as_owner": True,
        "notebook_params": {
            "question": question
        },
        "settings": {
            "name": "New Job 2024-07-25 15:45:48",
            "email_notifications": {
                "no_alert_for_skipped_runs": False
            },
            "webhook_notifications": {},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "Run-notebook",
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": "/Workspace/Users/shachak@apac.corpdir.net/Sales-usecase",
                        "source": "WORKSPACE"
                    },
                    "existing_cluster_id": "0724-071458-4e98ju6j",
                    "timeout_seconds": 0,
                    "email_notifications": {}
                }
            ],
            "format": "MULTI_TASK",
            "queue": {
                "enabled": True
            }
        },
        "created_time": 1721902549326
    }
    response = requests.post(run_now_url, headers=headers, json=payload)
    if response.status_code != 200:
        return jsonify({"error": "Failed to run notebook", "details": response.text}), response.status_code

    run_id = response.json().get('run_id')

    # Wait for the job to complete
    get_run_url = f"{base_url}/jobs/runs/get"
    while True:
        run_response = requests.get(get_run_url, headers=headers, params={"run_id": run_id})
        run_info = run_response.json()
        if run_info['state']['life_cycle_state'] in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
            break
        time.sleep(10)  # Wait for 10 seconds before checking again

    # Fetch output for each task
    get_output_url = f"{base_url}/jobs/runs/get-output"
    all_outputs = []
    for task in run_info['tasks']:
        task_run_id = task['run_id']
        output_response = requests.get(get_output_url, headers=headers, params={"run_id": task_run_id})
        if output_response.status_code == 200:
            all_outputs.append(output_response.json())
        else:
            all_outputs.append({"error": f"Failed to get output for task {task['task_key']}"})

    return jsonify(all_outputs)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
