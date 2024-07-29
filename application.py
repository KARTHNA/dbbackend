from flask import Flask, request, jsonify
import requests
import json
import time
import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        "notebook_params": {
            "question": question
        }
    }
    response = requests.post(run_now_url, headers=headers, json=payload)
    if response.status_code != 200:
        logger.error(f"Failed to run notebook: {response.text}")
        return jsonify({"error": "Failed to run notebook", "details": response.text}), response.status_code

    run_id = response.json().get('run_id')
    logger.info(f"Started Databricks job with run_id: {run_id}")

    # Wait for the job to complete
    get_run_url = f"{base_url}/jobs/runs/get"
    while True:
        run_response = requests.get(get_run_url, headers=headers, params={"run_id": run_id})
        run_info = run_response.json()
        logger.info(f"Job status: {run_info['state']['life_cycle_state']}")
        if run_info['state']['life_cycle_state'] in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
            break
        time.sleep(3)  # Reduce wait time to 3 seconds

    # Fetch output for each task
    get_output_url = f"{base_url}/jobs/runs/get-output"
    all_outputs = []
    for task in run_info['tasks']:
        task_run_id = task['run_id']
        output_response = requests.get(get_output_url, headers=headers, params={"run_id": task_run_id})
        if output_response.status_code == 200:
            try:
                output_json = output_response.json()
                logger.info(f"Output for task {task['task_key']}: {output_json}")
                all_outputs.append(output_json)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON for task {task['task_key']}")
                all_outputs.append({"error": f"Failed to decode JSON for task {task['task_key']}"})
        else:
            logger.error(f"Failed to get output for task {task['task_key']}: {output_response.text}")
            all_outputs.append({"error": f"Failed to get output for task {task['task_key']}"})

    return jsonify(all_outputs)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
