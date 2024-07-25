from flask import Flask, request, jsonify, send_file
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import io
import requests
import os
from mlflow.models import infer_signature
import mlflow

app = Flask(__name__)

DATABRICKS_INSTANCE = "https://adb-1145123843843530.10.azuredatabricks.net"
DATABRICKS_TOKEN = "dapi14e632c9dafbfaaa5f425aea828174a7-2"

# Chatbot logic
def chatbot_response(user_input):
    return f"You said: {user_input}"

class ChatbotModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        user_input = model_input.iloc[0]['user_input']
        return chatbot_response(user_input)

# Function to call Databricks API and run a notebook
def run_databricks_notebook(notebook_path, parameters):
    url = f"{DATABRICKS_INSTANCE}/api/2.0/jobs/run-now"
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }
    payload = {
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": parameters
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

# Data visualization logic
def foo(request_id, query):
    # Call Databricks notebook
    try:
        databricks_response = run_databricks_notebook("/Workspace/Users/karthna@apac.corpdir.net/flaskapi_connect_with_backend", {"param1": query})
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to run Databricks notebook: {e}")

    # Mock data for visualization
    data = {
        'Category': ['A', 'B', 'C', 'D'],
        'Values': [23, 45, 56, 28]
    }
    df = pd.DataFrame(data)

    plots = {}
    plt.figure(figsize=(10, 6))
    sns.barplot(x='Category', y='Values', data=df)
    img_bar = io.BytesIO()
    plt.savefig(img_bar, format='png')
    img_bar.seek(0)
    plots['bar'] = img_bar.getvalue()  # Store bytes directly
    plt.close()

    plt.figure(figsize=(10, 6))
    plt.plot(df['Category'], df['Values'])
    img_line = io.BytesIO()
    plt.savefig(img_line, format='png')
    img_line.seek(0)
    plots['line'] = img_line.getvalue()  # Store bytes directly
    plt.close()

    plt.figure(figsize=(6, 6))
    plt.pie(df['Values'], labels=df['Category'], autopct="%1.1f%%")
    img_pie = io.BytesIO()
    plt.savefig(img_pie, format='png')
    img_pie.seek(0)
    plots['pie'] = img_pie.getvalue()  # Store bytes directly
    plt.close()

    flag = 0
    remarks = "Here are some visualizations of your data."
    table_data = df.to_dict(orient='records')

    return flag, request_id, plots, remarks, table_data

# Route to process chatbot request
@app.route('/chatbot', methods=['POST'])
def chatbot():
    try:
        req_data = request.get_json()
        user_message = req_data.get('message')

        model = ChatbotModel()
        df = pd.DataFrame([{'user_input': user_message}])
        bot_response = model.predict(None, df)

        response = {
            'response': bot_response
        }
        return jsonify(response)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Route to process data visualization request
@app.route('/process_request', methods=['POST'])
def process_request():
    try:
        req_data = request.get_json()
        query = req_data.get('user_query')
        request_id = req_data.get('request_id')

        flag, request_id, plots, remarks, table_data = foo(request_id, query)

        # Store plot data in app config
        app.config['PLOTS'] = plots

        response = {
            'flag': flag,
            'request_id': request_id,
            'remarks': remarks,
            'table_data': table_data,
            'plot_urls': {k: f"/get_plot/{k}" for k in plots.keys()}  # URLs to fetch plots
        }
        return jsonify(response)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Route to serve plot images
@app.route('/get_plot/<plot_id>', methods=['GET'])
def get_plot(plot_id):
    try:
        plot_data = app.config['PLOTS'].get(plot_id)
        if plot_data is None:
            return jsonify({'error': 'Plot not found'}), 404
        return send_file(io.BytesIO(plot_data), mimetype='image/png')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.config['PLOTS'] = {}  # Initialize a dictionary to store plot data in memory
    app.run(host='0.0.0.0', port=8888, debug=True)
