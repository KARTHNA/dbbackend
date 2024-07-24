from flask import Flask, request, jsonify
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import io
from mlflow.models import infer_signature
import mlflow

app = Flask(__name__)

# Chatbot logic
def chatbot_response(user_input):
    return f"You said: {user_input}"

class ChatbotModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        user_input = model_input.iloc[0]['user_input']
        return chatbot_response(user_input)

# Data visualization logic
def foo(request_id, query):
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
    print("Backend table data:", table_data)

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

        # Convert bytes to base64 in the response
        plots_base64 = {k: base64.b64encode(v).decode() for k, v in plots.items()}

        response = {
            'flag': flag,
            'request_id': request_id,
            'plots': plots_base64,
            'remarks': remarks,
            'table_data': table_data
        }
        return jsonify(response)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='localhost', port=8888, debug=True)

