import json
import urllib3
import certifi
import logging
import chromadb

# Constants
OPENAI_API_KEY = ''
API_URL = ""

def get_embedding(text):
    """Fetches the embedding for a given piece of text using OpenAI's API."""
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {OPENAI_API_KEY}'}
    data = {'input': text, 'model': 'text-embedding-ada-002'}
    encoded_data = json.dumps(data).encode('utf-8')

    response = http.request('POST', 'https://api.openai.com/v1/embeddings', headers=headers, body=encoded_data)
    response_data = json.loads(response.data.decode('utf-8'))
    return response_data['data'][0]['embedding']

def search_similar_embeddings(embedding, collection_name):
    """Search for similar embeddings in ChromaDB using the given embedding."""
    CHROMADB_HOST = ''
    CHROMADB_PORT = 8000
    client = chromadb.HttpClient(host=CHROMADB_HOST, port=CHROMADB_PORT)

    collection = client.get_collection(collection_name)

    results = collection.query(
        query_embeddings=[embedding],
        n_results=5
    )
    return results

def query(payload):
    """Makes an API request to a specified URL with the given payload."""
    response = []

    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer hf_XXXXX',
        'Content-Type': 'application/json'
    }
    if isinstance(payload['inputs'], dict) or isinstance(payload['inputs'], list):
        payload['inputs'] = json.dumps(payload['inputs'])
    encoded_payload = json.dumps(payload).encode('utf-8')
    response = http.request('POST', API_URL, headers=headers, body=encoded_payload)
    print(f"HTTP Status Code: {response.status}")
    print("Response Body:", response.data.decode('utf-8'))        
    return json.loads(response.data.decode('utf-8'))

def lambda_handler(event, context):
    print(event)
    status_code = 200
    array_of_rows_to_return = []

    try:
        event_body = event["body"]
        payload = json.loads(event_body)
        rows = payload["data"]

        for row in rows:
            row_number = row[0]
            # Assuming row[1] is a JSON string that includes collection_name
            prompt_data = json.loads(row[1])
            prompt_text = prompt_data['prompt_text']
            collection_name = prompt_data['collection_name']
            
            embedding = get_embedding(prompt_text)
            if embedding is not None:
                search_results = search_similar_embeddings(embedding, collection_name)
                chroma_response = search_results['documents']
                print(chroma_response)

                output_value = ["Processed Output:", chroma_response]
                # Optionally enhance output with additional API callpayload
                hugging_prompt = f'Respond in 100 words or less. Given {chroma_response}, respond to {prompt_text}'
                payload = {
                    "inputs": {
                        "embeddings": hugging_prompt
                    }
                }
                huggingface_result = query(payload)
                output_value = [f"{huggingface_result}"]
            else:
                output_value = ["Error", "Failed to retrieve embedding."]
            
            row_to_return = [row_number, output_value]
            array_of_rows_to_return.append(row_to_return)
            print(output_value)

        json_compatible_string_to_return = json.dumps({"data": array_of_rows_to_return})

    except Exception as err:
        status_code = 400
        json_compatible_string_to_return = json.dumps({"data": str(err)})

    return {
        'statusCode': status_code,
        'body': json_compatible_string_to_return
    }

