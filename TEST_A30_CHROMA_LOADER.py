import boto3
import csv
import io
import chromadb
import json

def lambda_handler(event, context):
    print(event)
    records = event['Records']
    
    for record in records:
        bucket_name = record['s3']['bucket']['name']
        file_key = record['s3']['object']['key']
        
        # Your existing code for processing each file goes here

        CHROMADB_HOST = ''
        CHROMADB_PORT = 8000

        # chromadb.client
        client = chromadb.HttpClient(host=CHROMADB_HOST, port=CHROMADB_PORT)

        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_body = response['Body'].read().decode('utf-8')
        
        # Read CSV content
        data = csv.reader(io.StringIO(csv_body))
        next(data)  # Skip header

        results = []
        collection_name = "TEST_A30"
        
        try:
            collection = client.get_collection(collection_name)
            print(f'collection {collection_name} exists already')
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            collection = client.create_collection(collection_name)
            print(f'collection {collection_name} created')

        # Process each row in the CSV file
        for item in data:
            prompt_hash = item[0]
            prompt = item[1]
            embedding_str = item[2]
            embedding_floats = [float(num) for num in embedding_str.split(',')]

            document_id = prompt_hash
            collection.add(
                documents=[prompt],
                metadatas=[{"source": "snowflake"}],
                ids=[document_id],
                embeddings=[embedding_floats]
            )
            results.append(document_id)
            
        collection_peek = collection.peek()

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Data successfully transferred from Snowflake to ChromaDB.', 'sneak_peak_of_the_collection_so_far': collection_peek})
    }
