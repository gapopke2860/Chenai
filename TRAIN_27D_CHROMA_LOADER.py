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
        
        # Setup ChromaDB client
        CHROMADB_HOST = ''
        CHROMADB_PORT = 8000
        client = chromadb.HttpClient(host=CHROMADB_HOST, port=CHROMADB_PORT)

        # Setup S3 client and get the CSV file
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_body = response['Body'].read().decode('utf-8')
        
        data = csv.reader(io.StringIO(csv_body))
        next(data)  # Skip header

        results = []
        collection_name = "TRAIN_27"
        row_count = 0  # Initialize a row count
        
        try:
            collection = client.get_collection(collection_name)
            print(f'Collection {collection_name} exists already.')
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            collection = client.create_collection(collection_name)
            print(f'Collection {collection_name} created.')

        # Process each row in the CSV file
        for item in data:
            row_count += 1  # Increment row count
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
            
            # Every 100 rows, do something (e.g., log progress)
            if row_count % 100 == 0:
                print(f'Processed {row_count} rows so far.')

        collection_peek = collection.peek()
        print(collection_peek)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Data successfully transferred from Snowflake to ChromaDB.',
            'sneak_peek_of_the_collection_so_far': collection_peek
        })
    }
