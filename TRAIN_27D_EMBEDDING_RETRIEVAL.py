from datetime import datetime
import csv
import io
import json
import boto3
import snowflake.connector

def lambda_handler(event, context):
    SNOWFLAKE_ACCOUNT = ''
    SNOWFLAKE_USER = 'CHENAI'
    SNOWFLAKE_PASSWORD = ''
    SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
    SNOWFLAKE_DATABASE = 'CHENAI'
    SNOWFLAKE_SCHEMA = 'LANDING'
    TABLE_NAME = 'CHENAI.HEALTH_ASSISTANT_LANDING.TRAIN_27D_EMBEDDINGS'
    BUCKET_NAME = ''
    BASE_FILE_NAME = 'TRAIN_27D_AUTO/TRAIN_27D_EMBEDDINGS'

    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )

        # Execute the query in chunks of 10,000 records
        offset = 0
        chunk_size = 10000
        while True:
            # Fetch a chunk of records
            fetch_query = f"""
                SELECT
                    MD5(PROMPT) AS PromptHash,
                    PROMPT,
                    EMBEDDING
                FROM
                    {TABLE_NAME}
                ORDER BY
                    ID
                OFFSET {offset} ROWS
                FETCH NEXT {chunk_size} ROWS ONLY
            """
            cur = conn.cursor()
            cur.execute(fetch_query)
            rows = cur.fetchall()

            if not rows:
                break  # No more records to fetch

            # Convert data to CSV format
            output = io.StringIO()
            csv_writer = csv.writer(output)
            csv_writer.writerow(['PromptHash', 'PROMPT', 'EMBEDDING'])  # Writing header
            csv_writer.writerows(rows)
            csv_data = output.getvalue()
            output.close()

            # Generate unique file name based on current timestamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
            file_name = f"{BASE_FILE_NAME}_{timestamp}.csv"

            # Write CSV data to S3
            s3 = boto3.client('s3')
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=csv_data)

            offset += chunk_size

        # Trigger another Lambda function to process the data from S3
        lambda_client = boto3.client('lambda')

        return {
            'statusCode': 200,
            'body': 'Data successfully fetched and written to CSV.'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
