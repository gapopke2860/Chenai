import snowflake.connector
import boto3
import csv
import io
import json

def lambda_handler(event, context):
    SNOWFLAKE_ACCOUNT = ''
    SNOWFLAKE_USER = 'CHENAI'
    SNOWFLAKE_PASSWORD = ''
    SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
    SNOWFLAKE_DATABASE = 'CHENAI'
    SNOWFLAKE_SCHEMA = 'LANDING'
    TABLE_NAME = 'CHENAI.HEALTH_ASSISTANT_LANDING.TEST_A30_EMBEDDINGS'
    BUCKET_NAME = ''
    FILE_NAME = 'TEST_A30_AUTO_START/TEST_A30_EMBEDDINGS.csv'

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
        
        # Execute the query
        fetch_query = f"""
        SELECT
            MD5(PROMPT) AS PromptHash,
            PROMPT,
            EMBEDDING
        FROM
            {TABLE_NAME}
        """
        cur = conn.cursor()
        cur.execute(fetch_query)
        rows = cur.fetchall()

        # Convert data to CSV format
        output = io.StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow(['PromptHash', 'PROMPT', 'EMBEDDING'])  # Writing header
        csv_writer.writerows(rows)
        csv_data = output.getvalue()
        output.close()

        # Write CSV data to S3
        s3 = boto3.client('s3')
        s3.put_object(Bucket=BUCKET_NAME, Key=FILE_NAME, Body=csv_data)

        return {
            'statusCode': 200,
            'body': 'Data successfully fetched and written to CSV.'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
