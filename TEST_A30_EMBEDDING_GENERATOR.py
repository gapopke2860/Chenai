import json
import urllib3
import snowflake.connector
import certifi
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# OpenAI API key
OPENAI_API_KEY = ''

# Snowflake connection details
SNOWFLAKE_ACCOUNT = ''
SNOWFLAKE_USER = 'CHENAI'
SNOWFLAKE_PASSWORD = ''
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'CHENAI'
SNOWFLAKE_SCHEMA = 'LANDING'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
TABLE_NAME = 'CHENAI.HEALTH_ASSISTANT_LANDING.TEST_A30'
NEW_TABLE_NAME = f'{TABLE_NAME}_EMBEDDINGS'

def get_embedding(text):
    """Fetches the embedding for a given piece of text using OpenAI's API."""
    http = urllib3.PoolManager(ca_certs=certifi.where())
    try:
        response = http.request(
            'POST',
            'https://api.openai.com/v1/embeddings',
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {OPENAI_API_KEY}'
            },
            body=json.dumps({'input': text, 'model': 'text-embedding-ada-002'})
        )
        if response.status == 200:
            return json.loads(response.data.decode('utf-8'))['data'][0]['embedding']
        else:
            logger.error(f"API Error: {response.status} - {response.data.decode('utf-8')}")
            return None
    except Exception as e:
        logger.error(f"Exception in get_embedding: {e}")
        return None

def ensure_table_exists(cursor):
    """Ensure the PROMPT_EMBEDDINGS table exists or create it."""
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {NEW_TABLE_NAME} LIKE {TABLE_NAME}
    """)

def update_embeddings_in_batches(batch_size=100):
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE
    )
    cur = conn.cursor()
    
    # Ensure the new table exists
    ensure_table_exists(cur)
    
    logger.info('SUCCESSFUL SNOWFLAKE CONNECTION')

    # Count total records in the original table
    count_original_query = f"SELECT COUNT(*) FROM {TABLE_NAME}"
    cur.execute(count_original_query)
    total_original_records = cur.fetchone()[0]

    # Count total records in the new table
    count_new_query = f"SELECT COUNT(*) FROM {NEW_TABLE_NAME}"
    cur.execute(count_new_query)
    total_new_records = cur.fetchone()[0]

    # Calculate the number of records to process
    records_to_process = total_original_records - total_new_records

    total_batches = (records_to_process + batch_size - 1) // batch_size

    logger.info(f"Total original records: {total_original_records}, Total new records: {total_new_records}")
    logger.info(f"Records to process: {records_to_process}, Total batches: {total_batches}")

    total_rows_processed = 0  # Keep track of the total rows processed

    offset = total_new_records

    for _ in range(total_batches):
        fetch_query = f"""
        SELECT ID, PROMPT
        FROM {TABLE_NAME}
        ORDER BY ID
        LIMIT {batch_size} 
        OFFSET {offset}
        """
        cur.execute(fetch_query)
        rows = cur.fetchall()

        if not rows:
            logger.info("No more rows to process.")
            break

        batch_data = []
        for row in rows:
            embedding = get_embedding(row[1])
            if embedding:
                embedding_str = ','.join(map(str, embedding))
                batch_data.append((row[0], row[1], embedding_str))
            else:
                logger.info(f'Embedding for row {total_rows_processed+1} failed to return an embedding.')

        if batch_data:
            insert_query = f"""
                INSERT INTO {NEW_TABLE_NAME} (ID, PROMPT, EMBEDDING)
                VALUES (%s, %s, %s)
            """
            cur.executemany(insert_query, batch_data)
            conn.commit()
            total_rows_processed += len(batch_data)

        logger.info(f"Processed batch: {total_rows_processed // batch_size + 1} of {total_batches}, Rows processed: {total_rows_processed}")

        offset += batch_size

    cur.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully inserted {total_rows_processed} embeddings into {NEW_TABLE_NAME}.')
    }


def lambda_handler(event, context):
    return update_embeddings_in_batches()
