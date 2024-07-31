import json
import os
import zipfile
from datetime import datetime
import psycopg2
from psycopg2 import sql

#storing meta data like batch ID, sample ID, sample name etc
def meta_data_func(data, path, file_key, conn, uuid_val):
    try:

        unique_id = uuid_val

        def get_value(key):
            try:
                return data['data'].get(key, '')
            except:
                return data.get(key, '')

        data = {
            'path': file_key,
            'timestamp': get_value('timestamp'),
            "sampleName": get_value('sampleName'),
            "batchName": get_value('batchName'),
            "type": get_value('type'),
            "instrument": get_value('instrument'),
            "level": get_value('level'),
            "stage": get_value('stage'),
            "methodId": get_value('methodId'),
            "group": get_value('group')

        }
        data['assetId'] = str(unique_id)

        
        cursor = conn.cursor()
        truncate_query = sql.SQL("TRUNCATE TABLE test_db.metadata;")
        cursor.execute(truncate_query)
        # Construct the SQL query for insertion
        insert_query = sql.SQL("""
    INSERT INTO test_db.metadata (assetId, path, "timestamp", sampleName, batchName, type, instrument, level, stage, methodId,  "group") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""")

        # Execute the query with data
        cursor.execute(insert_query, (
            data.get('assetId', None),
            data.get('path', None),
            data.get('timestamp', None),
            data.get('sampleName', None),
            data.get('batchName', None),
            data.get('type', None),
            data.get('instrument', None),
            data.get('level', None),
            data.get('stage', None),
            data.get('methodId', None),
            data.get('group', None)
        ))

        print('Insertion Completed For Meta Data')
        # Commit the transaction and close the connection
        conn.commit()

    except Exception as e:
        # Rollback in case of any error
        conn.rollback()
        print("Error executing the META DATA query:", e)

    finally:
        # Close the cursor and connection
        cursor.close()
