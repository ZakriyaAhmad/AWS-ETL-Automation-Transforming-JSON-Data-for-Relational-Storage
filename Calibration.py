import json
import math
import boto3
import os
import zipfile
import uuid
import datetime
from psycopg2 import sql


def calibration_qualifiers(data, unique_id, conn):
    try:
        cursor = conn.cursor()
        truncate_query = sql.SQL("TRUNCATE TABLE test_db.calibration;")
        cursor.execute(truncate_query)
        uu_id = str(unique_id)
        for target_list in data['TargetCompounds']:
            Calibration_data = {k: v for k,
                                v in target_list['Calibration'].items()} 
            Calibration_data = Calibration_data['Items']
            Calibration_data['assetId'] = uu_id
            insert_query = sql.SQL("""
    INSERT INTO test_db.calibration(
	assetid, batchid, sampleid, compoundid, levelid, calibrationstdacquisitiondatetime, calibrationstdpathname, calibrationtype, levelaveragecounter, levelconcentration, levelenable, levellastupdatetime, levelname, levelresponse, levelresponsefactor, levelrsd)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""")

        # Execute the query with data
            cursor.execute(insert_query, (
                Calibration_data.get('assetid', None),
                Calibration_data.get('BatchID', None),
                Calibration_data.get('SampleID', None),
                Calibration_data.get('CompoundID', None),
                Calibration_data.get('LevelID', None),
                Calibration_data.get('CalibrationSTDAcquisitionDateTime', None),
                Calibration_data.get('CalibrationSTDPathName', None),
                Calibration_data.get('CalibrationType', None),
                Calibration_data.get('LevelAverageCounter', None),
                Calibration_data.get('LevelConcentration', None),
                Calibration_data.get('LevelEnable', None),
                Calibration_data.get('LevelLastUpdateTime', None),
                Calibration_data.get('LevelName', None),
                Calibration_data.get('LevelResponse', None),
                Calibration_data.get('LevelResponseFactor', None),
                Calibration_data.get('LevelRSD', None)
            )) 
        sql_query = """
    INSERT INTO samples_raw_data.calibration(
	assetid, batchid, sampleid, compoundid, levelid, calibrationstdacquisitiondatetime, calibrationstdpathname, calibrationtype, levelaveragecounter, levelconcentration, levelenable, levellastupdatetime, levelname, levelresponse, levelresponsefactor, levelrsd)
select 	assetid, batchid, sampleid, compoundid, levelid, calibrationstdacquisitiondatetime, calibrationstdpathname, calibrationtype, levelaveragecounter, levelconcentration, levelenable, levellastupdatetime, levelname, levelresponse, levelresponsefactor, levelrsd
from test_db.calibration;
"""
        cursor.execute(sql_query)
        # Commit the transaction and close the connection
        conn.commit()
        print("Data Ingested Into Production DB For Calibration Data")

    except Exception as e:
        # Rollback in case of any error
        conn.rollback()
        print("Error executing the Calibration query:", e)
        with open("error_log.txt", "a") as file:
            file.write(f"Error executing the Calibration query: {e}\n")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()