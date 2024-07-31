import json
import math
import boto3
import os
import zipfile, uuid, datetime 
from psycopg2 import sql

#storing batch data in the tables
def batch_data(data,unique_id,conn):
   
    try:
      
        Batch_data = {k: v for k, v in data['Items'].items() }
        def get_value(key):
            try:
                return Batch_data['data'].get(key, '')
            except:
                return Batch_data.get(key, '')
        cursor = conn.cursor()
        truncate_query = sql.SQL("TRUNCATE TABLE test_db.batchdata;")
        cursor.execute(truncate_query)
        uu_id = str(unique_id)
        meta_data_dict = {
    'BatchID': get_value('BatchID'),
    'SampleID': get_value('SampleID')
}

# Construct the SQL query for updating metadata
        update_query = sql.SQL("""
    UPDATE test_db.metadata 
    SET "BatchID" = %s, "SampleID" = %s 
    WHERE assetId = %s;
""")

# Execute the update query
        cursor.execute(update_query, (meta_data_dict['BatchID'], meta_data_dict['SampleID'], uu_id))

        batch_data_dict = {
    'BatchID': get_value('BatchID'),
    'SampleID': get_value('SampleID'),
    'AcqDateTime': get_value('AcqDateTime'),
    'AcqDateTimeLocal': get_value('AcqDateTimeLocal'),
    'AcqMethodFileName': get_value('AcqMethodFileName'),
    'AcqMethodPathName': get_value('AcqMethodPathName'),
    'AcqOperator': get_value('AcqOperator'),
    'BalanceOverride': get_value('BalanceOverride'),
    'Barcode': get_value('Barcode'),
    'CalibrationReferenceSampleID': get_value('CalibrationReferenceSampleID'),
    'Comment': get_value('Comment'),
    'Completed': get_value('Completed'),
    'DADateTime': get_value('DADateTime'),
    'DAMethodFileName': get_value('DAMethodFileName'),
    'DAMethodPathName': get_value('DAMethodPathName'),
    'DataFileName': get_value('DataFileName'),
    'DataPathName': get_value('DataPathName'),
    'Dilution': get_value('Dilution'),
    'DualInjector': get_value('DualInjector'),
    'DualInjectorAcqDateTime': get_value('DualInjectorAcqDateTime'),
    'DualInjectorBarcode': get_value('DualInjectorBarcode'),
    'DualInjectorExpectedBarcode': get_value('DualInjectorExpectedBarcode'),
    'DualInjectorVial': get_value('DualInjectorVial'),
    'DualInjectorVolume': get_value('DualInjectorVolume'),
    'EquilibrationTime': get_value('EquilibrationTime'),
    'ExpectedBarcode': get_value('ExpectedBarcode'),
    'GraphicSampleChromatogram': get_value('GraphicSampleChromatogram'),
    'InjectionsPerPosition': get_value('InjectionsPerPosition'),
    'InjectorVolume': get_value('InjectorVolume'),
    'InstrumentName': get_value('InstrumentName'),
    'InstrumentType': get_value('InstrumentType'),
    'ISTDDilution': get_value('ISTDDilution'),
    'LevelName': get_value('LevelName'),
    'Locked': get_value('Locked'),
    'MatrixSpikeDilution': get_value('MatrixSpikeDilution'),
    'MatrixSpikeGroup': get_value('MatrixSpikeGroup'),
    'MatrixType': get_value('MatrixType'),
    'OutlierCCTime': get_value('OutlierCCTime'),
    'PlateCode': get_value('PlateCode'),
    'PlatePosition': get_value('PlatePosition'),
    'QuantitationMessage': get_value('QuantitationMessage'),
    'RackCode': get_value('RackCode'),
    'RackPosition': get_value('RackPosition'),
    'RunStartValvePositionDescription': get_value('RunStartValvePositionDescription'),
    'RunStartValvePositionNumber': get_value('RunStartValvePositionNumber'),
    'RunStopValvePositionDescription': get_value('RunStopValvePositionDescription'),
    'RunStopValvePositionNumber': get_value('RunStopValvePositionNumber'),
    'SampleAmount': get_value('SampleAmount'),
    'SampleApproved': get_value('SampleApproved'),
    'SampleGroup': get_value('SampleGroup'),
    'SampleInformation': get_value('SampleInformation'),
    'SampleName': get_value('SampleName'),
    'SamplePosition': get_value('SamplePosition'),
    'SamplePrepFileName': get_value('SamplePrepFileName'),
    'SamplePrepPathName': get_value('SamplePrepPathName'),
    'SampleType': get_value('SampleType'),
    'SamplingDateTime': get_value('SamplingDateTime'),
    'SamplingTime': get_value('SamplingTime'),
    'SequenceFileName': get_value('SequenceFileName'),
    'SequencePathName': get_value('SequencePathName'),
    'SurrogateDilution': get_value('SurrogateDilution'),
    'TotalSampleAmount': get_value('TotalSampleAmount'),
    'TuneFileLastTimeStamp': get_value('TuneFileLastTimeStamp'),
    'TuneFileName': get_value('TuneFileName'),
    'TunePathName': get_value('TunePathName'),
    'TrayName': get_value('TrayName'),
    'UserDefined': get_value('UserDefined'),
    'UserDefined1': get_value('UserDefined1'),
    'UserDefined2': get_value('UserDefined2'),
    'UserDefined3': get_value('UserDefined3'),
    'UserDefined4': get_value('UserDefined4'),
    'UserDefined5': get_value('UserDefined5'),
    'UserDefined6': get_value('UserDefined6'),
    'UserDefined7': get_value('UserDefined7'),
    'UserDefined8': get_value('UserDefined8'),
    'UserDefined9': get_value('UserDefined9'),
    'Vial': get_value('Vial')
}

        batch_data_dict['assetId'] = uu_id
        #Construct the SQL query for insertion
        insert_query = sql.SQL("""
    INSERT INTO test_db.batchdata(
	assetid, "BatchID", "SampleID", "AcqDateTime", "AcqDateTimeLocal", "AcqMethodFileName", "AcqMethodPathName", "AcqOperator", "BalanceOverride", "Barcode", "CalibrationReferenceSampleID", "Comment", "Completed", "DADateTime", "DAMethodFileName", "DAMethodPathName", "DataFileName", "DataPathName", "Dilution", "DualInjector", "DualInjectorAcqDateTime", "DualInjectorBarcode", "DualInjectorExpectedBarcode", "DualInjectorVial", "DualInjectorVolume", "EquilibrationTime", "ExpectedBarcode", "GraphicSampleChromatogram", "InjectionsPerPosition", "InjectorVolume", "InstrumentName", "InstrumentType", "ISTDDilution", "LevelName", "Locked", "MatrixSpikeDilution", "MatrixSpikeGroup", "MatrixType", "OutlierCCTime", "PlateCode", "PlatePosition", "QuantitationMessage", "RackCode", "RackPosition", "RunStartValvePositionDescription", "RunStartValvePositionNumber", "RunStopValvePositionDescription", "RunStopValvePositionNumber", "SampleAmount", "SampleApproved", "SampleGroup", "SampleInformation", "SampleName", "SamplePosition", "SamplePrepFileName", "SamplePrepPathName", "SampleType", "SamplingDateTime", "SamplingTime", "SequenceFileName", "SequencePathName", "SurrogateDilution", "TotalSampleAmount", "TuneFileLastTimeStamp", "TuneFileName", "TunePathName", "TrayName", "UserDefined", "UserDefined1", "UserDefined2", "UserDefined3", "UserDefined4", "UserDefined5", "UserDefined6", "UserDefined7", "UserDefined8", "UserDefined9", "Vial")
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""")
    
        cursor.execute(insert_query, (
    batch_data_dict.get('assetId', None),
    batch_data_dict.get('BatchID', None),
    batch_data_dict.get('SampleID', None),
    batch_data_dict.get('AcqDateTime', None),
    batch_data_dict.get('AcqDateTimeLocal', None),
    batch_data_dict.get('AcqMethodFileName', None),
    batch_data_dict.get('AcqMethodPathName', None),
    batch_data_dict.get('AcqOperator', None),
    batch_data_dict.get('BalanceOverride', None),
    batch_data_dict.get('Barcode', None),
    batch_data_dict.get('CalibrationReferenceSampleID', None),
    batch_data_dict.get('Comment', None),
    batch_data_dict.get('Completed', None),
    batch_data_dict.get('DADateTime', None),
    batch_data_dict.get('DAMethodFileName', None),
    batch_data_dict.get('DAMethodPathName', None),
    batch_data_dict.get('DataFileName', None),
    batch_data_dict.get('DataPathName', None),
    batch_data_dict.get('Dilution', None),
    batch_data_dict.get('DualInjector', None),
    batch_data_dict.get('DualInjectorAcqDateTime', None),
    batch_data_dict.get('DualInjectorBarcode', None),
    batch_data_dict.get('DualInjectorExpectedBarcode', None),
    batch_data_dict.get('DualInjectorVial', None),
    batch_data_dict.get('DualInjectorVolume', None),
    batch_data_dict.get('EquilibrationTime', None),
    batch_data_dict.get('ExpectedBarcode', None),
    batch_data_dict.get('GraphicSampleChromatogram', None),
    batch_data_dict.get('InjectionsPerPosition', None),
    batch_data_dict.get('InjectorVolume', None),
    batch_data_dict.get('InstrumentName', None),
    batch_data_dict.get('InstrumentType', None),
    batch_data_dict.get('ISTDDilution', None),
    batch_data_dict.get('LevelName', None),
    batch_data_dict.get('Locked', None),
    batch_data_dict.get('MatrixSpikeDilution', None),
    batch_data_dict.get('MatrixSpikeGroup', None),
    batch_data_dict.get('MatrixType', None),
    batch_data_dict.get('OutlierCCTime', None),
    batch_data_dict.get('PlateCode', None),
    batch_data_dict.get('PlatePosition', None),
    batch_data_dict.get('QuantitationMessage', None),
    batch_data_dict.get('RackCode', None),
    batch_data_dict.get('RackPosition', None),
    batch_data_dict.get('RunStartValvePositionDescription', None),
    batch_data_dict.get('RunStartValvePositionNumber', None),
    batch_data_dict.get('RunStopValvePositionDescription', None),
    batch_data_dict.get('RunStopValvePositionNumber', None),
    batch_data_dict.get('SampleAmount', None),
    batch_data_dict.get('SampleApproved', None),
    batch_data_dict.get('SampleGroup', None),
    batch_data_dict.get('SampleInformation', None),
    batch_data_dict.get('SampleName', None),
    batch_data_dict.get('SamplePosition', None),
    batch_data_dict.get('SamplePrepFileName', None),
    batch_data_dict.get('SamplePrepPathName', None),
    batch_data_dict.get('SampleType', None),
    batch_data_dict.get('SamplingDateTime', None),
    batch_data_dict.get('SamplingTime', None),
    batch_data_dict.get('SequenceFileName', None),
    batch_data_dict.get('SequencePathName', None),
    batch_data_dict.get('SurrogateDilution', None),
    batch_data_dict.get('TotalSampleAmount', None),
    batch_data_dict.get('TuneFileLastTimeStamp', None),
    batch_data_dict.get('TuneFileName', None),
    batch_data_dict.get('TunePathName', None),
    batch_data_dict.get('TrayName', None),
    batch_data_dict.get('UserDefined', None),
    batch_data_dict.get('UserDefined1', None),
    batch_data_dict.get('UserDefined2', None),
    batch_data_dict.get('UserDefined3', None),
    batch_data_dict.get('UserDefined4', None),
    batch_data_dict.get('UserDefined5', None),
    batch_data_dict.get('UserDefined6', None),
    batch_data_dict.get('UserDefined7', None),
    batch_data_dict.get('UserDefined8', None),
    batch_data_dict.get('UserDefined9', None),
    batch_data_dict.get('Vial', None)
))

       

        sql_query = """
    INSERT INTO samples_raw_data.batchdata(
	assetid, "BatchID", "SampleID", "AcqDateTime", "AcqDateTimeLocal", "AcqMethodFileName", "AcqMethodPathName", "AcqOperator", "BalanceOverride", "Barcode", "CalibrationReferenceSampleID", "Comment", "Completed", "DADateTime", "DAMethodFileName", "DAMethodPathName", "DataFileName", "DataPathName", "Dilution", "DualInjector", "DualInjectorAcqDateTime", "DualInjectorBarcode", "DualInjectorExpectedBarcode", "DualInjectorVial", "DualInjectorVolume", "EquilibrationTime", "ExpectedBarcode", "GraphicSampleChromatogram", "InjectionsPerPosition", "InjectorVolume", "InstrumentName", "InstrumentType", "ISTDDilution", "LevelName", "Locked", "MatrixSpikeDilution", "MatrixSpikeGroup", "MatrixType", "OutlierCCTime", "PlateCode", "PlatePosition", "QuantitationMessage", "RackCode", "RackPosition", "RunStartValvePositionDescription", "RunStartValvePositionNumber", "RunStopValvePositionDescription", "RunStopValvePositionNumber", "SampleAmount", "SampleApproved", "SampleGroup", "SampleInformation", "SampleName", "SamplePosition", "SamplePrepFileName", "SamplePrepPathName", "SampleType", "SamplingDateTime", "SamplingTime", "SequenceFileName", "SequencePathName", "SurrogateDilution", "TotalSampleAmount", "TuneFileLastTimeStamp", "TuneFileName", "TunePathName", "TrayName", "UserDefined", "UserDefined1", "UserDefined2", "UserDefined3", "UserDefined4", "UserDefined5", "UserDefined6", "UserDefined7", "UserDefined8", "UserDefined9", "Vial")
select 
	assetid, "BatchID", "SampleID", "AcqDateTime", "AcqDateTimeLocal", "AcqMethodFileName", "AcqMethodPathName", "AcqOperator", "BalanceOverride", "Barcode", "CalibrationReferenceSampleID", "Comment", "Completed", "DADateTime", "DAMethodFileName", "DAMethodPathName", "DataFileName", "DataPathName", "Dilution", "DualInjector", "DualInjectorAcqDateTime", "DualInjectorBarcode", "DualInjectorExpectedBarcode", "DualInjectorVial", "DualInjectorVolume", "EquilibrationTime", "ExpectedBarcode", "GraphicSampleChromatogram", "InjectionsPerPosition", "InjectorVolume", "InstrumentName", "InstrumentType", "ISTDDilution", "LevelName", "Locked", "MatrixSpikeDilution", "MatrixSpikeGroup", "MatrixType", "OutlierCCTime", "PlateCode", "PlatePosition", "QuantitationMessage", "RackCode", "RackPosition", "RunStartValvePositionDescription", "RunStartValvePositionNumber", "RunStopValvePositionDescription", "RunStopValvePositionNumber", "SampleAmount", "SampleApproved", "SampleGroup", "SampleInformation", "SampleName", "SamplePosition", "SamplePrepFileName", "SamplePrepPathName", "SampleType", "SamplingDateTime", "SamplingTime", "SequenceFileName", "SequencePathName", "SurrogateDilution", "TotalSampleAmount", "TuneFileLastTimeStamp", "TuneFileName", "TunePathName", "TrayName", "UserDefined", "UserDefined1", "UserDefined2", "UserDefined3", "UserDefined4", "UserDefined5", "UserDefined6", "UserDefined7", "UserDefined8", "UserDefined9", "Vial"
from test_db.batchdata;
"""
        cursor.execute(sql_query)

        sql_query = """
    INSERT INTO samples_raw_data.metadata(
	assetid, "BatchID", "SampleID", path, "timestamp", samplename, batchname, type, instrument, level, stage, methodid, "group")
	select
	assetid, "BatchID", "SampleID", path, "timestamp", samplename, batchname, type, instrument, level, stage, methodid, "group"
	from test_db.metadata;
"""
        cursor.execute(sql_query)
        # Commit the transaction and close the connection
        conn.commit()
        print("Data Ingested Into Production DB For batch,Meta Data")

    except Exception as e:
        # Rollback in case of any error
        conn.rollback()
        print("Error executing the BATCH query:", e)
        with open("error_log.txt", "a") as file:
            file.write(f"Error executing the BATCH query: {e}\n")

    finally:
        # Close the cursor and connection
        cursor.close()



