import json
import math
import boto3
import os
import zipfile
import uuid
import datetime 
from psycopg2 import sql
import psycopg2
from psycopg2 import Error


def target_qualifiers(data, unique_id, conn):
    try:
        cursor = conn.cursor()
        truncate_query = sql.SQL("TRUNCATE TABLE test_db.targetqualifiers;")
        cursor.execute(truncate_query)
        uu_id = str(unique_id)
       
        for target_list in data['TargetCompounds']:

            for j in target_list['TargetQualifiers']:

                targetqualifier_data = {k: v for k, v in j['Items'].items()}

                def get_value(key):
                    try:
                        return targetqualifier_data['data'].get(key, '')
                    except:
                        return targetqualifier_data.get(key, '')

                targetqualifiers_data = {
                    'BatchID': get_value('BatchID'),
                    'SampleID': get_value('SampleID'),
                    'CompoundID': get_value('CompoundID'),
                    'QualifierID': get_value('QualifierID'),
                    'AreaSum': get_value('AreaSum'),
                    'CellAcceleratorVoltage': get_value('CellAcceleratorVoltage'),
                    'CollisionEnergy': get_value('CollisionEnergy'),
                    'CollisionEnergyDelta': get_value('CollisionEnergyDelta'),
                    'FragmentorVoltage': get_value('FragmentorVoltage'),
                    'FragmentorVoltageDelta': get_value('FragmentorVoltageDelta'),
                    'GraphicPeakQualifierChromatogram': get_value('GraphicPeakQualifierChromatogram'),
                    'IntegrationParameters': get_value('IntegrationParameters'),
                    'IntegrationParametersModified': get_value('IntegrationParametersModified'),
                    'IonPolarity': get_value('IonPolarity'),
                    'MassAccuracyLimit': get_value('MassAccuracyLimit'),
                    'MZ': get_value('MZ'),
                    'MZExtractionWindowFilterLeft': get_value('MZExtractionWindowFilterLeft'),
                    'MZExtractionWindowFilterRight': get_value('MZExtractionWindowFilterRight'),
                    'MZExtractionWindowUnits': get_value('MZExtractionWindowUnits'),
                    'OutlierPeakNotFound': get_value('OutlierPeakNotFound'),
                    'PeakFilterThreshold': get_value('PeakFilterThreshold'),
                    'PeakFilterThresholdValue': get_value('PeakFilterThresholdValue'),
                    'QualifierName': get_value('QualifierName'),
                    'QualifierRangeMaximum': get_value('QualifierRangeMaximum'),
                    'QualifierRangeMinimum': get_value('QualifierRangeMinimum'),
                    'QuantitationMessage': get_value('QuantitationMessage'),
                    'RelativeResponse': get_value('RelativeResponse'),
                    'ScanType': get_value('ScanType'),
                    'SelectedMZ': get_value('SelectedMZ'),
                    'ThresholdNumberOfPeaks': get_value('ThresholdNumberOfPeaks'),
                    'Transition': get_value('Transition'),
                    'Uncertainty': get_value('Uncertainty'),
                    'UserDefined': get_value('UserDefined'),
                    'assetId': get_value('assetId')
                }
                targetqualifiers_data['assetId'] = uu_id
                insert_query = sql.SQL("""
    INSERT INTO test_db.targetqualifiers(
	batchid, sampleid, compoundid, qualifierid, areasum, cellacceleratorvoltage, collisionenergy, collisionenergydelta, fragmentorvoltage, fragmentorvoltagedelta, graphicpeakqualifierchromatogram, integrationparameters, integrationparametersmodified, ionpolarity, massaccuracylimit, mz, mzextractionwindowfilterleft, mzextractionwindowfilterright, mzextractionwindowunits, outlierpeaknotfound, peakfilterthreshold, peakfilterthresholdvalue, qualifiername, qualifierrangemaximum, qualifierrangeminimum, quantitationmessage, relativeresponse, scantype, selectedmz, thresholdnumberofpeaks, transition, uncertainty, userdefined, assetid)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
""")
                cursor.execute(insert_query, (
                    targetqualifiers_data.get('BatchID', None),
                    targetqualifiers_data.get('SampleID', None),
                    targetqualifiers_data.get('CompoundID', None),
                    targetqualifiers_data.get('QualifierID', None),
                    targetqualifiers_data.get('AreaSum', None),
                    targetqualifiers_data.get('CellAcceleratorVoltage', None),
                    targetqualifiers_data.get('CollisionEnergy', None),
                    targetqualifiers_data.get('CollisionEnergyDelta', None),
                    targetqualifiers_data.get('FragmentorVoltage', None),
                    targetqualifiers_data.get('FragmentorVoltageDelta', None),
                    targetqualifiers_data.get(
                        'GraphicPeakQualifierChromatogram', None),
                    targetqualifiers_data.get('IntegrationParameters', None),
                    targetqualifiers_data.get(
                        'IntegrationParametersModified', None),
                    targetqualifiers_data.get('IonPolarity', None),
                    targetqualifiers_data.get('MassAccuracyLimit', None),
                    targetqualifiers_data.get('MZ', None),
                    targetqualifiers_data.get(
                        'MZExtractionWindowFilterLeft', None),
                    targetqualifiers_data.get(
                        'MZExtractionWindowFilterRight', None),
                    targetqualifiers_data.get('MZExtractionWindowUnits', None),
                    targetqualifiers_data.get('OutlierPeakNotFound', None),
                    targetqualifiers_data.get('PeakFilterThreshold', None),
                    targetqualifiers_data.get(
                        'PeakFilterThresholdValue', None),
                    targetqualifiers_data.get('QualifierName', None),
                    targetqualifiers_data.get('QualifierRangeMaximum', None),
                    targetqualifiers_data.get('QualifierRangeMinimum', None),
                    targetqualifiers_data.get('QuantitationMessage', None),
                    targetqualifiers_data.get('RelativeResponse', None),
                    targetqualifiers_data.get('ScanType', None),
                    targetqualifiers_data.get('SelectedMZ', None),
                    targetqualifiers_data.get('ThresholdNumberOfPeaks', None),
                    targetqualifiers_data.get('Transition', None),
                    targetqualifiers_data.get('Uncertainty', None),
                    targetqualifiers_data.get('UserDefined', None),
                    targetqualifiers_data.get('assetId', None))) 
        sql_query = """
    INSERT INTO samples_raw_data.targetqualifiers(
	batchid, sampleid, compoundid, qualifierid, areasum, cellacceleratorvoltage, collisionenergy, collisionenergydelta, fragmentorvoltage, fragmentorvoltagedelta, graphicpeakqualifierchromatogram, integrationparameters, integrationparametersmodified, ionpolarity, massaccuracylimit, mz, mzextractionwindowfilterleft, mzextractionwindowfilterright, mzextractionwindowunits, outlierpeaknotfound, peakfilterthreshold, peakfilterthresholdvalue, qualifiername, qualifierrangemaximum, qualifierrangeminimum, quantitationmessage, relativeresponse, scantype, selectedmz, thresholdnumberofpeaks, transition, uncertainty, userdefined, assetid)
select
	batchid, sampleid, compoundid, qualifierid, areasum, cellacceleratorvoltage, collisionenergy, collisionenergydelta, fragmentorvoltage, fragmentorvoltagedelta, graphicpeakqualifierchromatogram, integrationparameters, integrationparametersmodified, ionpolarity, massaccuracylimit, mz, mzextractionwindowfilterleft, mzextractionwindowfilterright, mzextractionwindowunits, outlierpeaknotfound, peakfilterthreshold, peakfilterthresholdvalue, qualifiername, qualifierrangemaximum, qualifierrangeminimum, quantitationmessage, relativeresponse, scantype, selectedmz, thresholdnumberofpeaks, transition, uncertainty, userdefined, assetid
from test_db.targetqualifiers;
"""
        cursor.execute(sql_query)
        # Commit the transaction and close the connection
        
        conn.commit()
        print("Data Ingested Into Production DB For Target_Qualifiers Data")

    except Exception as e:
        # Rollback in case of any error
        conn.rollback()
        print("Error executing the Target_Qualifiers query:", e)
        with open("error_log.txt", "a") as file:
            file.write(f"Error executing the targetqualifiers query: {e}\n")

    finally:
        # Close the cursor and connection
        cursor.close()
