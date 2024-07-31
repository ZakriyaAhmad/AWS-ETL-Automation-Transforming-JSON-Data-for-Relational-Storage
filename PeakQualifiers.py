import json
import math
import boto3
import os
import zipfile
import uuid
import datetime
from psycopg2 import sql


def peaksqualifiers_ingestion(data, unique_id, conn):
    try:
        cursor = conn.cursor()
        truncate_query = sql.SQL("TRUNCATE TABLE test_db.peakqualifiers;")
        cursor.execute(truncate_query)
        uu_id = str(unique_id)
        for target_list in data['TargetCompounds']:
            for peaks_data in target_list['Peaks']:
                Peaks_items_data = {k: v for k,
                                    v in peaks_data['Items'].items()}
                for PeakQualifiers_date in peaks_data['PeakQualifiers']:
                    PeakQualifiers_items_date = {
                        k: v for k, v in PeakQualifiers_date['Items'].items()}
                    PeakQualifiers_items_date['assetId'] = uu_id
                    insert_query = sql.SQL("""
    INSERT INTO test_db.peakqualifiers(
	assetid, "BatchID", "SampleID", "CompoundID", "PeakID", "QualifierID", "Area", "BaselineEnd", "BaselineEndOriginal", "BaselineStandardDeviation", "BaselineStart", "BaselineStartOriginal", "CoelutionScore", "FullWidthHalfMaximum", "Height", "IntegrationEndTime", "IntegrationEndTimeOriginal", "IntegrationMetricQualityFlags", "IntegrationQualityMetric", "IntegrationStartTime", "IntegrationStartTimeOriginal", "ManuallyIntegrated", "MassAccuracy", "MZ", "Noise", "NoiseRegions", "OutlierQualifierCoelutionScore", "OutlierQualifierFullWidthHalfMaximum", "OutlierQualifierIntegrationQualityMetric", "OutlierQualifierMassAccuracy", "OutlierQualifierOutOfLimits", "OutlierQualifierResolutionFront", "OutlierQualifierResolutionRear", "OutlierQualifierSignalToNoiseRatio", "OutlierQualifierSymmetry", "OutlierSaturationRecovery", "QualifierResponseRatio", "QualifierResponseRatioOriginal", "ResolutionFront", "ResolutionRear", "RetentionTime", "RetentionTimeOriginal", "SaturationRecoveryRatio", "SignalToNoiseRatio", "Symmetry", "UserCustomCalculation")
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
""")
                    cursor.execute(insert_query, (PeakQualifiers_items_date.get('assetId', None),
                                                  PeakQualifiers_items_date.get(
                                                      "BatchID", None),
                                                  PeakQualifiers_items_date.get(
                                                      "SampleID", None),
                                                  PeakQualifiers_items_date.get(
                                                      "CompoundID", None),
                                                  PeakQualifiers_items_date.get(
                                                      "PeakID", None),
                                                  PeakQualifiers_items_date.get(
                                                      "QualifierID", None),
                                                  PeakQualifiers_items_date.get(
                                                      "Area", None),
                                                  PeakQualifiers_items_date.get(
                                                      "BaselineEnd", None),
                                                  PeakQualifiers_items_date.get(
                                                      "BaselineEndOriginal", None),
                                                  PeakQualifiers_items_date.get(
                                                      "BaselineStandardDeviation", None),
                                                  PeakQualifiers_items_date.get(
                                                      "BaselineStart", None),
                                                  PeakQualifiers_items_date.get(
                                                      "BaselineStartOriginal", None),
                                                  PeakQualifiers_items_date.get(
                                                      "CoelutionScore", None),
                                                  PeakQualifiers_items_date.get(
                                                      "FullWidthHalfMaximum", None),
                                                  PeakQualifiers_items_date.get(
                                                      "Height", None),
                                                  PeakQualifiers_items_date.get(
                                                      "IntegrationEndTime", None),
                                                  PeakQualifiers_items_date.get(
                                                      "IntegrationEndTimeOriginal", None),
                                                  PeakQualifiers_items_date.get(
                                                      "IntegrationMetricQualityFlags", None),
                                                  PeakQualifiers_items_date.get(
                                                      "IntegrationQualityMetric", None),
                                                  PeakQualifiers_items_date.get(
                                                      "IntegrationStartTime", None),
                                                  PeakQualifiers_items_date.get(
                                                      "IntegrationStartTimeOriginal", None),
                                                  PeakQualifiers_items_date.get(
                                                      "ManuallyIntegrated", None),
                                                  PeakQualifiers_items_date.get(
                                                      "MassAccuracy", None),
                                                  PeakQualifiers_items_date.get(
                                                      "MZ", None),
                                                  PeakQualifiers_items_date.get(
                                                      "Noise", None),
                                                  PeakQualifiers_items_date.get(
                                                      "NoiseRegions", None),
                                                  PeakQualifiers_items_date.get(
                                                      "OutlierQualifierCoelutionScore", None),
                                                  PeakQualifiers_items_date.get(
                                                      "OutlierQualifierFullWidthHalfMaximum", None),
                                                  PeakQualifiers_items_date.get(
                                                      "OutlierQualifierIntegrationQualityMetric", None),
                                                  PeakQualifiers_items_date.get(
                                                      "OutlierQualifierMassAccuracy", None),
                                                  PeakQualifiers_items_date.get(
                                                      "OutlierQualifierOutOfLimits", None),
                                                  PeakQualifiers_items_date.get(
                                                      "OutlierQualifierResolutionFront", None),
                                                  PeakQualifiers_items_date.get(
                                                      "OutlierQualifierResolutionRear", None),
                                                  PeakQualifiers_items_date.get(
                                                      "OutlierQualifierSignalToNoiseRatio", None),
                                                  PeakQualifiers_items_date.get(
                                                      "OutlierQualifierSymmetry", None),
                                                  PeakQualifiers_items_date.get(
                                                      "OutlierSaturationRecovery", None),
                                                  PeakQualifiers_items_date.get(
                                                      "QualifierResponseRatio", None),
                                                  PeakQualifiers_items_date.get(
                                                      "QualifierResponseRatioOriginal", None),
                                                  PeakQualifiers_items_date.get(
                                                      "ResolutionFront", None),
                                                  PeakQualifiers_items_date.get(
                                                      "ResolutionRear", None),
                                                  PeakQualifiers_items_date.get(
                                                      "RetentionTime", None),
                                                  PeakQualifiers_items_date.get(
                                                      "RetentionTimeOriginal", None),
                                                  PeakQualifiers_items_date.get(
                                                      "SaturationRecoveryRatio", None),
                                                  PeakQualifiers_items_date.get(
                                                      "SignalToNoiseRatio", None),
                                                  PeakQualifiers_items_date.get(
                                                      "Symmetry", None),
                                                  PeakQualifiers_items_date.get(
                                                      "UserCustomCalculation", None)
                                                  ))
        sql_query = """INSERT INTO samples_raw_data.peakqualifiers(
	assetid, "BatchID", "SampleID", "CompoundID", "PeakID", "QualifierID", "Area", "BaselineEnd", "BaselineEndOriginal", "BaselineStandardDeviation", "BaselineStart", "BaselineStartOriginal", "CoelutionScore", "FullWidthHalfMaximum", "Height", "IntegrationEndTime", "IntegrationEndTimeOriginal", "IntegrationMetricQualityFlags", "IntegrationQualityMetric", "IntegrationStartTime", "IntegrationStartTimeOriginal", "ManuallyIntegrated", "MassAccuracy", "MZ", "Noise", "NoiseRegions", "OutlierQualifierCoelutionScore", "OutlierQualifierFullWidthHalfMaximum", "OutlierQualifierIntegrationQualityMetric", "OutlierQualifierMassAccuracy", "OutlierQualifierOutOfLimits", "OutlierQualifierResolutionFront", "OutlierQualifierResolutionRear", "OutlierQualifierSignalToNoiseRatio", "OutlierQualifierSymmetry", "OutlierSaturationRecovery", "QualifierResponseRatio", "QualifierResponseRatioOriginal", "ResolutionFront", "ResolutionRear", "RetentionTime", "RetentionTimeOriginal", "SaturationRecoveryRatio", "SignalToNoiseRatio", "Symmetry", "UserCustomCalculation")
select
	assetid, "BatchID", "SampleID", "CompoundID", "PeakID", "QualifierID", "Area", "BaselineEnd", "BaselineEndOriginal", "BaselineStandardDeviation", "BaselineStart", "BaselineStartOriginal", "CoelutionScore", "FullWidthHalfMaximum", "Height", "IntegrationEndTime", "IntegrationEndTimeOriginal", "IntegrationMetricQualityFlags", "IntegrationQualityMetric", "IntegrationStartTime", "IntegrationStartTimeOriginal", "ManuallyIntegrated", "MassAccuracy", "MZ", "Noise", "NoiseRegions", "OutlierQualifierCoelutionScore", "OutlierQualifierFullWidthHalfMaximum", "OutlierQualifierIntegrationQualityMetric", "OutlierQualifierMassAccuracy", "OutlierQualifierOutOfLimits", "OutlierQualifierResolutionFront", "OutlierQualifierResolutionRear", "OutlierQualifierSignalToNoiseRatio", "OutlierQualifierSymmetry", "OutlierSaturationRecovery", "QualifierResponseRatio", "QualifierResponseRatioOriginal", "ResolutionFront", "ResolutionRear", "RetentionTime", "RetentionTimeOriginal", "SaturationRecoveryRatio", "SignalToNoiseRatio", "Symmetry", "UserCustomCalculation"
from test_db.peakqualifiers;
"""
        cursor.execute(sql_query)
        # Commit the transaction and close the connection
        conn.commit()
        print("Data Ingested Into Production DB For peakqualifiers Data")

    except Exception as e:
        # Rollback in case of any error
        conn.rollback()
        print("Error executing the peakqualifiers query:", e)
        with open("error_log.txt", "a") as file:
            file.write(f"Error executing the peakqualifiers query: {e}\n")

    finally:
        # Close the cursor and connection
        cursor.close()
