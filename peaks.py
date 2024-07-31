import json
import math
import boto3
import os
import zipfile
import uuid
import datetime
from psycopg2 import sql


def peaks_ingestion(data, unique_id, conn):
    try:
        cursor = conn.cursor()
        truncate_query = sql.SQL("TRUNCATE TABLE test_db.peaks;")
        cursor.execute(truncate_query)
        uu_id = str(unique_id)
        
        for target_list in data['TargetCompounds']:
            for peaks_data in target_list['Peaks']:
                Peaks_items_data = {k: v for k,
                                    v in peaks_data['Items'].items()}
                Peaks_items_data['assetId'] = uu_id
                insert_query = sql.SQL("""
    INSERT INTO test_db.peaks(
	assetid, batchid, sampleid, compoundid, peakid, accuracy, alternativepeakrtdiff, alternativetargethit, area, areacorrectionresponse, baselinedraw, baselineend, baselineendoriginal, baselinestandarddeviation, baselinestart, baselinestartoriginal, calculatedconcentration, capacityfactor, ccistdresponseratio, ccresponseratio, estimatedconcentration, finalconcentration, fullwidthhalfmaximum, groupnumber, height, integrationendtime, integrationendtimeoriginal, integrationmetricqualityflags, integrationqualitymetric, integrationstarttime, integrationstarttimeoriginal, istdconcentrationratio, istdresponsepercentdeviation, istdresponseratio, manuallyintegrated, massabundancescore, massaccuracy, massaccuracyscore, massmatchscore, massspacingscore, matrixspikepercentdeviation, matrixspikepercentrecovery, mz, noise, noiseregions, numberofverifiedions, outlieraccuracy, outlierbelowlimitofdetection, outlierbelowlimitofquantitation, outlierblankconcentrationoutsidelimit, outliercapacityfactor, outlierccistdresponseratio, outlierccresponseratio, outlierccretentiontime, outlierfullwidthhalfmaximum, outlierintegrationqualitymetric, outlieristdresponse, outlieristdresponsepercentdeviation, outlierlibrarymatchscore, outliermassaccuracy, outliermassmatchscore, outliermatrixspikegrouprecovery, outliermatrixspikeoutoflimits, outliermatrixspikeoutsidepercentdeviation, outliermatrixspikepercentrecovery, outliernumberofverifiedions, outlieroutofcalibrationrange, outlierplates, outlierpurity, outlierqclcsrecoveryoutoflimits, outlierqcoutoflimits, outlierqcoutsidersd, outlierqvalue, outlierrelativeretentiontime, outlierresolutionfront, outlierresolutionrear, outlierretentiontime, outliersampleamountoutoflimits, outliersampleoutsidersd, outliersaturationrecovery, outliersignaltonoiseratiobelowlimit, outliersurrogateoutoflimits, outliersurrogatepercentrecovery, outliersymmetry, plates, promotehit, purity, qvaluecomputed, qvaluesort, referencelibrarymatchscore, relativeretentiontime, resolutionfront, resolutionrear, responseratio, retentionindex, retentiontime, retentiontimedifference, retentiontimedifferencekey, retentiontimeoriginal, samplersd, saturationrecoveryratio, selectedgroupretentiontime, selectedtargetretentiontime, signaltonoiseratio, surrogatepercentrecovery, symmetry, targetresponse, targetresponseoriginal, usercustomcalculation, usercustomcalculation1, usercustomcalculation2, usercustomcalculation3, usercustomcalculation4, width)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
""")
                cursor.execute(insert_query, (Peaks_items_data.get('assetId', None),
                                              Peaks_items_data.get(
                                                  'BatchID', None),
                                              Peaks_items_data.get(
                                                  'SampleID', None),
                                              Peaks_items_data.get(
                                                  'CompoundID', None),
                                              Peaks_items_data.get(
                                                  'PeakID', None),
                                              Peaks_items_data.get(
                                                  'Accuracy', None),
                                              Peaks_items_data.get(
                                                  'AlternativePeakRTDiff', None),
                                              Peaks_items_data.get(
                                                  'AlternativeTargetHit', None),
                                              Peaks_items_data.get(
                                                  'Area', None),
                                              Peaks_items_data.get(
                                                  'AreaCorrectionResponse', None),
                                              Peaks_items_data.get(
                                                  'BaselineDraw', None),
                                              Peaks_items_data.get(
                                                  'BaselineEnd', None),
                                              Peaks_items_data.get(
                                                  'BaselineEndOriginal', None),
                                              Peaks_items_data.get(
                                                  'BaselineStandardDeviation', None),
                                              Peaks_items_data.get(
                                                  'BaselineStart', None),
                                              Peaks_items_data.get(
                                                  'BaselineStartOriginal', None),
                                              Peaks_items_data.get(
                                                  'CalculatedConcentration', None),
                                              Peaks_items_data.get(
                                                  'CapacityFactor', None),
                                              Peaks_items_data.get(
                                                  'CCISTDResponseRatio', None),
                                              Peaks_items_data.get(
                                                  'CCResponseRatio', None),
                                              Peaks_items_data.get(
                                                  'EstimatedConcentration', None),
                                              Peaks_items_data.get(
                                                  'FinalConcentration', None),
                                              Peaks_items_data.get(
                                                  'FullWidthHalfMaximum', None),
                                              Peaks_items_data.get(
                                                  'GroupNumber', None),
                                              Peaks_items_data.get(
                                                  'Height', None),
                                              Peaks_items_data.get(
                                                  'IntegrationEndTime', None),
                                              Peaks_items_data.get(
                                                  'IntegrationEndTimeOriginal', None),
                                              Peaks_items_data.get(
                    'IntegrationMetricQualityFlags', None),
                    Peaks_items_data.get('IntegrationQualityMetric', None),
                    Peaks_items_data.get('IntegrationStartTime', None),
                    Peaks_items_data.get('IntegrationStartTimeOriginal', None),
                    Peaks_items_data.get('ISTDConcentrationRatio', None),
                    Peaks_items_data.get('ISTDResponsePercentDeviation', None),
                    Peaks_items_data.get('ISTDResponseRatio', None),
                    Peaks_items_data.get('ManuallyIntegrated', None),
                    Peaks_items_data.get('MassAbundanceScore', None),
                    Peaks_items_data.get('MassAccuracy', None),
                    Peaks_items_data.get('MassAccuracyScore', None),
                    Peaks_items_data.get('MassMatchScore', None),
                    Peaks_items_data.get('MassSpacingScore', None),
                    Peaks_items_data.get('MatrixSpikePercentDeviation', None),
                    Peaks_items_data.get('MatrixSpikePercentRecovery', None),
                    Peaks_items_data.get('MZ', None),
                    Peaks_items_data.get('Noise', None),
                    Peaks_items_data.get('NoiseRegions', None),
                    Peaks_items_data.get('NumberOfVerifiedIons', None),
                    Peaks_items_data.get('OutlierAccuracy', None),
                    Peaks_items_data.get('OutlierBelowLimitOfDetection', None),
                    Peaks_items_data.get(
                        'OutlierBelowLimitOfQuantitation', None),
                    Peaks_items_data.get(
                        'OutlierBlankConcentrationOutsideLimit', None),
                    Peaks_items_data.get('OutlierCapacityFactor', None),
                    Peaks_items_data.get('OutlierCCISTDResponseRatio', None),
                    Peaks_items_data.get('OutlierCCResponseRatio', None),
                    Peaks_items_data.get('OutlierCCRetentionTime', None),
                    Peaks_items_data.get('OutlierFullWidthHalfMaximum', None),
                    Peaks_items_data.get(
                        'OutlierIntegrationQualityMetric', None),
                    Peaks_items_data.get('OutlierISTDResponse', None),
                    Peaks_items_data.get(
                        'OutlierISTDResponsePercentDeviation', None),
                    Peaks_items_data.get('OutlierLibraryMatchScore', None),
                    Peaks_items_data.get('OutlierMassAccuracy', None),
                    Peaks_items_data.get('OutlierMassMatchScore', None),
                    Peaks_items_data.get(
                        'OutlierMatrixSpikeGroupRecovery', None),
                    Peaks_items_data.get(
                        'OutlierMatrixSpikeOutOfLimits', None),
                    Peaks_items_data.get(
                        'OutlierMatrixSpikeOutsidePercentDeviation', None),
                    Peaks_items_data.get(
                        'OutlierMatrixSpikePercentRecovery', None),
                    Peaks_items_data.get('OutlierNumberOfVerifiedIons', None),
                    Peaks_items_data.get('OutlierOutOfCalibrationRange', None),
                    Peaks_items_data.get('OutlierPlates', None),
                    Peaks_items_data.get('OutlierPurity', None),
                    Peaks_items_data.get(
                        'OutlierQCLCSRecoveryOutOfLimits', None),
                    Peaks_items_data.get('OutlierQCOutOfLimits', None),
                    Peaks_items_data.get('OutlierQCOutsideRSD', None),
                    Peaks_items_data.get('OutlierQValue', None),
                    Peaks_items_data.get('OutlierRelativeRetentionTime', None),
                    Peaks_items_data.get('OutlierResolutionFront', None),
                    Peaks_items_data.get('OutlierResolutionRear', None),
                    Peaks_items_data.get('OutlierRetentionTime', None),
                    Peaks_items_data.get(
                        'OutlierSampleAmountOutOfLimits', None),
                    Peaks_items_data.get('OutlierSampleOutsideRSD', None),
                    Peaks_items_data.get('OutlierSaturationRecovery', None),
                    Peaks_items_data.get(
                        'OutlierSignalToNoiseRatioBelowLimit', None),
                    Peaks_items_data.get('OutlierSurrogateOutOfLimits', None),
                    Peaks_items_data.get(
                        'OutlierSurrogatePercentRecovery', None),
                    Peaks_items_data.get('OutlierSymmetry', None),
                    Peaks_items_data.get('Plates', None),
                    Peaks_items_data.get('PromoteHit', None),
                    Peaks_items_data.get('Purity', None),
                    Peaks_items_data.get('QValueComputed', None),
                    Peaks_items_data.get('QValueSort', None),
                    Peaks_items_data.get('ReferenceLibraryMatchScore', None),
                    Peaks_items_data.get('RelativeRetentionTime', None),
                    Peaks_items_data.get('ResolutionFront', None),
                    Peaks_items_data.get('ResolutionRear', None),
                    Peaks_items_data.get('ResponseRatio', None),
                    Peaks_items_data.get('RetentionIndex', None),
                    Peaks_items_data.get('RetentionTime', None),
                    Peaks_items_data.get('RetentionTimeDifference', None),
                    Peaks_items_data.get('RetentionTimeDifferenceKey', None),
                    Peaks_items_data.get('RetentionTimeOriginal', None),
                    Peaks_items_data.get('SampleRSD', None),
                    Peaks_items_data.get('SaturationRecoveryRatio', None),
                    Peaks_items_data.get('SelectedGroupRetentionTime', None),
                    Peaks_items_data.get('SelectedTargetRetentionTime', None),
                    Peaks_items_data.get('SignalToNoiseRatio', None),
                    Peaks_items_data.get('SurrogatePercentRecovery', None),
                    Peaks_items_data.get('Symmetry', None),
                    Peaks_items_data.get('TargetResponse', None),
                    Peaks_items_data.get('TargetResponseOriginal', None),
                    Peaks_items_data.get('UserCustomCalculation', None),
                    Peaks_items_data.get('UserCustomCalculation1', None),
                    Peaks_items_data.get('UserCustomCalculation2', None),
                    Peaks_items_data.get('UserCustomCalculation3', None),
                    Peaks_items_data.get('UserCustomCalculation4', None),
                    Peaks_items_data.get('Width', None),))
                
        sql_query = """INSERT INTO samples_raw_data.peaks(
	assetid, batchid, sampleid, compoundid, peakid, accuracy, alternativepeakrtdiff, alternativetargethit, area, areacorrectionresponse, baselinedraw, baselineend, baselineendoriginal, baselinestandarddeviation, baselinestart, baselinestartoriginal, calculatedconcentration, capacityfactor, ccistdresponseratio, ccresponseratio, estimatedconcentration, finalconcentration, fullwidthhalfmaximum, groupnumber, height, integrationendtime, integrationendtimeoriginal, integrationmetricqualityflags, integrationqualitymetric, integrationstarttime, integrationstarttimeoriginal, istdconcentrationratio, istdresponsepercentdeviation, istdresponseratio, manuallyintegrated, massabundancescore, massaccuracy, massaccuracyscore, massmatchscore, massspacingscore, matrixspikepercentdeviation, matrixspikepercentrecovery, mz, noise, noiseregions, numberofverifiedions, outlieraccuracy, outlierbelowlimitofdetection, outlierbelowlimitofquantitation, outlierblankconcentrationoutsidelimit, outliercapacityfactor, outlierccistdresponseratio, outlierccresponseratio, outlierccretentiontime, outlierfullwidthhalfmaximum, outlierintegrationqualitymetric, outlieristdresponse, outlieristdresponsepercentdeviation, outlierlibrarymatchscore, outliermassaccuracy, outliermassmatchscore, outliermatrixspikegrouprecovery, outliermatrixspikeoutoflimits, outliermatrixspikeoutsidepercentdeviation, outliermatrixspikepercentrecovery, outliernumberofverifiedions, outlieroutofcalibrationrange, outlierplates, outlierpurity, outlierqclcsrecoveryoutoflimits, outlierqcoutoflimits, outlierqcoutsidersd, outlierqvalue, outlierrelativeretentiontime, outlierresolutionfront, outlierresolutionrear, outlierretentiontime, outliersampleamountoutoflimits, outliersampleoutsidersd, outliersaturationrecovery, outliersignaltonoiseratiobelowlimit, outliersurrogateoutoflimits, outliersurrogatepercentrecovery, outliersymmetry, plates, promotehit, purity, qvaluecomputed, qvaluesort, referencelibrarymatchscore, relativeretentiontime, resolutionfront, resolutionrear, responseratio, retentionindex, retentiontime, retentiontimedifference, retentiontimedifferencekey, retentiontimeoriginal, samplersd, saturationrecoveryratio, selectedgroupretentiontime, selectedtargetretentiontime, signaltonoiseratio, surrogatepercentrecovery, symmetry, targetresponse, targetresponseoriginal, usercustomcalculation, usercustomcalculation1, usercustomcalculation2, usercustomcalculation3, usercustomcalculation4, width)
select 
	assetid, batchid, sampleid, compoundid, peakid, accuracy, alternativepeakrtdiff, alternativetargethit, area, areacorrectionresponse, baselinedraw, baselineend, baselineendoriginal, baselinestandarddeviation, baselinestart, baselinestartoriginal, calculatedconcentration, capacityfactor, ccistdresponseratio, ccresponseratio, estimatedconcentration, finalconcentration, fullwidthhalfmaximum, groupnumber, height, integrationendtime, integrationendtimeoriginal, integrationmetricqualityflags, integrationqualitymetric, integrationstarttime, integrationstarttimeoriginal, istdconcentrationratio, istdresponsepercentdeviation, istdresponseratio, manuallyintegrated, massabundancescore, massaccuracy, massaccuracyscore, massmatchscore, massspacingscore, matrixspikepercentdeviation, matrixspikepercentrecovery, mz, noise, noiseregions, numberofverifiedions, outlieraccuracy, outlierbelowlimitofdetection, outlierbelowlimitofquantitation, outlierblankconcentrationoutsidelimit, outliercapacityfactor, outlierccistdresponseratio, outlierccresponseratio, outlierccretentiontime, outlierfullwidthhalfmaximum, outlierintegrationqualitymetric, outlieristdresponse, outlieristdresponsepercentdeviation, outlierlibrarymatchscore, outliermassaccuracy, outliermassmatchscore, outliermatrixspikegrouprecovery, outliermatrixspikeoutoflimits, outliermatrixspikeoutsidepercentdeviation, outliermatrixspikepercentrecovery, outliernumberofverifiedions, outlieroutofcalibrationrange, outlierplates, outlierpurity, outlierqclcsrecoveryoutoflimits, outlierqcoutoflimits, outlierqcoutsidersd, outlierqvalue, outlierrelativeretentiontime, outlierresolutionfront, outlierresolutionrear, outlierretentiontime, outliersampleamountoutoflimits, outliersampleoutsidersd, outliersaturationrecovery, outliersignaltonoiseratiobelowlimit, outliersurrogateoutoflimits, outliersurrogatepercentrecovery, outliersymmetry, plates, promotehit, purity, qvaluecomputed, qvaluesort, referencelibrarymatchscore, relativeretentiontime, resolutionfront, resolutionrear, responseratio, retentionindex, retentiontime, retentiontimedifference, retentiontimedifferencekey, retentiontimeoriginal, samplersd, saturationrecoveryratio, selectedgroupretentiontime, selectedtargetretentiontime, signaltonoiseratio, surrogatepercentrecovery, symmetry, targetresponse, targetresponseoriginal, usercustomcalculation, usercustomcalculation1, usercustomcalculation2, usercustomcalculation3, usercustomcalculation4, width
from test_db.peaks;
"""
        cursor.execute(sql_query)
        # Commit the transaction and close the connection
        conn.commit()
        print("Data Ingested Into Production DB For Peaks Data")
        

    except Exception as e:
        # Rollback in case of any error
        conn.rollback()
        print("Error executing the Peaks query:",e)
        with open("error_log.txt", "a") as file:
            file.write(f"Error executing the peaks query: {e}\n")

    finally:
        # Close the cursor and connection
        cursor.close()
