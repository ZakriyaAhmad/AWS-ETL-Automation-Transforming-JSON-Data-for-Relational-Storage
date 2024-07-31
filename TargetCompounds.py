import json
import math
import boto3
import os
import zipfile
import uuid
import datetime
from psycopg2 import sql

# storing batch data in the tables
def target_compounds(data, unique_id, conn):
    try:
        cursor = conn.cursor()
        truncate_query = sql.SQL("TRUNCATE TABLE test_db.target_compounds;")
        cursor.execute(truncate_query)
        
        for target_list in data['TargetCompounds']:

            Target_compounds_data = {
                k: v for k, v in target_list['Items'].items()}

            def get_value(key):
                try:
                    return Target_compounds_data['data'].get(key, '')
                except:
                    return Target_compounds_data.get(key, '')
            Target_compounds_items_data = {
                'BatchID': get_value('BatchID'),
                'SampleID': get_value('SampleID'),
                'CompoundID': get_value('CompoundID'),
                'AccuracyLimitMultiplierLOQ': get_value('AccuracyLimitMultiplierLOQ'),
                'AccuracyMaximumPercentDeviation': get_value('AccuracyMaximumPercentDeviation'),
                'AgilentID': get_value('AgilentID'),
                'AlternativePeakCriteria': get_value('AlternativePeakCriteria'),
                'AlternativePeakID': get_value('AlternativePeakID'),
                'AreaCorrectionFactor': get_value('AreaCorrectionFactor'),
                'AreaCorrectionSelectedMZ': get_value('AreaCorrectionSelectedMZ'),
                'AreaCorrectionMZ': get_value('AreaCorrectionMZ'),
                'AverageRelativeRetentionTime': get_value('AverageRelativeRetentionTime'),
                'AverageResponseFactor': get_value('AverageResponseFactor'),
                'AverageResponseFactorRSD': get_value('AverageResponseFactorRSD'),
                'BlankResponseOffset': get_value('BlankResponseOffset'),
                'CalibrationRangeFilter': get_value('CalibrationRangeFilter'),
                'CalibrationReferenceCompoundID': get_value('CalibrationReferenceCompoundID'),
                'CapacityFactorLimit': get_value('CapacityFactorLimit'),
                'CASNumber': get_value('CASNumber'),
                'CCISTDResponseRatioLimitHigh': get_value('CCISTDResponseRatioLimitHigh'),
                'CCISTDResponseRatioLimitLow': get_value('CCISTDResponseRatioLimitLow'),
                'CCResponseRatioLimitHigh': get_value('CCResponseRatioLimitHigh'),
                'CCResponseRatioLimitLow': get_value('CCResponseRatioLimitLow'),
                'CellAcceleratorVoltage': get_value('CellAcceleratorVoltage'),
                'CoelutionScoreLimit': get_value('CoelutionScoreLimit'),
                'CollisionEnergy': get_value('CollisionEnergy'),
                'CollisionEnergyDelta': get_value('CollisionEnergyDelta'),
                'ColumnVoidTime': get_value('ColumnVoidTime'),
                'CompoundApproved': get_value('CompoundApproved'),
                'CompoundGroup': get_value('CompoundGroup'),
                'CompoundMath': get_value('CompoundMath'),
                'CompoundName': get_value('CompoundName'),
                'CompoundType': get_value('CompoundType'),
                'ConcentrationUnits': get_value('ConcentrationUnits'),
                'CurveFit': get_value('CurveFit'),
                'CurveFitFormula': get_value('CurveFitFormula'),
                'CurveFitLimitHigh': get_value('CurveFitLimitHigh'),
                'CurveFitLimitLow': get_value('CurveFitLimitLow'),
                'CurveFitMinimumR': get_value('CurveFitMinimumR'),
                'CurveFitMinimumR2': get_value('CurveFitMinimumR2'),
                'CurveFitOrigin': get_value('CurveFitOrigin'),
                'CurveFitR': get_value('CurveFitR'),
                'CurveFitR2': get_value('CurveFitR2'),
                'CurveFitStatus': get_value('CurveFitStatus'),
                'CurveFitWeight': get_value('CurveFitWeight'),
                'DilutionHighestConcentration': get_value('DilutionHighestConcentration'),
                'DilutionPattern': get_value('DilutionPattern'),
                'DynamicTargetCompoundID': get_value('DynamicTargetCompoundID'),
                'DynamicTargetRank': get_value('DynamicTargetRank'),
                'ExpectedConcentration': get_value('ExpectedConcentration'),
                'FragmentorVoltage': get_value('FragmentorVoltage'),
                'FragmentorVoltageDelta': get_value('FragmentorVoltageDelta'),
                'FullWidthHalfMaximumLimitHigh': get_value('FullWidthHalfMaximumLimitHigh'),
                'FullWidthHalfMaximumLimitLow': get_value('FullWidthHalfMaximumLimitLow'),
                'GraphicPeakChromatogram': get_value('GraphicPeakChromatogram'),
                'GraphicPeakQualifiers': get_value('GraphicPeakQualifiers'),
                'GraphicPeakSpectrum': get_value('GraphicPeakSpectrum'),
                'GraphicTargetCompoundCalibration': get_value('GraphicTargetCompoundCalibration'),
                'ID': get_value('ID'),
                'IntegrationParameters': get_value('IntegrationParameters'),
                'IntegrationParametersModified': get_value('IntegrationParametersModified'),
                'Integrator': get_value('Integrator'),
                'IonPolarity': get_value('IonPolarity'),
                'IonSource': get_value('IonSource'),
                'ISTDCompoundID': get_value('ISTDCompoundID'),
                'ISTDConcentration': get_value('ISTDConcentration'),
                'ISTDFlag': get_value('ISTDFlag'),
                'ISTDResponseLimitHigh': get_value('ISTDResponseLimitHigh'),
                'ISTDResponseLimitLow': get_value('ISTDResponseLimitLow'),
                'ISTDResponseMaximumPercentDeviation': get_value('ISTDResponseMaximumPercentDeviation'),
                'ISTDResponseMinimumPercentDeviation': get_value('ISTDResponseMinimumPercentDeviation'),
                'KEGGID': get_value('KEGGID'),
                'LeftRetentionTimeDelta': get_value('LeftRetentionTimeDelta'),
                'LibraryMatchScore': get_value('LibraryMatchScore'),
                'LibraryMatchScoreMinimum': get_value('LibraryMatchScoreMinimum'),
                'LibraryRetentionIndex': get_value('LibraryRetentionIndex'),
                'LibraryRetentionTime': get_value('LibraryRetentionTime'),
                'LimitOfDetection': get_value('LimitOfDetection'),
                'LimitOfQuantitation': get_value('LimitOfQuantitation'),
                'LinearResponseRangeMax': get_value('LinearResponseRangeMax'),
                'LinearResponseRangeMin': get_value('LinearResponseRangeMin'),
                'MassAccuracyLimit': get_value('MassAccuracyLimit'),
                'MassMatchScoreMinimum': get_value('MassMatchScoreMinimum'),
                'MatrixAConcentrationLimitHigh': get_value('MatrixAConcentrationLimitHigh'),
                'MatrixAConcentrationLimitLow': get_value('MatrixAConcentrationLimitLow'),
                'MatrixBConcentrationLimitHigh': get_value('MatrixBConcentrationLimitHigh'),
                'MatrixBConcentrationLimitLow': get_value('MatrixBConcentrationLimitLow'),
                'MatrixSpikeBConcentration': get_value('MatrixSpikeBConcentration'),
                'MatrixSpikeBPercentRecoveryMaximum': get_value('MatrixSpikeBPercentRecoveryMaximum'),
                'MatrixSpikeBPercentRecoveryMinimum': get_value('MatrixSpikeBPercentRecoveryMinimum'),
                'MatrixSpikeConcentration': get_value('MatrixSpikeConcentration'),
                'MatrixSpikeMaximumPercentDeviation': get_value('MatrixSpikeMaximumPercentDeviation'),
                'MatrixSpikeBMaximumPercentDeviation': get_value('MatrixSpikeBMaximumPercentDeviation'),
                'MatrixSpikePercentRecoveryMaximum': get_value('MatrixSpikePercentRecoveryMaximum'),
                'MatrixSpikePercentRecoveryMinimum': get_value('MatrixSpikePercentRecoveryMinimum'),
                'MatrixTypeOverride': get_value('MatrixTypeOverride'),
                'MaximumAverageResponseFactorRSD': get_value('MaximumAverageResponseFactorRSD'),
                'MaximumBlankConcentration': get_value('MaximumBlankConcentration'),
                'MaximumBlankResponse': get_value('MaximumBlankResponse'),
                'MaximumCCResponseFactorDeviation': get_value('MaximumCCResponseFactorDeviation'),
                'MaximumNumberOfHits': get_value('MaximumNumberOfHits'),
                'MaximumPercentResidual': get_value('MaximumPercentResidual'),
                'MethodDetectionLimit': get_value('MethodDetectionLimit'),
                'MinimumAssignedNoise': get_value('MinimumAssignedNoise'),
                'MinimumAverageResponseFactor': get_value('MinimumAverageResponseFactor'),
                'MinimumCCRelativeResponseFactor': get_value('MinimumCCRelativeResponseFactor'),
                'MinimumPercentPurity': get_value('MinimumPercentPurity'),
                'MinimumSignalToNoiseRatio': get_value('MinimumSignalToNoiseRatio'),
                'MolecularFormula': get_value('MolecularFormula'),
                'Multiplier': get_value('Multiplier'),
                'MZ': get_value('MZ'),
                'MZAdditional': get_value('MZAdditional'),
                'MZExtractionWindowFilterLeft': get_value('MZExtractionWindowFilterLeft'),
                'MZExtractionWindowFilterRight': get_value('MZExtractionWindowFilterRight'),
                'MZExtractionWindowUnits': get_value('MZExtractionWindowUnits'),
                'MZScanRangeHigh': get_value('MZScanRangeHigh'),
                'MZScanRangeLow': get_value('MZScanRangeLow'),
                'NeutralLossGain': get_value('NeutralLossGain'),
                'NoiseAlgorithmType': get_value('NoiseAlgorithmType'),
                'NoiseOfRawSignal': get_value('NoiseOfRawSignal'),
                'NoiseReference': get_value('NoiseReference'),
                'NoiseRegions': get_value('NoiseRegions'),
                'NoiseStandardDeviationMultiplier': get_value('NoiseStandardDeviationMultiplier'),
                'NonReferenceWindowOverride': get_value('NonReferenceWindowOverride'),
                'NumberOfVerifiedIonsLimit': get_value('NumberOfVerifiedIonsLimit'),
                'OutlierAlternativePeak': get_value('OutlierAlternativePeak'),
                'OutlierAverageResponseFactor': get_value('OutlierAverageResponseFactor'),
                'OutlierAverageResponseFactorRSD': get_value('OutlierAverageResponseFactorRSD'),
                'OutlierBlankResponseOutsideLimit': get_value('OutlierBlankResponseOutsideLimit'),
                'OutlierCCAverageResponseFactor': get_value('OutlierCCAverageResponseFactor'),
                'OutlierCCRelativeResponseFactor': get_value('OutlierCCRelativeResponseFactor'),
                'OutlierCurveFitR': get_value('OutlierCurveFitR'),
                'OutlierCustomCalculation': get_value('OutlierCustomCalculation'),
                'OutlierMethodDetectionLimit': get_value('OutlierMethodDetectionLimit'),
                'OutlierMinimumCurveFitR2': get_value('OutlierMinimumCurveFitR2'),
                'OutlierPeakNotFound': get_value('OutlierPeakNotFound'),
                'OutlierRelativeResponseFactor': get_value('OutlierRelativeResponseFactor'),
                'OutlierRelativeStandardError': get_value('OutlierRelativeStandardError'),
                'OutlierResponseCheckBelowLimit': get_value('OutlierResponseCheckBelowLimit'),
                'OutlierResponseFactor': get_value('OutlierResponseFactor'),
                'PeakFilterThreshold': get_value('PeakFilterThreshold'),
                'PeakFilterThresholdValue': get_value('PeakFilterThresholdValue'),
                'PeakSelectionCriterion': get_value('PeakSelectionCriterion'),
                'PlatesCalculationType': get_value('PlatesCalculationType'),
                'PlatesLimit': get_value('PlatesLimit'),
                'PrimaryHitPeakID': get_value('PrimaryHitPeakID'),
                'QCLCSMaximumRecoveryA': get_value('QCLCSMaximumRecoveryA'),
                'QCLCSMaximumRecoveryB': get_value('QCLCSMaximumRecoveryB'),
                'QCLCSMinimumRecoveryA': get_value('QCLCSMinimumRecoveryA'),
                'QCLCSMinimumRecoveryB': get_value('QCLCSMinimumRecoveryB'),
                'QCMaximumDeviation': get_value('QCMaximumDeviation'),
                'QCMaximumPercentRSD': get_value('QCMaximumPercentRSD'),
                'QualifierRatioMethod': get_value('QualifierRatioMethod'),
                'QuantitateByHeight': get_value('QuantitateByHeight'),
                'QuantitationMessage': get_value('QuantitationMessage'),
                'QValueMinimum': get_value('QValueMinimum'),
                'ReferenceMSPathName': get_value('ReferenceMSPathName'),
                'ReferenceWindowOverride': get_value('ReferenceWindowOverride'),
                'RelativeISTDMultiplier': get_value('RelativeISTDMultiplier'),
                'RelativeResponseFactorMaximumPercentDeviation': get_value('RelativeResponseFactorMaximumPercentDeviation'),
                'RelativeRetentionTimeMaximumPercentDeviation': get_value('RelativeRetentionTimeMaximumPercentDeviation'),
                'RelativeStandardError': get_value('RelativeStandardError'),
                'RelativeStandardErrorMaximum': get_value('RelativeStandardErrorMaximum'),
                'ResolutionCalculationType': get_value('ResolutionCalculationType'),
                'ResolutionLimit': get_value('ResolutionLimit'),
                'ResponseCheckMinimum': get_value('ResponseCheckMinimum'),
                'ResponseFactorMaximumPercentDeviation': get_value('ResponseFactorMaximumPercentDeviation'),
                'RetentionIndex': get_value('RetentionIndex'),
                'RetentionTime': get_value('RetentionTime'),
                'RetentionTimeDeltaUnits': get_value('RetentionTimeDeltaUnits'),
                'RetentionTimeWindow': get_value('RetentionTimeWindow'),
                'RetentionTimeWindowCC': get_value('RetentionTimeWindowCC'),
                'RetentionTimeWindowUnits': get_value('RetentionTimeWindowUnits'),
                'RightRetentionTimeDelta': get_value('RightRetentionTimeDelta'),
                'RxUnlabeledIsotopicDilution': get_value('RxUnlabeledIsotopicDilution'),
                'RyLabeledIsotopicDilution': get_value('RyLabeledIsotopicDilution'),
                'SampleAmountLimitHigh': get_value('SampleAmountLimitHigh'),
                'SampleAmountLimitLow': get_value('SampleAmountLimitLow'),
                'SampleMaximumPercentRSD': get_value('SampleMaximumPercentRSD'),
                'ScanType': get_value('ScanType'),
                'SelectedMZ': get_value('SelectedMZ'),
                'SignalInstance': get_value('SignalInstance'),
                'SignalName': get_value('SignalName'),
                'SignalRetentionTimeOffset': get_value('SignalRetentionTimeOffset'),
                'SignalToNoiseMultiplier': get_value('SignalToNoiseMultiplier'),
                'SignalType': get_value('SignalType'),
                'Smoothing': get_value('Smoothing'),
                'SmoothingFunctionWidth': get_value('SmoothingFunctionWidth'),
                'SmoothingGaussianWidth': get_value('SmoothingGaussianWidth'),
                'Species': get_value('Species'),
                'SpectrumBaselineThreshold': get_value('SpectrumBaselineThreshold'),
                'SpectrumExtractionOverride': get_value('SpectrumExtractionOverride'),
                'SpectrumExtractionOverrideCollisionEnergy': get_value('SpectrumExtractionOverrideCollisionEnergy'),
                'SpectrumScanInclusion': get_value('SpectrumScanInclusion'),
                'SpectrumPeakHeightPercentThreshold': get_value('SpectrumPeakHeightPercentThreshold'),
                'SpectrumPercentSaturationThreshold': get_value('SpectrumPercentSaturationThreshold'),
                'SpectrumQuantifierQualifierOnly': get_value('SpectrumQuantifierQualifierOnly'),
                'Sublist': get_value('Sublist'),
                'SurrogateConcentration': get_value('SurrogateConcentration'),
                'SurrogateConcentrationLimitHigh': get_value('SurrogateConcentrationLimitHigh'),
                'SurrogateConcentrationLimitLow': get_value('SurrogateConcentrationLimitLow'),
                'SurrogatePercentRecoveryMaximum': get_value('SurrogatePercentRecoveryMaximum'),
                'SurrogatePercentRecoveryMinimum': get_value('SurrogatePercentRecoveryMinimum'),
                'SymmetryCalculationType': get_value('SymmetryCalculationType'),
                'SymmetryLimitHigh': get_value('SymmetryLimitHigh'),
                'SymmetryLimitLow': get_value('SymmetryLimitLow'),
                'TargetCompoundIDStatus': get_value('TargetCompoundIDStatus'),
                'ThresholdNumberOfPeaks': get_value('ThresholdNumberOfPeaks'),
                'TimeReferenceFlag': get_value('TimeReferenceFlag'),
                'TimeSegment': get_value('TimeSegment'),
                'Transition': get_value('Transition'),
                'TriggeredTransitions': get_value('TriggeredTransitions'),
                'UncertaintyRelativeOrAbsolute': get_value('UncertaintyRelativeOrAbsolute'),
                'UserAnnotation': get_value('UserAnnotation'),
                'UserCustomCalculation': get_value('UserCustomCalculation'),
                'UserCustomCalculationLimitHigh': get_value('UserCustomCalculationLimitHigh'),
                'UserCustomCalculationLimitLow': get_value('UserCustomCalculationLimitLow'),
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
                'UserDefinedTargetCompoundID': get_value('UserDefinedTargetCompoundID'),
                'VerifiedIonType': get_value('VerifiedIonType'),
                'WavelengthExtractionRangeHigh': get_value('WavelengthExtractionRangeHigh'),
                'WavelengthExtractionRangeLow': get_value('WavelengthExtractionRangeLow'),
                'WavelengthReferenceRangeHigh': get_value('WavelengthReferenceRangeHigh'),
                'WavelengthReferenceRangeLow': get_value('WavelengthReferenceRangeLow')
            }

            uu_id = str(unique_id)
            Target_compounds_items_data['assetId'] = uu_id
            
            
            insert_query = sql.SQL("""
    INSERT INTO test_db.target_compounds(
	assetid, batchid, sampleid, compoundid, accuracylimitmultiplierloq, accuracymaximumpercentdeviation, agilentid, alternativepeakcriteria, alternativepeakid, areacorrectionfactor, areacorrectionselectedmz, areacorrectionmz, averagerelativeretentiontime, averageresponsefactor, averageresponsefactorrsd, blankresponseoffset, calibrationrangefilter, calibrationreferencecompoundid, capacityfactorlimit, casnumber, ccistdresponseratiolimithigh, ccistdresponseratiolimitlow, ccresponseratiolimithigh, ccresponseratiolimitlow, cellacceleratorvoltage, coelutionscorelimit, collisionenergy, collisionenergydelta, columnvoidtime, compoundapproved, compoundgroup, compoundmath, compoundname, compoundtype, concentrationunits, curvefit, curvefitformula, curvefitlimithigh, curvefitlimitlow, curvefitminimumr, curvefitminimumr2, curvefitorigin, curvefitr, curvefitr2, curvefitstatus, curvefitweight, dilutionhighestconcentration, dilutionpattern, dynamictargetcompoundid, dynamictargetrank, expectedconcentration, fragmentorvoltage, fragmentorvoltagedelta, fullwidthhalfmaximumlimithigh, fullwidthhalfmaximumlimitlow, graphicpeakchromatogram, graphicpeakqualifiers, graphicpeakspectrum, graphictargetcompoundcalibration, integrationparameters, integrationparametersmodified, integrator, ionpolarity, ionsource, istdcompoundid, istdconcentration, istdflag, istdresponselimithigh, istdresponselimitlow, istdresponsemaximumpercentdeviation, istdresponseminimumpercentdeviation, keggid, leftretentiontimedelta, librarymatchscore, librarymatchscoreminimum, libraryretentionindex, libraryretentiontime, limitofdetection, limitofquantitation, linearresponserangemax, linearresponserangemin, massaccuracylimit, massmatchscoreminimum, matrixaconcentrationlimithigh, matrixaconcentrationlimitlow, matrixbconcentrationlimithigh, matrixbconcentrationlimitlow, matrixspikebconcentration, matrixspikebpercentrecoverymaximum, matrixspikebpercentrecoveryminimum, matrixspikeconcentration, matrixspikemaximumpercentdeviation, matrixspikebmaximumpercentdeviation, matrixspikepercentrecoverymaximum, matrixspikepercentrecoveryminimum, matrixtypeoverride, maximumaverageresponsefactorrsd, maximumblankconcentration, maximumblankresponse, maximumccresponsefactordeviation, maximumnumberofhits, maximumpercentresidual, methoddetectionlimit, minimumassignednoise, minimumaverageresponsefactor, minimumccrelativeresponsefactor, minimumpercentpurity, minimumsignaltonoiseratio, molecularformula, multiplier, mz, mzadditional, mzextractionwindowfilterleft, mzextractionwindowfilterright, mzextractionwindowunits, mzscanrangehigh, mzscanrangelow, neutrallossgain, noisealgorithmtype, noiseofrawsignal, noisereference, noiseregions, noisestandarddeviationmultiplier, nonreferencewindowoverride, numberofverifiedionslimit, outlieralternativepeak, outlieraverageresponsefactor, outlieraverageresponsefactorrsd, outlierblankresponseoutsidelimit, outlierccaverageresponsefactor, outlierccrelativeresponsefactor, outliercurvefitr, outliercustomcalculation, outliermethoddetectionlimit, outlierminimumcurvefitr2, outlierpeaknotfound, outlierrelativeresponsefactor, outlierrelativestandarderror, outlierresponsecheckbelowlimit, outlierresponsefactor, peakfilterthreshold, peakfilterthresholdvalue, peakselectioncriterion, platescalculationtype, plateslimit, primaryhitpeakid, qclcsmaximumrecoverya, qclcsmaximumrecoveryb, qclcsminimumrecoverya, qclcsminimumrecoveryb, qcmaximumdeviation, qcmaximumpercentrsd, qualifierratiomethod, quantitatebyheight, quantitationmessage, qvalueminimum, referencemspathname, referencewindowoverride, relativeistdmultiplier, relativeresponsefactormaximumpercentdeviation, relativeretentiontimemaximumpercentdeviation, relativestandarderror, relativestandarderrormaximum, resolutioncalculationtype, resolutionlimit, responsecheckminimum, responsefactormaximumpercentdeviation, retentionindex, retentiontime, retentiontimedeltaunits, retentiontimewindow, retentiontimewindowcc, retentiontimewindowunits, rightretentiontimedelta, rxunlabeledisotopicdilution, rylabeledisotopicdilution, sampleamountlimithigh, sampleamountlimitlow, samplemaximumpercentrsd, scantype, selectedmz, signalinstance, signalname, signalretentiontimeoffset, signaltonoisemultiplier, signaltype, smoothing, smoothingfunctionwidth, smoothinggaussianwidth, species, spectrumbaselinethreshold, spectrumextractionoverride, spectrumextractionoverridecollisionenergy, spectrumscaninclusion, spectrumpeakheightpercentthreshold, spectrumpercentsaturationthreshold, spectrumquantifierqualifieronly, sublist, surrogateconcentration, surrogateconcentrationlimithigh, surrogateconcentrationlimitlow, surrogatepercentrecoverymaximum, surrogatepercentrecoveryminimum, symmetrycalculationtype, symmetrylimithigh, symmetrylimitlow, targetcompoundidstatus, thresholdnumberofpeaks, timereferenceflag, timesegment, transition, triggeredtransitions, uncertaintyrelativeorabsolute, userannotation, usercustomcalculation, usercustomcalculationlimithigh, usercustomcalculationlimitlow, userdefined, userdefined1, userdefined2, userdefined3, userdefined4, userdefined5, userdefined6, userdefined7, userdefined8, userdefined9, userdefinedtargetcompoundid, verifiediontype, wavelengthextractionrangehigh, wavelengthextractionrangelow, wavelengthreferencerangehigh, wavelengthreferencerangelow,"ID")
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);""")
            cursor.execute(insert_query, (
                Target_compounds_items_data.get('assetId', None),
                Target_compounds_items_data.get('BatchID', None),
                Target_compounds_items_data.get('SampleID', None),
                Target_compounds_items_data.get('CompoundID', None),
                Target_compounds_items_data.get(
                    'AccuracyLimitMultiplierLOQ', None),
                Target_compounds_items_data.get(
                    'AccuracyMaximumPercentDeviation', None),
                Target_compounds_items_data.get('AgilentID', None),
                Target_compounds_items_data.get(
                    'AlternativePeakCriteria', None),
                Target_compounds_items_data.get('AlternativePeakID', None),
                Target_compounds_items_data.get('AreaCorrectionFactor', None),
                Target_compounds_items_data.get(
                    'AreaCorrectionSelectedMZ', None),
                Target_compounds_items_data.get('AreaCorrectionMZ', None),
                Target_compounds_items_data.get(
                    'AverageRelativeRetentionTime', None),
                Target_compounds_items_data.get('AverageResponseFactor', None),
                Target_compounds_items_data.get(
                    'AverageResponseFactorRSD', None),
                Target_compounds_items_data.get('BlankResponseOffset', None),
                Target_compounds_items_data.get(
                    'CalibrationRangeFilter', None),
                Target_compounds_items_data.get(
                    'CalibrationReferenceCompoundID', None),
                Target_compounds_items_data.get('CapacityFactorLimit', None),
                Target_compounds_items_data.get('CASNumber', None),
                Target_compounds_items_data.get(
                    'CCISTDResponseRatioLimitHigh', None),
                Target_compounds_items_data.get(
                    'CCISTDResponseRatioLimitLow', None),
                Target_compounds_items_data.get(
                    'CCResponseRatioLimitHigh', None),
                Target_compounds_items_data.get(
                    'CCResponseRatioLimitLow', None),
                Target_compounds_items_data.get(
                    'CellAcceleratorVoltage', None),
                Target_compounds_items_data.get('CoelutionScoreLimit', None),
                Target_compounds_items_data.get('CollisionEnergy', None),
                Target_compounds_items_data.get('CollisionEnergyDelta', None),
                Target_compounds_items_data.get('ColumnVoidTime', None),
                Target_compounds_items_data.get('CompoundApproved', None),
                Target_compounds_items_data.get('CompoundGroup', None),
                Target_compounds_items_data.get('CompoundMath', None),
                Target_compounds_items_data.get('CompoundName', None),
                Target_compounds_items_data.get('CompoundType', None),
                Target_compounds_items_data.get('ConcentrationUnits', None),
                Target_compounds_items_data.get('CurveFit', None),
                Target_compounds_items_data.get('CurveFitFormula', None),
                Target_compounds_items_data.get('CurveFitLimitHigh', None),
                Target_compounds_items_data.get('CurveFitLimitLow', None),
                Target_compounds_items_data.get('CurveFitMinimumR', None),
                Target_compounds_items_data.get('CurveFitMinimumR2', None),
                Target_compounds_items_data.get('CurveFitOrigin', None),
                Target_compounds_items_data.get('CurveFitR', None),
                Target_compounds_items_data.get('CurveFitR2', None),
                Target_compounds_items_data.get('CurveFitStatus', None),
                Target_compounds_items_data.get('CurveFitWeight', None),
                Target_compounds_items_data.get(
                    'DilutionHighestConcentration', None),
                Target_compounds_items_data.get('DilutionPattern', None),
                Target_compounds_items_data.get(
                    'DynamicTargetCompoundID', None),
                Target_compounds_items_data.get('DynamicTargetRank', None),
                Target_compounds_items_data.get('ExpectedConcentration', None),
                Target_compounds_items_data.get('FragmentorVoltage', None),
                Target_compounds_items_data.get(
                    'FragmentorVoltageDelta', None),
                Target_compounds_items_data.get(
                    'FullWidthHalfMaximumLimitHigh', None),
                Target_compounds_items_data.get(
                    'FullWidthHalfMaximumLimitLow', None),
                Target_compounds_items_data.get(
                    'GraphicPeakChromatogram', None),
                Target_compounds_items_data.get('GraphicPeakQualifiers', None),
                Target_compounds_items_data.get('GraphicPeakSpectrum', None),
                Target_compounds_items_data.get(
                    'GraphicTargetCompoundCalibration', None),
                
                Target_compounds_items_data.get('IntegrationParameters', None),
                Target_compounds_items_data.get(
                    'IntegrationParametersModified', None),
                Target_compounds_items_data.get('Integrator', None),
                Target_compounds_items_data.get('IonPolarity', None),
                Target_compounds_items_data.get('IonSource', None),
                Target_compounds_items_data.get('ISTDCompoundID', None),
                Target_compounds_items_data.get('ISTDConcentration', None),
                Target_compounds_items_data.get('ISTDFlag', None),
                Target_compounds_items_data.get('ISTDResponseLimitHigh', None),
                Target_compounds_items_data.get('ISTDResponseLimitLow', None),
                Target_compounds_items_data.get(
                    'ISTDResponseMaximumPercentDeviation', None),
                Target_compounds_items_data.get(
                    'ISTDResponseMinimumPercentDeviation', None),
                Target_compounds_items_data.get('KEGGID', None),
                Target_compounds_items_data.get(
                    'LeftRetentionTimeDelta', None),
                Target_compounds_items_data.get('LibraryMatchScore', None),
                Target_compounds_items_data.get(
                    'LibraryMatchScoreMinimum', None),
                Target_compounds_items_data.get('LibraryRetentionIndex', None),
                Target_compounds_items_data.get('LibraryRetentionTime', None),
                Target_compounds_items_data.get('LimitOfDetection', None),
                Target_compounds_items_data.get('LimitOfQuantitation', None),
                Target_compounds_items_data.get(
                    'LinearResponseRangeMax', None),
                Target_compounds_items_data.get(
                    'LinearResponseRangeMin', None),
                Target_compounds_items_data.get('MassAccuracyLimit', None),
                Target_compounds_items_data.get('MassMatchScoreMinimum', None),
                Target_compounds_items_data.get(
                    'MatrixAConcentrationLimitHigh', None),
                Target_compounds_items_data.get(
                    'MatrixAConcentrationLimitLow', None),
                Target_compounds_items_data.get(
                    'MatrixBConcentrationLimitHigh', None),
                Target_compounds_items_data.get(
                    'MatrixBConcentrationLimitLow', None),
                Target_compounds_items_data.get(
                    'MatrixSpikeBConcentration', None),
                Target_compounds_items_data.get(
                    'MatrixSpikeBPercentRecoveryMaximum', None),
                Target_compounds_items_data.get(
                    'MatrixSpikeBPercentRecoveryMinimum', None),
                Target_compounds_items_data.get(
                    'MatrixSpikeConcentration', None),
                Target_compounds_items_data.get(
                    'MatrixSpikeMaximumPercentDeviation', None),
                Target_compounds_items_data.get(
                    'MatrixSpikeBMaximumPercentDeviation', None),
                Target_compounds_items_data.get(
                    'MatrixSpikePercentRecoveryMaximum', None),
                Target_compounds_items_data.get(
                    'MatrixSpikePercentRecoveryMinimum', None),
                Target_compounds_items_data.get('MatrixTypeOverride', None),
                Target_compounds_items_data.get(
                    'MaximumAverageResponseFactorRSD', None),
                Target_compounds_items_data.get(
                    'MaximumBlankConcentration', None),
                Target_compounds_items_data.get('MaximumBlankResponse', None),
                Target_compounds_items_data.get(
                    'MaximumCCResponseFactorDeviation', None),
                Target_compounds_items_data.get('MaximumNumberOfHits', None),
                Target_compounds_items_data.get(
                    'MaximumPercentResidual', None),
                Target_compounds_items_data.get('MethodDetectionLimit', None),
                Target_compounds_items_data.get('MinimumAssignedNoise', None),
                Target_compounds_items_data.get(
                    'MinimumAverageResponseFactor', None),
                Target_compounds_items_data.get(
                    'MinimumCCRelativeResponseFactor', None),
                Target_compounds_items_data.get('MinimumPercentPurity', None),
                Target_compounds_items_data.get(
                    'MinimumSignalToNoiseRatio', None),
                Target_compounds_items_data.get('MolecularFormula', None),
                Target_compounds_items_data.get('Multiplier', None),
                Target_compounds_items_data.get('MZ', None),
                Target_compounds_items_data.get('MZAdditional', None),
                Target_compounds_items_data.get(
                    'MZExtractionWindowFilterLeft', None),
                Target_compounds_items_data.get(
                    'MZExtractionWindowFilterRight', None),
                Target_compounds_items_data.get(
                    'MZExtractionWindowUnits', None),
                Target_compounds_items_data.get('MZScanRangeHigh', None),
                Target_compounds_items_data.get('MZScanRangeLow', None),
                Target_compounds_items_data.get('NeutralLossGain', None),
                Target_compounds_items_data.get('NoiseAlgorithmType', None),
                Target_compounds_items_data.get('NoiseOfRawSignal', None),
                Target_compounds_items_data.get('NoiseReference', None),
                Target_compounds_items_data.get('NoiseRegions', None),
                Target_compounds_items_data.get(
                    'NoiseStandardDeviationMultiplier', None),
                Target_compounds_items_data.get(
                    'NonReferenceWindowOverride', None),
                Target_compounds_items_data.get(
                    'NumberOfVerifiedIonsLimit', None),
                Target_compounds_items_data.get(
                    'OutlierAlternativePeak', None),
                Target_compounds_items_data.get(
                    'OutlierAverageResponseFactor', None),
                Target_compounds_items_data.get(
                    'OutlierAverageResponseFactorRSD', None),
                Target_compounds_items_data.get(
                    'OutlierBlankResponseOutsideLimit', None),
                Target_compounds_items_data.get(
                    'OutlierCCAverageResponseFactor', None),
                Target_compounds_items_data.get(
                    'OutlierCCRelativeResponseFactor', None),
                Target_compounds_items_data.get('OutlierCurveFitR', None),
                Target_compounds_items_data.get(
                    'OutlierCustomCalculation', None),
                Target_compounds_items_data.get(
                    'OutlierMethodDetectionLimit', None),
                Target_compounds_items_data.get(
                    'OutlierMinimumCurveFitR2', None),
                Target_compounds_items_data.get('OutlierPeakNotFound', None),
                Target_compounds_items_data.get(
                    'OutlierRelativeResponseFactor', None),
                Target_compounds_items_data.get(
                    'OutlierRelativeStandardError', None),
                Target_compounds_items_data.get(
                    'OutlierResponseCheckBelowLimit', None),
                Target_compounds_items_data.get('OutlierResponseFactor', None),
                Target_compounds_items_data.get('PeakFilterThreshold', None),
                Target_compounds_items_data.get(
                    'PeakFilterThresholdValue', None),
                Target_compounds_items_data.get(
                    'PeakSelectionCriterion', None),
                Target_compounds_items_data.get('PlatesCalculationType', None),
                Target_compounds_items_data.get('PlatesLimit', None),
                Target_compounds_items_data.get('PrimaryHitPeakID', None),
                Target_compounds_items_data.get('QCLCSMaximumRecoveryA', None),
                Target_compounds_items_data.get('QCLCSMaximumRecoveryB', None),
                Target_compounds_items_data.get('QCLCSMinimumRecoveryA', None),
                Target_compounds_items_data.get('QCLCSMinimumRecoveryB', None),
                Target_compounds_items_data.get('QCMaximumDeviation', None),
                Target_compounds_items_data.get('QCMaximumPercentRSD', None),
                Target_compounds_items_data.get('QualifierRatioMethod', None),
                Target_compounds_items_data.get('QuantitateByHeight', None),
                Target_compounds_items_data.get('QuantitationMessage', None),
                Target_compounds_items_data.get('QValueMinimum', None),
                Target_compounds_items_data.get('ReferenceMSPathName', None),
                Target_compounds_items_data.get(
                    'ReferenceWindowOverride', None),
                Target_compounds_items_data.get(
                    'RelativeISTDMultiplier', None),
                Target_compounds_items_data.get(
                    'RelativeResponseFactorMaximumPercentDeviation', None),
                Target_compounds_items_data.get(
                    'RelativeRetentionTimeMaximumPercentDeviation', None),
                Target_compounds_items_data.get('RelativeStandardError', None),
                Target_compounds_items_data.get(
                    'RelativeStandardErrorMaximum', None),
                Target_compounds_items_data.get(
                    'ResolutionCalculationType', None),
                Target_compounds_items_data.get('ResolutionLimit', None),
                Target_compounds_items_data.get('ResponseCheckMinimum', None),
                Target_compounds_items_data.get(
                    'ResponseFactorMaximumPercentDeviation', None),
                Target_compounds_items_data.get('RetentionIndex', None),
                Target_compounds_items_data.get('RetentionTime', None),
                Target_compounds_items_data.get(
                    'RetentionTimeDeltaUnits', None),
                Target_compounds_items_data.get('RetentionTimeWindow', None),
                Target_compounds_items_data.get('RetentionTimeWindowCC', None),
                Target_compounds_items_data.get(
                    'RetentionTimeWindowUnits', None),
                Target_compounds_items_data.get(
                    'RightRetentionTimeDelta', None),
                Target_compounds_items_data.get(
                    'RxUnlabeledIsotopicDilution', None),
                Target_compounds_items_data.get(
                    'RyLabeledIsotopicDilution', None),
                Target_compounds_items_data.get('SampleAmountLimitHigh', None),
                Target_compounds_items_data.get('SampleAmountLimitLow', None),
                Target_compounds_items_data.get(
                    'SampleMaximumPercentRSD', None),
                Target_compounds_items_data.get('ScanType', None),
                Target_compounds_items_data.get('SelectedMZ', None),
                Target_compounds_items_data.get('SignalInstance', None),
                Target_compounds_items_data.get('SignalName', None),
                Target_compounds_items_data.get(
                    'SignalRetentionTimeOffset', None),
                Target_compounds_items_data.get(
                    'SignalToNoiseMultiplier', None),
                Target_compounds_items_data.get('SignalType', None),
                Target_compounds_items_data.get('Smoothing', None),
                Target_compounds_items_data.get(
                    'SmoothingFunctionWidth', None),
                Target_compounds_items_data.get(
                    'SmoothingGaussianWidth', None),
                Target_compounds_items_data.get('Species', None),
                Target_compounds_items_data.get(
                    'SpectrumBaselineThreshold', None),
                Target_compounds_items_data.get(
                    'SpectrumExtractionOverride', None),
                Target_compounds_items_data.get(
                    'SpectrumExtractionOverrideCollisionEnergy', None),
                Target_compounds_items_data.get('SpectrumScanInclusion', None),
                Target_compounds_items_data.get(
                    'SpectrumPeakHeightPercentThreshold', None),
                Target_compounds_items_data.get(
                    'SpectrumPercentSaturationThreshold', None),
                Target_compounds_items_data.get(
                    'SpectrumQuantifierQualifierOnly', None),
                Target_compounds_items_data.get('Sublist', None),
                Target_compounds_items_data.get(
                    'SurrogateConcentration', None),
                Target_compounds_items_data.get(
                    'SurrogateConcentrationLimitHigh', None),
                Target_compounds_items_data.get(
                    'SurrogateConcentrationLimitLow', None),
                Target_compounds_items_data.get(
                    'SurrogatePercentRecoveryMaximum', None),
                Target_compounds_items_data.get(
                    'SurrogatePercentRecoveryMinimum', None),
                Target_compounds_items_data.get(
                    'SymmetryCalculationType', None),
                Target_compounds_items_data.get('SymmetryLimitHigh', None),
                Target_compounds_items_data.get('SymmetryLimitLow', None),
                Target_compounds_items_data.get(
                    'TargetCompoundIDStatus', None),
                Target_compounds_items_data.get(
                    'ThresholdNumberOfPeaks', None),
                Target_compounds_items_data.get('TimeReferenceFlag', None),
                Target_compounds_items_data.get('TimeSegment', None),
                Target_compounds_items_data.get('Transition', None),
                Target_compounds_items_data.get('TriggeredTransitions', None),
                Target_compounds_items_data.get(
                    'UncertaintyRelativeOrAbsolute', None),
                Target_compounds_items_data.get('UserAnnotation', None),
                Target_compounds_items_data.get('UserCustomCalculation', None),
                Target_compounds_items_data.get(
                    'UserCustomCalculationLimitHigh', None),
                Target_compounds_items_data.get(
                    'UserCustomCalculationLimitLow', None),
                Target_compounds_items_data.get('UserDefined', None),
                Target_compounds_items_data.get('UserDefined1', None),
                Target_compounds_items_data.get('UserDefined2', None),
                Target_compounds_items_data.get('UserDefined3', None),
                Target_compounds_items_data.get('UserDefined4', None),
                Target_compounds_items_data.get('UserDefined', None),
                Target_compounds_items_data.get('UserDefined6', None),
                Target_compounds_items_data.get('UserDefined7', None),
                Target_compounds_items_data.get('UserDefined8', None),
                Target_compounds_items_data.get('UserDefined9', None),
                Target_compounds_items_data.get(
                    'UserDefinedTargetCompoundID', None),
                Target_compounds_items_data.get('VerifiedIonType', None),
                Target_compounds_items_data.get(
                    'WavelengthExtractionRangeHigh', None),
                Target_compounds_items_data.get(
                    'WavelengthExtractionRangeLow', None),
                Target_compounds_items_data.get(
                    'WavelengthReferenceRangeHigh', None),
                Target_compounds_items_data.get('WavelengthReferenceRangeLow', None),
                Target_compounds_items_data.get('ID', None)))
            
        sql_query = """INSERT INTO samples_raw_data.target_compounds(
	assetid, batchid, sampleid, compoundid, accuracylimitmultiplierloq, accuracymaximumpercentdeviation, agilentid, alternativepeakcriteria, alternativepeakid, areacorrectionfactor, areacorrectionselectedmz, areacorrectionmz, averagerelativeretentiontime, averageresponsefactor, averageresponsefactorrsd, blankresponseoffset, calibrationrangefilter, calibrationreferencecompoundid, capacityfactorlimit, casnumber, ccistdresponseratiolimithigh, ccistdresponseratiolimitlow, ccresponseratiolimithigh, ccresponseratiolimitlow, cellacceleratorvoltage, coelutionscorelimit, collisionenergy, collisionenergydelta, columnvoidtime, compoundapproved, compoundgroup, compoundmath, compoundname, compoundtype, concentrationunits, curvefit, curvefitformula, curvefitlimithigh, curvefitlimitlow, curvefitminimumr, curvefitminimumr2, curvefitorigin, curvefitr, curvefitr2, curvefitstatus, curvefitweight, dilutionhighestconcentration, dilutionpattern, dynamictargetcompoundid, dynamictargetrank, expectedconcentration, fragmentorvoltage, fragmentorvoltagedelta, fullwidthhalfmaximumlimithigh, fullwidthhalfmaximumlimitlow, graphicpeakchromatogram, graphicpeakqualifiers, graphicpeakspectrum, graphictargetcompoundcalibration, integrationparameters, integrationparametersmodified, integrator, ionpolarity, ionsource, istdcompoundid, istdconcentration, istdflag, istdresponselimithigh, istdresponselimitlow, istdresponsemaximumpercentdeviation, istdresponseminimumpercentdeviation, keggid, leftretentiontimedelta, librarymatchscore, librarymatchscoreminimum, libraryretentionindex, libraryretentiontime, limitofdetection, limitofquantitation, linearresponserangemax, linearresponserangemin, massaccuracylimit, massmatchscoreminimum, matrixaconcentrationlimithigh, matrixaconcentrationlimitlow, matrixbconcentrationlimithigh, matrixbconcentrationlimitlow, matrixspikebconcentration, matrixspikebpercentrecoverymaximum, matrixspikebpercentrecoveryminimum, matrixspikeconcentration, matrixspikemaximumpercentdeviation, matrixspikebmaximumpercentdeviation, matrixspikepercentrecoverymaximum, matrixspikepercentrecoveryminimum, matrixtypeoverride, maximumaverageresponsefactorrsd, maximumblankconcentration, maximumblankresponse, maximumccresponsefactordeviation, maximumnumberofhits, maximumpercentresidual, methoddetectionlimit, minimumassignednoise, minimumaverageresponsefactor, minimumccrelativeresponsefactor, minimumpercentpurity, minimumsignaltonoiseratio, molecularformula, multiplier, mz, mzadditional, mzextractionwindowfilterleft, mzextractionwindowfilterright, mzextractionwindowunits, mzscanrangehigh, mzscanrangelow, neutrallossgain, noisealgorithmtype, noiseofrawsignal, noisereference, noiseregions, noisestandarddeviationmultiplier, nonreferencewindowoverride, numberofverifiedionslimit, outlieralternativepeak, outlieraverageresponsefactor, outlieraverageresponsefactorrsd, outlierblankresponseoutsidelimit, outlierccaverageresponsefactor, outlierccrelativeresponsefactor, outliercurvefitr, outliercustomcalculation, outliermethoddetectionlimit, outlierminimumcurvefitr2, outlierpeaknotfound, outlierrelativeresponsefactor, outlierrelativestandarderror, outlierresponsecheckbelowlimit, outlierresponsefactor, peakfilterthreshold, peakfilterthresholdvalue, peakselectioncriterion, platescalculationtype, plateslimit, primaryhitpeakid, qclcsmaximumrecoverya, qclcsmaximumrecoveryb, qclcsminimumrecoverya, qclcsminimumrecoveryb, qcmaximumdeviation, qcmaximumpercentrsd, qualifierratiomethod, quantitatebyheight, quantitationmessage, qvalueminimum, referencemspathname, referencewindowoverride, relativeistdmultiplier, relativeresponsefactormaximumpercentdeviation, relativeretentiontimemaximumpercentdeviation, relativestandarderror, relativestandarderrormaximum, resolutioncalculationtype, resolutionlimit, responsecheckminimum, responsefactormaximumpercentdeviation, retentionindex, retentiontime, retentiontimedeltaunits, retentiontimewindow, retentiontimewindowcc, retentiontimewindowunits, rightretentiontimedelta, rxunlabeledisotopicdilution, rylabeledisotopicdilution, sampleamountlimithigh, sampleamountlimitlow, samplemaximumpercentrsd, scantype, selectedmz, signalinstance, signalname, signalretentiontimeoffset, signaltonoisemultiplier, signaltype, smoothing, smoothingfunctionwidth, smoothinggaussianwidth, species, spectrumbaselinethreshold, spectrumextractionoverride, spectrumextractionoverridecollisionenergy, spectrumscaninclusion, spectrumpeakheightpercentthreshold, spectrumpercentsaturationthreshold, spectrumquantifierqualifieronly, sublist, surrogateconcentration, surrogateconcentrationlimithigh, surrogateconcentrationlimitlow, surrogatepercentrecoverymaximum, surrogatepercentrecoveryminimum, symmetrycalculationtype, symmetrylimithigh, symmetrylimitlow, targetcompoundidstatus, thresholdnumberofpeaks, timereferenceflag, timesegment, transition, triggeredtransitions, uncertaintyrelativeorabsolute, userannotation, usercustomcalculation, usercustomcalculationlimithigh, usercustomcalculationlimitlow, userdefined, userdefined1, userdefined2, userdefined3, userdefined4, userdefined5, userdefined6, userdefined7, userdefined8, userdefined9, userdefinedtargetcompoundid, verifiediontype, wavelengthextractionrangehigh, wavelengthextractionrangelow, wavelengthreferencerangehigh, wavelengthreferencerangelow, "ID")
select 
	assetid, batchid, sampleid, compoundid, accuracylimitmultiplierloq, accuracymaximumpercentdeviation, agilentid, alternativepeakcriteria, alternativepeakid, areacorrectionfactor, areacorrectionselectedmz, areacorrectionmz, averagerelativeretentiontime, averageresponsefactor, averageresponsefactorrsd, blankresponseoffset, calibrationrangefilter, calibrationreferencecompoundid, capacityfactorlimit, casnumber, ccistdresponseratiolimithigh, ccistdresponseratiolimitlow, ccresponseratiolimithigh, ccresponseratiolimitlow, cellacceleratorvoltage, coelutionscorelimit, collisionenergy, collisionenergydelta, columnvoidtime, compoundapproved, compoundgroup, compoundmath, compoundname, compoundtype, concentrationunits, curvefit, curvefitformula, curvefitlimithigh, curvefitlimitlow, curvefitminimumr, curvefitminimumr2, curvefitorigin, curvefitr, curvefitr2, curvefitstatus, curvefitweight, dilutionhighestconcentration, dilutionpattern, dynamictargetcompoundid, dynamictargetrank, expectedconcentration, fragmentorvoltage, fragmentorvoltagedelta, fullwidthhalfmaximumlimithigh, fullwidthhalfmaximumlimitlow, graphicpeakchromatogram, graphicpeakqualifiers, graphicpeakspectrum, graphictargetcompoundcalibration, integrationparameters, integrationparametersmodified, integrator, ionpolarity, ionsource, istdcompoundid, istdconcentration, istdflag, istdresponselimithigh, istdresponselimitlow, istdresponsemaximumpercentdeviation, istdresponseminimumpercentdeviation, keggid, leftretentiontimedelta, librarymatchscore, librarymatchscoreminimum, libraryretentionindex, libraryretentiontime, limitofdetection, limitofquantitation, linearresponserangemax, linearresponserangemin, massaccuracylimit, massmatchscoreminimum, matrixaconcentrationlimithigh, matrixaconcentrationlimitlow, matrixbconcentrationlimithigh, matrixbconcentrationlimitlow, matrixspikebconcentration, matrixspikebpercentrecoverymaximum, matrixspikebpercentrecoveryminimum, matrixspikeconcentration, matrixspikemaximumpercentdeviation, matrixspikebmaximumpercentdeviation, matrixspikepercentrecoverymaximum, matrixspikepercentrecoveryminimum, matrixtypeoverride, maximumaverageresponsefactorrsd, maximumblankconcentration, maximumblankresponse, maximumccresponsefactordeviation, maximumnumberofhits, maximumpercentresidual, methoddetectionlimit, minimumassignednoise, minimumaverageresponsefactor, minimumccrelativeresponsefactor, minimumpercentpurity, minimumsignaltonoiseratio, molecularformula, multiplier, mz, mzadditional, mzextractionwindowfilterleft, mzextractionwindowfilterright, mzextractionwindowunits, mzscanrangehigh, mzscanrangelow, neutrallossgain, noisealgorithmtype, noiseofrawsignal, noisereference, noiseregions, noisestandarddeviationmultiplier, nonreferencewindowoverride, numberofverifiedionslimit, outlieralternativepeak, outlieraverageresponsefactor, outlieraverageresponsefactorrsd, outlierblankresponseoutsidelimit, outlierccaverageresponsefactor, outlierccrelativeresponsefactor, outliercurvefitr, outliercustomcalculation, outliermethoddetectionlimit, outlierminimumcurvefitr2, outlierpeaknotfound, outlierrelativeresponsefactor, outlierrelativestandarderror, outlierresponsecheckbelowlimit, outlierresponsefactor, peakfilterthreshold, peakfilterthresholdvalue, peakselectioncriterion, platescalculationtype, plateslimit, primaryhitpeakid, qclcsmaximumrecoverya, qclcsmaximumrecoveryb, qclcsminimumrecoverya, qclcsminimumrecoveryb, qcmaximumdeviation, qcmaximumpercentrsd, qualifierratiomethod, quantitatebyheight, quantitationmessage, qvalueminimum, referencemspathname, referencewindowoverride, relativeistdmultiplier, relativeresponsefactormaximumpercentdeviation, relativeretentiontimemaximumpercentdeviation, relativestandarderror, relativestandarderrormaximum, resolutioncalculationtype, resolutionlimit, responsecheckminimum, responsefactormaximumpercentdeviation, retentionindex, retentiontime, retentiontimedeltaunits, retentiontimewindow, retentiontimewindowcc, retentiontimewindowunits, rightretentiontimedelta, rxunlabeledisotopicdilution, rylabeledisotopicdilution, sampleamountlimithigh, sampleamountlimitlow, samplemaximumpercentrsd, scantype, selectedmz, signalinstance, signalname, signalretentiontimeoffset, signaltonoisemultiplier, signaltype, smoothing, smoothingfunctionwidth, smoothinggaussianwidth, species, spectrumbaselinethreshold, spectrumextractionoverride, spectrumextractionoverridecollisionenergy, spectrumscaninclusion, spectrumpeakheightpercentthreshold, spectrumpercentsaturationthreshold, spectrumquantifierqualifieronly, sublist, surrogateconcentration, surrogateconcentrationlimithigh, surrogateconcentrationlimitlow, surrogatepercentrecoverymaximum, surrogatepercentrecoveryminimum, symmetrycalculationtype, symmetrylimithigh, symmetrylimitlow, targetcompoundidstatus, thresholdnumberofpeaks, timereferenceflag, timesegment, transition, triggeredtransitions, uncertaintyrelativeorabsolute, userannotation, usercustomcalculation, usercustomcalculationlimithigh, usercustomcalculationlimitlow, userdefined, userdefined1, userdefined2, userdefined3, userdefined4, userdefined5, userdefined6, userdefined7, userdefined8, userdefined9, userdefinedtargetcompoundid, verifiediontype, wavelengthextractionrangehigh, wavelengthextractionrangelow, wavelengthreferencerangehigh, wavelengthreferencerangelow, "ID"
from test_db.target_compounds;""" 

        cursor.execute(sql_query)
        # Commit the transaction and close the connection
        conn.commit()
        print("Data Ingested Into Production DB For Target CompoundsData")
       

    except Exception as e:
        # Rollback in case of any error
        conn.rollback()
        print("Error executing the Target compounds query:", e)
        with open("error_log.txt", "a") as file:
            file.write(f"Error executing the Target compounds query: {e}\n")

    finally:
        # Close the cursor and connection
        cursor.close()
        

