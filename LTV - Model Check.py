
# Load Packages
import re
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import multiprocessing


os.chdir("C:/Program Files/Python37/Lib/site-packages")
import dask.dataframe as dd
#import lifelines
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import pyodbc


from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
import statsmodels.formula.api as smf

import urllib
import sqlalchemy

# Environments Settings
NumberOfCores = round(multiprocessing.cpu_count() / 8)+1

# Setting Working Environment
# os.chdir("E:\Customer Care\Python Scripts")
os.chdir("E:/LTV - New Script")
os.getcwd()

# Panda Options
pd.options.display.precision = 33
pd.options.display.max_rows = 40
pd.options.display.max_columns = 20
pd.options.display.float_format = '{:.2f}'.format


## Ranks Dictionary -------------------------------------------------------
def Ranks_Dictionary(temp_data, ranks_num):
    quantile_seq = np.linspace(1 / ranks_num, 1, ranks_num)
    overall_quantile = list(map(lambda x: round(np.quantile(temp_data, x), 6), quantile_seq))
    overall_quantile = pd.concat([pd.DataFrame(quantile_seq), pd.DataFrame(overall_quantile)], axis=1)
    overall_quantile.columns = ['quantile', 'value']
    overall_quantile['lag_value'] = overall_quantile['value'].shift(1)
    overall_quantile.loc[:, 'lag_value'] = overall_quantile['lag_value'].fillna(float('-inf'))
    overall_quantile.loc[:, 'value'][len(overall_quantile['value']) - 1] = float('inf')
    overall_quantile['rank'] = list(range(1, len(overall_quantile['value']) + 1))
    overall_quantile = overall_quantile.loc[overall_quantile['value']!= overall_quantile['lag_value'], :]
    return overall_quantile


## jitter -------------------------------------------------------
def RJitter(x,factor):
    z = max(x)-min(x)
    amount = factor * (z/50)
    x = x + np.random.uniform(-amount, amount, len(x))
    return(x)

## Numeric YMC -------------------------------------------------------
def NumericYMC(Variable, Y, NumberOfGroups):

    # Create dictionary for Variable
    #Dictionary = Ranks_Dictionary(Variable + np.random.uniform(0, 0.00001, len(Variable)), ranks_num=NumberOfGroups)
    Dictionary = Ranks_Dictionary(RJitter(Variable,0.00001), ranks_num=NumberOfGroups)
    Dictionary.index = pd.IntervalIndex.from_arrays(Dictionary['lag_value'],
                                                    Dictionary['value'],
                                                    closed='left')
    # Convert Each value in variable to rank
    Variable = pd.DataFrame({'Variable': Variable, 'Y': Y})
    V = Variable['Variable']
    Variable['rank'] = Dictionary.loc[V]['rank'].reset_index(drop=True)

    # Create The aggregation for each rank
    def aggregations(x):
        Mean_YMC = np.mean(x)
        Median_YMC = np.median(x)
        Sd_YMC = np.std(x, ddof=1)
        Quantile99_YMC = np.nanquantile(x, 0.99)
        NumberOfObservation = len(x)
        DataReturned = pd.DataFrame({'Mean_YMC': [Mean_YMC],
                                     'Median_YMC': [Median_YMC],
                                     'Sd_YMC': [Sd_YMC],
                                     'Quantile99_YMC': [Quantile99_YMC],
                                     'NumberOfObservation': [NumberOfObservation]})

        return DataReturned

    AggregationTable = Variable.groupby('rank')['Y'].apply(aggregations).reset_index()
    AggregationTable = AggregationTable.drop(columns=['level_1'])

    # Merge to The Dictionary
    Dictionary = Dictionary.merge(AggregationTable, how='left', on=['rank'])

    Dictionary.loc[:, 'Mean_YMC'] = Dictionary['Mean_YMC'].fillna(np.mean(Y.dropna()))
    Dictionary.loc[:, 'Median_YMC'] = Dictionary['Median_YMC'].fillna(np.median(Y.dropna()))
    Dictionary.loc[:, 'Sd_YMC'] = Dictionary['Sd_YMC'].fillna(np.std(Y.dropna(), ddof=1))
    Dictionary.loc[:, 'Quantile99_YMC'] = Dictionary['Quantile99_YMC'].fillna(np.nanquantile(Y.dropna(), 0.99))
    Dictionary.loc[:, 'NumberOfObservation'] = Dictionary['NumberOfObservation'].fillna(0)

    return Dictionary



# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------- Read The Data ----------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
##Read ------------------------------------------------------------------------------------------------------------------------

sql = "select Account_Holder_ID,Season,PrimaryVertical,Seniority,Country_Name from [Dev_Predict_DB].[LTV].[DataPanel_Model_TrainTest] where Is_Train=1"
# conn = pyodbc.connect('DRIVER={SQL Server};SERVER=HRZ-DWHSS-PRD;DATABASE=Predict_DB;Trusted_Connection=yes')
conn = pyodbc.connect('SERVER=HRZ-DWHSS-PRD;DRIVER={SQL Server};DATABASE=Dev_Predict_DB;UID=bipredict;PWD=CokeZero!')
DataToSplit = pd.DataFrame(pd.read_sql(sql, conn))
conn.close()
len(DataToSplit)# 3759598
del(sql,conn)

##Segment Construction---------------------------------------------------------------------------------------------------------
DataToSplit['PrimaryVertical'] = DataToSplit['PrimaryVertical'].astype(str)
DataToSplit['PrimaryVertical'] = DataToSplit['PrimaryVertical'].apply(lambda x: re.sub("[^a-zA-Z0-9 -]", "", x))
DataToSplit['SeniorityFactor'] = np.where(DataToSplit['Seniority'] <= 30, "Seniority_30",
                                             np.where(DataToSplit['Seniority'] <= 90, "Seniority_30-90",
                                                      np.where(DataToSplit['Seniority'] <= 180, "Seniority_90-180","Seniority_180+")))
##Segment RegistrationVertical+Country+SeniorityFactor
DataToSplit['Segment'] = [*map(' '.join, zip(DataToSplit['PrimaryVertical'],
                                             DataToSplit['Country_Name'],
                                             DataToSplit['SeniorityFactor']))]

##Segment RegistrationVertical+SeniorityFactor
FrequencyTable = pd.DataFrame(DataToSplit['Segment'].value_counts())
DataToSplit['Segment'] = np.where(DataToSplit['Segment'].isin(FrequencyTable.loc[FrequencyTable.Segment <= 2000].index),
                                     [*map(' '.join, zip(DataToSplit['PrimaryVertical'],
                                                         DataToSplit['SeniorityFactor']))],
                                     DataToSplit['Segment'])
##Segment RegistrationVertical
FrequencyTable = pd.DataFrame(DataToSplit['Segment'].value_counts())
DataToSplit['Segment'] = np.where(DataToSplit['Segment'].isin(FrequencyTable.loc[FrequencyTable.Segment <= 2000].index),
                                  DataToSplit['PrimaryVertical'],
                                  DataToSplit['Segment'])
##Other Segment
FrequencyTable = pd.DataFrame(DataToSplit['Segment'].value_counts())
DataToSplit['Segment'] = np.where(DataToSplit['Segment'].isin(FrequencyTable.loc[FrequencyTable.Segment <= 2000].index),
                                     'Other',
                                     DataToSplit['Segment'])


## ---------------------------------------------------------------------------------------------------------------------------------
## Files Delete in Data Pickle/DataModelChecking -----------------------------------------------------------------------------------
## ---------------------------------------------------------------------------------------------------------------------------------
for filename in os.listdir('Data Pickle/DataModelChecking/'):
    os.remove(os.getcwd() + '/Data Pickle/DataModelChecking/' + filename)

## Save The New Data Segments for predictions --------------------------------------------------------------------------------------
Start = datetime.now()
# conn = pyodbc.connect('DRIVER={SQL Server};SERVER=HRZ-DWHSS-PRD;DATABASE=Predict_DB;Trusted_Connection=yes')
conn = pyodbc.connect('SERVER=HRZ-DWHSS-PRD;DRIVER={SQL Server};DATABASE=Dev_Predict_DB;UID=bipredict;PWD=CokeZero!')
cursor = conn.cursor()
for Segment in DataToSplit['Segment'].unique():
    # Segment = 'VIP - Non China Seniority_30-90'
    SegmentData = DataToSplit.loc[DataToSplit['Segment']==Segment,:]
        
    ## SQL Query to create a temporarytable
    sql = """
            DROP TABLE IF EXISTS #SegmentTable;
            CREATE TABLE #SegmentTable
            (
                Account_Holder_ID INT
                    DEFAULT -1,
                Season INT
                    DEFAULT-1
            );
            
            INSERT INTO #SegmentTable (Account_Holder_ID,Season)
            select -1,-1
            SQLString
         """

    SQLString = 'UNION ALL SELECT ' + SegmentData['Account_Holder_ID'].astype(str)+',' + SegmentData['Season'].astype(str)
    SQLString = ' \n '.join(SQLString)
    sql = sql.replace('SQLString', SQLString,1)   

    ## Drop the table and close the connection
    cursor.execute(sql)
    conn.commit()

    ## Read the specific data joined with the temporary table
    DataModel = pd.DataFrame(pd.read_sql("""select DPMTT.* 
                                            from [Dev_Predict_DB].[LTV].[DataPanel_Model_TrainTest] as DPMTT 
                                            inner join #SegmentTable as SegmentTable
                                            on DPMTT.Account_Holder_ID = SegmentTable.Account_Holder_ID AND
                                            DPMTT.Season = SegmentTable.Season 
                                            where Is_Train=1 """, conn))
    #len(DataModel)  # 5230765
    
    ## Write the table
    Path = 'Data Pickle/DataModelChecking/LTV - DataModel - Segment.pckl'
    Path = Path.replace('Segment', Segment, 1)
    f = open(Path, 'wb')
    pickle.dump(DataModel, f)
    f.close()
    del f,Path
    
    if len(SegmentData)!=len(DataModel):
        print(Segment + ": "  + 'False')

## Closing The connection
conn.close()

## Calculating the time duration of segment saving
End = datetime.now()
print(End-Start)##1:51:13.782012

## Remove not used Variables
del sql,conn,DataToSplit,cursor,DataModel,FrequencyTable,Segment,SegmentData,SQLString


## -------------------------------------------------------------------
## Model Function ----------------------------------------------------
## -------------------------------------------------------------------
def RunModelParallel(Data):
    #print(Data.iloc[0,0])
    Segment = Data.iloc[0,0]
    
    os.chdir("E:/LTV - New Script")


    ## Load The pickle data ----------------------------------------------------
    Path = 'Data Pickle/DataModelChecking/LTV - DataModel - Segment.pckl'
    # Segment = 'VIP - Greater China China Seniority_30'
    # Segment = 'MLM United States of America Seniority_180+'
    Path = Path.replace('Segment', Segment, 1)
    f = open(Path, 'rb')
    obj = pickle.load(f)
    f.close()
    DataModel=obj
    del (obj,f,Path)

    # DataModel = DataModel.head(5000)
    
    ## jitter the Y=0 for that the model will converge
    DataModel['Y'] = np.where(DataModel['Y']<0,0,DataModel['Y'])
    DataModel['Y'] = np.where(DataModel['Y']==0,DataModel['Y'] + np.random.uniform(0, 0.01, np.array(DataModel['Y'].shape)),DataModel['Y'])
    DataModel['Y'] = np.where(DataModel['Y']<=0.1,DataModel['Y'] + np.random.uniform(0, 0.001, np.array(DataModel['Y'].shape)),DataModel['Y'])
    DataModel['Y'] = np.where(DataModel['Y']<=0.1,DataModel['Y'] + np.random.uniform(0, 0.0001, np.array(DataModel['Y'].shape)),DataModel['Y'])
    DataModel['Y'] = np.where(DataModel['Y']<=0.1,DataModel['Y'] + np.random.uniform(0, 0.00001, np.array(DataModel['Y'].shape)),DataModel['Y'])
    DataModel['Y'] = np.where(DataModel['Y']<=0.1,DataModel['Y'] + np.random.uniform(0, 0.000001, np.array(DataModel['Y'].shape)),DataModel['Y'])
    DataModel['Y'] = np.where(DataModel['Y']<=0.1,DataModel['Y'] + np.random.uniform(0, 0.0000001, np.array(DataModel['Y'].shape)),DataModel['Y'])



    ###YMC Trick for categorical Data-----------------------------------------------------------------------------------------------------------------------------
    FactorVariables = ["Account_Holder_Language", "MainDomain", "Country_Name",
                       "Nationality_Country", "City", "RegistrationCompletionMonth", "RegistrationCompletionWeekday",
                       "FTL_Month", "FTL_Weekday", "UTM_Source", "UTM_Medium",
                       "UTM_Campaign", "UTM", "First_TM", "First_TM_Flow", "Account_Holder_Reg_Program",
                       "RegistrationVertical", "IsCoreVertical",
                       "ValueSegment", "Tier",
    
                       "FirstBrowser", "FirstDevice", "FirstPlatform", "PrimaryBrowser", "NumberOfBrowsers",
                       "PrimaryDevice", "NumberOfDevices", "PrimaryPlatform", "NumberOfPlatforms",
    
                       "PrimaryInquiry", "NumberOfDistinctInquiry",
    
                       "PrimaryVertical", "PrimaryVolumeType", "PrimaryVolumeTypeCategory",
                       "PrimaryVolumeTypeSubCategory", "PrimaryLoader",
    
                       "PrimaryRevenueLoaderName", "PrimaryRevenueVerticalName", "PrimaryRevenueItem", "PrimaryRevenueType",
                       "PrimaryRevenureSubCategory", "PrimaryRevenueCategory", "PrimaryRevenueActivity",
                       "PrimaryUsageType", "PrimaryUsageTypeSubCategory", "PrimaryUsageTypeCategory",
                       "PrimaryUsageCurrency", "PrimaryMerchent", "IsLastLoadAfterLastUsage",
                       "ActivitySegment", "FLC", "RFClassPredict_30", "RFClassPredict_90", "RFClassPredict_180",
                       
                       "NumberOfDistinctUsageEndCountry","NumberOfDistinctExternalBankName",
                       "NumberOfUsageTypeCategory", "NumberOfUsageTypeSubCategory", "NumberOfUsageType",
                       "NumberOfRevenueLoaderName", "NumberOfRevenueItems", "NumberOfRevenueTypes",
                       "NumberOfRevenueSubCategories", "NumberOfRevenueCategories", "NumberOfRevenueActivities",
                       "NumberOfVerticals", "NumberOfDistinctFirstDepartment", "NumberOfLoadersName",
    
                       "NumberOfBearingEntityID_UntilNow", "NumberOfBearingCustomerCategoryID_UntilNow",
                       "NumberOfDistinctTransactionCode_Recently", "NumberOfMerchentCategory",
                       "NumberOfOfCurrencies",
                       
                       "PrimaryOpportunitiesStageDescription",
                       "PrimaryOpportunitiesTypeDescription",
                       "PrimaryOpportunitiesDiligenceStatuses"]
    
    ### creating YMC Dictionaries for Factors ------------------------------------------------------------
    def aggregations(x):
        if (len(x)<=0):
            DataReturned = pd.DataFrame({'YMC_Mean_Variable': [0],
                                         'YMC_Median_Variable': [0],
                                         'YMC_Quantile_Variable': [0],
                                         'YMC_Sd_Variable': [0],
                                         'NumberOfObservation': [0]})
            return DataReturned
        else:
            Mean_YMC = np.mean(x)
            Median_YMC = np.median(x)
            Quantile_YMC = np.nanquantile(x, 0.99)
            Sd_YMC = np.std(x,ddof=1)#ddof=1 means Divided by N-1
            NumberOfObservation = len(x)
            DataReturned = pd.DataFrame({'YMC_Mean_Variable': [Mean_YMC],
                                         'YMC_Median_Variable': [Median_YMC],
                                         'YMC_Quantile_Variable': [Quantile_YMC],
                                         'YMC_Sd_Variable': [Sd_YMC],
                                         'NumberOfObservation': [NumberOfObservation]})
        return DataReturned
    
    
    ### Creating YMC Dictionaries for Factors ------------------------------------------------------------
    YMC_Factor_Dictionary_List = dict()
    
    for VariableToConvert in FactorVariables:
        # VariableToConvert="City"
        # Creating variable to merge it to YMC
        Variable = DataModel.loc[:, ["Account_Holder_ID", "Season", "Y", VariableToConvert]]
        Variable.columns = ["Account_Holder_ID", "Season", "Y", "VariableToConvert"]
        Variable.loc[:, 'VariableToConvert'] = Variable['VariableToConvert'].astype(str)
        Variable.loc[:, 'VariableToConvert'] = Variable['VariableToConvert'].fillna('NULL')
        
        # Group all the Not Frequent Factor to one factor group
        NotFrequentFactorGroup = pd.DataFrame(Variable.groupby('VariableToConvert')['Y'].apply(
            lambda x: 'Rare' if len(x) <= 50 else 'Frequent')).reset_index()
        NotFrequentFactorGroup.columns = ["VariableName", "SmallGroupOrNot"]
        Variable.loc[:, 'VariableToConvert'] = np.where(Variable['VariableToConvert'].isin(
            NotFrequentFactorGroup.loc[NotFrequentFactorGroup.SmallGroupOrNot == 'Frequent'].VariableName),
            Variable['VariableToConvert'], 'Not Frequent Factor')
        
        
        
        Dictionary_Variable_YMC = Variable.groupby('VariableToConvert')['Y'].apply(aggregations).reset_index()
        Dictionary_Variable_YMC = Dictionary_Variable_YMC.drop(columns=['level_1'])
        Dictionary_Variable_YMC.columns = ["Variable","YMC_Mean_Variable","YMC_Median_Variable","YMC_Quantile_Variable","YMC_Sd_Variable","NumberOfObservation"]
        ##Sort the Dictionary
        Dictionary_Variable_YMC = Dictionary_Variable_YMC.sort_values(by='YMC_Mean_Variable', ascending=False)
        
        
        # Creating Dictionaries for all not frequent factors
        try:
            NotFrequentFactor_Dictionary = pd.DataFrame(data={
                'Variable': NotFrequentFactorGroup.loc[NotFrequentFactorGroup.SmallGroupOrNot == 'Rare'].VariableName})
            NotFrequentFactor_Dictionary['YMC_Mean_Variable'] = float(Dictionary_Variable_YMC.loc[Dictionary_Variable_YMC.Variable == 'Not Frequent Factor'].YMC_Mean_Variable)
            NotFrequentFactor_Dictionary['YMC_Median_Variable'] = float(Dictionary_Variable_YMC.loc[Dictionary_Variable_YMC.Variable == 'Not Frequent Factor'].YMC_Median_Variable)
            NotFrequentFactor_Dictionary['YMC_Quantile_Variable'] = float(Dictionary_Variable_YMC.loc[Dictionary_Variable_YMC.Variable == 'Not Frequent Factor'].YMC_Quantile_Variable)
            NotFrequentFactor_Dictionary['YMC_Sd_Variable'] = float(Dictionary_Variable_YMC.loc[Dictionary_Variable_YMC.Variable == 'Not Frequent Factor'].YMC_Sd_Variable)
            NotFrequentFactor_Dictionary['NumberOfObservation'] = float(Dictionary_Variable_YMC.loc[Dictionary_Variable_YMC.Variable == 'Not Frequent Factor'].NumberOfObservation)
        except:
            1 + 1
        
        if NotFrequentFactor_Dictionary is None:
            NotFrequentFactor_Dictionary = pd.DataFrame(data={'Variable': ['Not Frequent Factor']})
            NotFrequentFactor_Dictionary['YMC_Mean_Variable'] = float(0)
            NotFrequentFactor_Dictionary['YMC_Median_Variable'] = float(0)
            NotFrequentFactor_Dictionary['YMC_Quantile_Variable'] = float(0)
            NotFrequentFactor_Dictionary['YMC_Sd_Variable'] = float(0)
            NotFrequentFactor_Dictionary['NumberOfObservation'] = float(0)
        
        
        # Binding the 2 dictionaries
        Dictionary_Variable_YMC = Dictionary_Variable_YMC.append(NotFrequentFactor_Dictionary)
        
        # Inserting the dictionary into a list
        YMC_Factor_Dictionary_List[VariableToConvert] = Dictionary_Variable_YMC
        
    ##Delete all temporary Variables
    del NotFrequentFactor_Dictionary
    del NotFrequentFactorGroup
    del Dictionary_Variable_YMC
    del VariableToConvert
    del Variable
       
    ##We will insert the Total YMC Measures for all the new predictions that are not in the Data Panel Model
    TotalYMean=np.mean(DataModel['Y'])
    TotalYMedian=np.median(DataModel['Y'])
    TotalYQuantile=np.quantile(DataModel['Y'],0.99)
    TotalYSd=np.std(DataModel['Y'],ddof=1)   
    
    ## Inserting the YMC Values from the dictionaries ------------------------------------------------------------
    for VariableName in YMC_Factor_Dictionary_List:
        # VariableName="FirstDevice"
        DataModel.loc[:, VariableName] = DataModel[VariableName].astype(str)
        
        YMC_Dictionary = YMC_Factor_Dictionary_List[VariableName]
        YMC_Dictionary = YMC_Dictionary.drop(columns=['NumberOfObservation'])
        YMC_Dictionary.columns = [VariableName, 
                                  VariableName+"_Mean_YMC",
                                  VariableName+"_Median_YMC",
                                  VariableName+"_Quantile_YMC",
                                  VariableName+"_Sd_YMC"]
    
        DataModel = DataModel.join(YMC_Dictionary.set_index([VariableName]), how='left', on=[VariableName])
        DataModel.loc[:, VariableName+"_Mean_YMC"] = DataModel[VariableName+"_Mean_YMC"].fillna(TotalYMean)
        DataModel.loc[:, VariableName+"_Median_YMC"] = DataModel[VariableName+"_Median_YMC"].fillna(TotalYMedian)
        DataModel.loc[:, VariableName+"_Quantile_YMC"] = DataModel[VariableName+"_Quantile_YMC"].fillna(TotalYQuantile)
        DataModel.loc[:, VariableName+"_Sd_YMC"] = DataModel[VariableName+"_Sd_YMC"].fillna(TotalYSd)
    
    
    del YMC_Dictionary
    del VariableName 
        
    # Numeric YMC -------------------------------------------------------------------------------------------------------------
    NumericVariables = ["Hours_From_RegComp_to_FTL", "Hours_From_RegStarted_to_FTL", "Minutes_RegStarted_to_Complete", "Age",
                        "Seniority", "TimeBetweenRegistrationCompleteToCutPoint", "NumberOfEnteringToAccount",
                        "LastLogAndFirstLogDifference", "LastLogCutPointDifference", "NumberOfBrowsers",
                        "NumberOfDevices", "NumberOfPlatforms", "SumMailSent", "SumMailOpened", "SDMailOpened",
                        "SumMailClicked", "LastMailDateCutPointDifference", "DaysFromLastIncident",
                        "NumberOfIncidents", "NumberOfDistinctInquiry", "NumberOfDistinctFirstSubjectDescription",
                        "NumberOfDistinctFirstSubSubjectDescription", "NumberOfDistinctFirstDepartment",
                        "NumberOfMostFrequentInquiry", "HoursBetweenLastLoadToCutPoint", "HoursBetweenFirstLoadToCutPoint_InVolumeDays",
                        "SumVolume_InVolumeDays", "SdVolume_InVolumeDays", "GeometricMeanVolume_InVolumeDays", "CV_InVolumeDays",
                        "SumOfCrossCountryVolume_InVolumeDays", "MaxVolume_InVolumeDays", "NumberOfLoads_InVolumeDays",
                        "DistinctMonthsActivity_InVolumeDays", "BillingServices_InVolumeDays", "MAP_InVolumeDays",
                        "MassPayout_InVolumeDays", "PaymentService_InVolumeDays", "Private_InVolumeDays",
                        "Unknown_InVolumeDays", "SumVolumeMap_InVolumeDays", "NumberOfVolumeMap_InVolumeDays",
                        "SumVolumePaymentRequest_InVolumeDays", "NumberOfVolumePaymentRequest_InVolumeDays", "NumberOfVerticals",
                        "NumberOfLoadersName", "SumVolume_UntilNow", "SdVolume_UntilNow", "GeometricMeanVolume_UntilNow",
                        "CV_UntilNow", "MaxVolume_UntilNow", "SumOfCrossCountryVolume_UntilNow", "NumberOfLoads_UntilNow",
                        "DistinctMonthsActivity_UntilNow", "SumVolumeMap_UntilNow",
                        "NumberOfVolumeMap_UntilNow", "SumVolumePaymentRequest_UntilNow", "NumberOfVolumePaymentRequest_UntilNow",
                        "NumberOfLoadsInFirst24H", "VolumeAmountInFirst24H", "NumberOfLoads_Volume1", "SumVolume_Volume1",
                        "SumVolumeMAP_Volume1", "NumberOfVolumeMAP_Volume1", "SdVolume_Volume1", "MaxVolume_Volume1", "AmountOfPaymentRequest_Volume1",
                        "GeometricMeanVolume_Volume1", "SumOfCrossCountryVolume_Volume1", "NumberOfLoads_Volume2", "SumVolume_Volume2",
                        "SumVolumeMAP_Volume2", "NumberOfVolumeMAP_Volume2", "SdVolume_Volume2", "MaxVolume_Volume2",
                        "AmountOfPaymentRequest_Volume2", "GeometricMeanVolume_Volume2",
                        "SumOfCrossCountryVolume_Volume2", "NumberOfLoads_Volume3", "SumVolume_Volume3",
                        "SumVolumeMAP_Volume3", "NumberOfVolumeMAP_Volume3", "SdVolume_Volume3", "MaxVolume_Volume3",
                        "AmountOfPaymentRequest_Volume3", "GeometricMeanVolume_Volume3", "SumOfCrossCountryVolume_Volume3", "SumVolume_Volume4",
                        "SumVolume_Volume5", "SumVolume_Volume6", "Payee_InternalMovementAmount", "Payee_NumberOfInternalMovementAmount",
                        "SumManageCurrencies", "GeometricMeanManageCurrencies", "NumberOfManageCurrencies",
                        "DaysBetweenFirstRevenueToCutPoint", "DaysFromLastRevenueToCutPoint", "NumberOfDistinctTransactionCode_UntilNow", "NumberOfRevnueTransactions_UntilNow",
                        "PercentOfFX_UntilNow", "NumberOfFX_UntilNow", "PercentOfCrossCountry_UntilNow",
                        "NumberOfCrossCountry_UntilNow", "SumRevenue_UntilNow", "GeometricMeanRevenue_UntilNow", "SdRevenue_UntilNow", "MeanRevenue_UntilNow",
                        "MaxRevenue_UntilNow", "SumRevenueWorkingCapital_UntilNow", "SumRevenueUsage_UntilNow",
                        "SumRevenueOther_UntilNow", "SumRevenueVolume_UntilNow", "SumRevenueInternalMovements_UntilNow", "SumRevenueFX_UntilNow", "SumRevenueCrossCountry_UntilNow",
                        "SumRevenueChargedOffline_UntilNow", "SumRevenueBearingEntity_UntilNow", "GeometricMeanRevenueWorkingCapital_UntilNow",
                        "GeometricMeanRevenueUsage_UntilNow", "GeometricMeanRevenueOther_UntilNow", "GeometricMeanRevenueVolume_UntilNow",
                        "GeometricMeanRevenueInternalMovements_UntilNow", "GeometricMeanFX_UntilNow", "GeometricMeanCrossCountry_UntilNow",
                        "NumberOfBearingEntityID_UntilNow", "NumberOfBearingCustomerCategoryID_UntilNow", "NumberOfDistinctTransactionCode_Recently",
                        "NumberOfRevnueTransactions_Recently", "PercentOfFX_Recently", "NumberOfFX_Recently",
                        "PercentOfCrossCountry_Recently", "NumberOfCrossCountry_Recently", "SumRevenue_Recently",
                        "GeometricMeanRevenue_Recently", "SdRevenue_Recently", "MeanRevenue_Recently", "MaxRevenue_Recently",
                        "SumRevenueWorkingCapital_Recently", "SumRevenueUsage_Recently", "SumRevenueOther_Recently", "SumRevenueVolume_Recently",
                        "SumRevenueInternalMovements_Recently", "SumRevenueFX_Recently", "SumRevenueCrossCountry_Recently",
                        "SumRevenueChargedOffline_Recently", "SumRevenueBearingEntity_Recently",
                        "GeometricMeanRevenueWorkingCapital_Recently", "GeometricMeanRevenueUsage_Recently",
                        "GeometricMeanRevenueOther_Recently", "GeometricMeanRevenueVolume_Recently",
                        "GeometricMeanRevenueInternalMovements_Recently", "GeometricMeanFX_Recently", "GeometricMeanCrossCountry_Recently",
                        "NumberOfBearingEntityID_Recently", "NumberOfBearingCustomerCategoryID_Recently",
                        "NumberOfRevenueLoaderName", "NumberOfRevenueItems", "NumberOfRevenueTypes",
                        "NumberOfRevenueSubCategories", "NumberOfRevenueCategories", "NumberOfRevenueActivities",
                        "SumUsage_UntilTotal", "NumberOfUsage_UntilTotal", "LastLoadLastUsageDiff",

                        "DaysBetweenFirstUsageToCutPoint", "DaysFromLastUsageToCutPoint", "SumIsFX",
                        "SumIsCrossCountry", "SumIsAutoWithdrawal", "NumberOfMerchentCategory",
                        "DiversityOfMerchentCategory", "NumberOfOfCurrencies", "SumCNP",
                        "NumberOfUsage", "SumUsage", "GeometricMeanUsage", "MaxUsage",
                        "SdUsage", "MeanUsage", "SumUsage_FX", "SumUsage_NotInUSD",
                        "SumUsage_CNP", "SumUsage_CrossCountry", "SumUsage_MAP", "SumUsage_ATM_POS",
                        "SumUsage_Withdrawal", "MeanUsage_MAP", "MaxUsage_MAP", "MeanUsage_ATM_POS",
                        "MaxUsage_ATM_POS", "MeanUsage_Withdrawal", "MaxUsage_Withdrawal",
                        "NumberOfStores", "NumberOfStoresRecently", "LastStoreCreationToCutPointDaysDifference",
                        "FirstStoreCreationToCutPointDaysDifference",

                        "LTV", "ChurnFromLTVModel", "FLW",
                        "RFProbsPredict_30", "RFProbsPredict_90", "RFProbsPredict_180",

                        "Payer_InternalMovementAmount", "Payer_NumberOfInternalMovementAmount",
                        
                        "OpportunityProbability", 
                        "OpportunityMonthlyVolumeCommitmentAmountUSD",
                        "OpportunityExpectedAnnualVolumeAmountUSD",
                        "OpportunityExpectedAnnualRevenueAmountUSD",
                        "OpportunityForecastAnnualRevenueAmountUSD"
                        ]
    
    
    # Churn Prediction --------------------------------------------------------
    ##REmoving correlated variables
    TempData = DataModel.loc[:, ["_YMC" in i for i in DataModel.columns] | np.isin(DataModel.columns,NumericVariables)].astype(float)
    TempData = TempData.iloc[:, ~(TempData.apply(lambda x: round(np.var(x.astype(float)),4) == 0,axis=0).values)]
    CorMat = TempData.corr().abs()# Create correlation matrix 
    CorMat.iloc[np.triu(np.ones(CorMat.shape), k=0).astype(np.bool)] = 0
    to_drop = [column for column in CorMat.columns if any(CorMat[column] > 0.9)]
    TempData = TempData.drop(TempData[to_drop], axis=1)# Drop features  
    VariablesToGLMModel = TempData.columns##colnames of variables that don't correlated
    
    ## LogisticRegression
    if np.sum(DataModel['Churn90Days'])<=100:
        DataModel['Churn90Days']=np.random.choice([0,1], size=len(DataModel), replace=True)
        
    ##Normilize Function
    GLMNormalizeFunction = StandardScaler()
    GLMNormalizeFunction = GLMNormalizeFunction.fit(DataModel.loc[:, VariablesToGLMModel])
    NormalizedData = GLMNormalizeFunction.transform(DataModel.loc[:, VariablesToGLMModel])

    ##Logistit Regression
    glmModel = LogisticRegression(max_iter=350)
    glmModel = glmModel.fit(NormalizedData, DataModel['Churn90Days'])
    DataModel['Churn90DaysPredict']=glmModel.predict_proba(NormalizedData)[:,1]
    NumericVariables=[*NumericVariables,"Churn90DaysPredict"]

    # del TempData,CorMat,to_drop,NormalizedData
    ## PCA Model---------------------------------------------------------------------
    # Taking only the numeric columns and reducing the constant column (without variance)
    YMCColumns = DataModel.loc[:, ["_YMC" in Col for Col in DataModel.columns]].columns
    PCAColnames = np.concatenate((NumericVariables, YMCColumns))
    SubPCAData = DataModel.loc[:,PCAColnames].astype(float)
    SubPCAData = SubPCAData.iloc[:, ~(SubPCAData.apply(lambda x: round(np.var(x),4) == 0,axis=0).values)]
    PCAColnames = SubPCAData.columns

    ##Normilize Function
    Normalize = StandardScaler()
    Normalize = Normalize.fit(SubPCAData)
    
    ##PCA Model
    pca = PCA()
    pca = pca.fit(Normalize.transform(SubPCAData))
    
    ##We need to change 97% to 95%
    ColnamesInPca = np.array(list(map(lambda x: round(x, 4), 100 * pca.explained_variance_ratio_.cumsum())))<= 80 # percent explained variance
    
    ##Predict The PCA  Data
    PcaData = pca.transform(Normalize.transform(SubPCAData))
    PcaData = pd.concat([DataModel['Y'].reset_index(drop=True), pd.DataFrame(PcaData[:, ColnamesInPca]).reset_index(drop=True)], axis=1)
    PcaData.columns = list('Y') + list(map(lambda x: 'PC_' + x, np.array(range(1, sum(ColnamesInPca) + 1)).astype(str)))

    ## Adding pca to DataModel -------------------------------------------------
    DataModel = pd.concat([DataModel.reset_index(drop=True), PcaData.drop(['Y'], axis=1).reset_index(drop=True)], axis=1)

    del YMCColumns
    # Data Manipulation --------------------------------------------------------
    ## Create a YMC dictionary's
    YMC_Dictionary_Numeric_List = dict()
    for VariableToConvert in PcaData.drop(['Y'], axis=1).columns:
        Variable = DataModel[VariableToConvert]
        Variable = Variable.fillna(0)
        YMC_Dictionary_Numeric_List[VariableToConvert] = NumericYMC(Variable=Variable, Y=DataModel['Y'],NumberOfGroups=max(7, round(len(DataModel) / 600)))

    ## Insert the YMC to the DataModel
    Numeric_YMC = pd.DataFrame(data={'Account_Holder_ID': DataModel['Account_Holder_ID'], 'Season': DataModel['Season']})
    if len(PcaData.drop(['Y'], axis=1).columns) != 0:
        for VariableToConvert in PcaData.drop(['Y'], axis=1).columns:
            Variable = pd.DataFrame(data={'Account_Holder_ID': DataModel['Account_Holder_ID'], 'Season': DataModel['Season'], VariableToConvert: DataModel[VariableToConvert]})
            Variable.loc[:,VariableToConvert] = Variable[VariableToConvert].fillna(0)

            # Inserting the numeric dictionary into VariableDictionary
            VariableDictionary = YMC_Dictionary_Numeric_List[VariableToConvert]

            # Adding All the YMC
            VariableDictionary.index = pd.IntervalIndex.from_arrays(VariableDictionary['lag_value'],
                                                                    VariableDictionary['value'],
                                                                    closed='left')
            V = Variable[VariableToConvert]
            Variable[['Mean_YMC', 'Median_YMC', 'Sd_YMC', 'Quantile99_YMC']] = VariableDictionary.loc[V][['Mean_YMC','Median_YMC','Sd_YMC','Quantile99_YMC']].reset_index(drop=True)

            # Adding to YMC Table
            YMC = pd.DataFrame(data={'VariableToConvert_Mean': Variable['Mean_YMC'],
                                     'VariableToConvert_Median': Variable['Median_YMC'],
                                     'VariableToConvert_Sd': Variable['Sd_YMC'],
                                     'VariableToConvert_Quantile99': Variable['Quantile99_YMC']})
            
            Numeric_YMC = pd.concat([Numeric_YMC, YMC], axis=1)
            Numeric_YMC.columns = list(map(lambda x: x.replace('VariableToConvert', (VariableToConvert + '_YMC'), 1), Numeric_YMC.columns))


        DataModel = DataModel.join(Numeric_YMC.set_index(['Account_Holder_ID', 'Season']), how='left', on=['Account_Holder_ID', 'Season'])
    else:
        pass
    del V,Variable,VariableDictionary,YMC
    
    ##Remove All NA coefficients And reducing the number of variables in the model -------------------------------------------------------------------
    VariablesToTheModel=DataModel.loc[:, ["_YMC" in Col for Col in DataModel.columns]].columns
    
    TempData = DataModel.loc[:, VariablesToTheModel].astype(float)
    TempData = TempData.iloc[:, ~(TempData.apply(lambda x: round(np.var(x.astype(float)),4) == 0,axis=0).values)]  
    CorMat = TempData.corr().abs()# Create correlation matrix 
    CorMat.iloc[np.triu(np.ones(CorMat.shape), k=0).astype(np.bool)] = 0
    to_drop = [column for column in CorMat.columns if any(CorMat[column] > 0.85)]
    TempData = TempData.drop(TempData[to_drop], axis=1)# Drop features  
    VariablesToTheModel = TempData.columns##colnames of variables that don't correlated
    
    
    ##Linear Regression
    LMForm = "+".join(["Y~1",*VariablesToTheModel])
    LMModel = smf.ols(LMForm, DataModel)
    LMModel = LMModel.fit()
    #DataModel['Predicted'] = LMModel.predict(DataModel)
    
    # LMModel = LinearRegression()#normalize=True
    # LMModel = LMModel.fit(DataModel.loc[:, VariablesToTheModel], DataModel['Y'])
    # Predicted=pd.DataFrame(LMModel.predict(DataModel.loc[:, VariablesToTheModel]))
    # OLSModel = OLS(DataModel['Y'],DataModel.loc[:, VariablesToTheModel]).fit()
    
    #LMModel.summary()
    ReducedModel = pd.DataFrame({'Variable': LMModel.pvalues.index.values,'PValue': LMModel.pvalues.values})
    ReducedModel = ReducedModel.loc[ReducedModel['PValue']<=0.1,:]
    ReducedModel = ReducedModel.loc[~np.isin(ReducedModel['Variable'],'Intercept'),:]
    
    del TempData,CorMat,to_drop
    
    # Formula -------------------------------------------------------------------------------------------------------------------------------
    ## Adding YMC Variables
    if ReducedModel.shape[0]==0:
        VariablesToTheModel = []
    else:
        VariablesToTheModel = ReducedModel[["_YMC" in Name for Name in ReducedModel['Variable']]]['Variable']
    
    ## Adding raw numeric variables
    VariablesToTheModel = [*VariablesToTheModel,*["SumRevenue_Recently","LTV","SumUsage","SumVolume_InVolumeDays","ThisYearLTV"]]
    
    # Adding Transformation
    VariablesToTheModel = [*VariablesToTheModel,*["np.divide(SumRevenue_UntilNow,(np.abs(SumVolume_UntilNow)+1))",
                                                "np.divide(SumUsage,(np.abs(SumRevenue_Recently)+1))",
                                                "np.divide(HoursBetweenLastLoadToCutPoint,(np.abs(Seniority)+1))",
                                                "np.divide((np.abs(LTV)+1),(np.abs(ThisYearLTV)+1))",
                                                "np.divide(ThisYearLTV,(np.abs(Seniority)+1))"]]
    
    ##Updating Linear Regression
    LMForm = "+".join(["Y~1",*VariablesToTheModel])
    LMModel = smf.ols(LMForm, DataModel)
    LMModel = LMModel.fit()
    
    ## Quantile Regression Model --------------------------------------------------------------------------------------
    try:
        form = "+".join(["Y~1+", *VariablesToTheModel])
        Model = smf.quantreg(form, DataModel)
        #Model = smf.quantreg(form, sm.tools.add_constant(DataModel,prepend=True))
        QrModel = Model.fit(q=0.5,max_iter = 2000)
        DataModel['Predict'] = QrModel.predict(DataModel)
    
        #print(QrModel.summary())
        del form, VariablesToTheModel
    except:
        try:
            ColumnsToJitter = [*DataModel.loc[:, ["_YMC" in Col for Col in DataModel.columns]].columns, *["Seniority", "HoursBetweenLastLoadToCutPoint",
                                                                                                          "SumRevenue_UntilNow", "SumVolume_UntilNow", "SumRevenue_Recently", "LTV", "SumUsage", "SumVolume_InVolumeDays", "ThisYearLTV"]]
            JitteredDataModel = pd.DataFrame()
            for ColumnToJitter in ColumnsToJitter:
                JitteredColumn = RJitter(RJitter(RJitter(DataModel.loc[:, ColumnToJitter].astype(float), 0.00001), 0.00001), 0.0001)
                #JitteredDataModel.concat(JitteredColumn,axis=1)
                JitteredDataModel = pd.concat([JitteredDataModel, JitteredColumn], axis=1)
    
            JitteredDataModel = pd.concat([JitteredDataModel, DataModel['Y']], axis=1)
    
            form = "+".join(["Y~1+", *VariablesToTheModel])
            Model = smf.quantreg(form, JitteredDataModel)
            #Model = smf.quantreg(form, sm.tools.add_constant(DataModel,prepend=True))
            QrModel = Model.fit(q=0.5,max_iter = 2000)
            DataModel['Predict'] = QrModel.predict(DataModel)
    
            del ColumnToJitter, JitteredDataModel, ColumnsToJitter
    
        except:
            try:
                ## Coaliniarity Problem
    
                ## Adding YMC Variables
                VariablesToTheModel = ReducedModel[["_YMC" in Name for Name in ReducedModel['Variable']]]['Variable']
    
                ## Adding raw numeric variables
                VariablesToTheModel = [*VariablesToTheModel, *["SumRevenue_Recently",
                                                               "LTV", "SumUsage", "SumVolume_InVolumeDays", "ThisYearLTV"]]
    
                ##Removing variables that are correlated
                TempData = DataModel.loc[:, VariablesToTheModel].astype(float)
                TempData = TempData.iloc[:, ~(TempData.apply(
                    lambda x: round(np.var(x.astype(float)), 4) == 0, axis=0).values)]
                CorMat = TempData.corr().abs()  # Create correlation matrix
                CorMat.iloc[np.triu(np.ones(CorMat.shape), k=0).astype(np.bool)] = 0
                to_drop = [column for column in CorMat.columns if any(CorMat[column] > 0.4)]
                TempData = TempData.drop(TempData[to_drop], axis=1)  # Drop features
                VariablesToTheModel = TempData.columns  # colnames of variables that don't correlated
    
                ##Quantreg without Coaliniatiry
                form = "+".join(["Y~1", *VariablesToTheModel])
                Model = smf.quantreg(form, DataModel)
                QrModel = Model.fit(q=0.5,max_iter = 2000)
                del form, VariablesToTheModel, TempData, to_drop, CorMat,
    
            except:
                ##Quantreg without Coaliniatiry
                form = "+".join(["Y~1"])
                Model = smf.quantreg(form, DataModel)
                QrModel = Model.fit(q=0.5)
                print("There is strong coaliniarity")
                del form
        
    #print(QrModel.summary())

    # Prediction for calibration----------------------------------------------------------
    DataModel['Predict'] = QrModel.predict(DataModel)
    DataModel['Predict'] = np.maximum(1,DataModel['Predict'])
    
    ##LTV Upper Cut Point
    UpperBorder = np.quantile(DataModel['Predict'],0.999)
    UpperLTVValue = np.mean(DataModel.loc[DataModel['Predict']>=UpperBorder,:]['Predict'])
    DataModel['Predict'] = np.where(DataModel['Predict']>=UpperBorder,UpperLTVValue,DataModel['Predict'])

            
    # Calibration ----------------------------------------------------------
    pred = DataModel['Predict'][DataModel['Predict']<np.quantile(DataModel['Predict'],0.9)]
    predTop = DataModel['Predict'][DataModel['Predict']>=np.quantile(DataModel['Predict'],0.9)]
    
    if (len(pred)==0 or len(predTop)==0):
        DataModel['Predict'] = DataModel['Predict']+np.random.uniform(0, 0.001, len(DataModel['Predict']))
        DataModel['Predict'] = DataModel['Predict']+np.random.uniform(0, 0.00001, len(DataModel['Predict']))
        DataModel['Predict'] = DataModel['Predict']+np.random.uniform(0, 0.000001, len(DataModel['Predict']))
        DataModel['Predict'] = DataModel['Predict']+np.random.uniform(0, 0.0000001, len(DataModel['Predict']))
        DataModel['Predict'] = DataModel['Predict']+np.random.uniform(0, 0.0000001, len(DataModel['Predict']))
        DataModel['Predict'] = DataModel['Predict']+np.random.uniform(0, 0.0000001, len(DataModel['Predict']))

        pred = DataModel['Predict'][DataModel['Predict']<np.quantile(DataModel['Predict'],0.9)]
        predTop = DataModel['Predict'][DataModel['Predict']>=np.quantile(DataModel['Predict'],0.9)]        
 
    Calibration1 = Ranks_Dictionary(pred,max(10,round(len(pred)/700)))
    Calibration2 = Ranks_Dictionary(predTop,max(5,round(len(predTop)/400)))
    
    Calibration1['value'][Calibration1['value']==float("inf")] = Calibration2.loc[Calibration2['rank']==1,'value'][0]
    Calibration2 = Calibration2.loc[Calibration2['rank']>1,:]   
    
    Calibration = pd.concat([Calibration1, Calibration2]) 
    Calibration['rank'] = np.linspace(start=1,stop=len(Calibration),num=len(Calibration),dtype=int)
    
    # Creating interval index for fast location 
    Calibration.index = pd.IntervalIndex.from_arrays(Calibration['lag_value'],
                                                    Calibration['value'],
                                                    closed='left')
    
    # Convert Each value in variable to rank
    DataModel['PredictedRank'] = Calibration.loc[DataModel['Predict']]['rank'].reset_index(drop=True)
    
    
    a = DataModel.groupby('PredictedRank').agg(mean = ('Predict', 'mean'),length = ('Predict', 'count')).reset_index()
    b = DataModel.groupby('PredictedRank').agg(mean = ('Y', 'mean'),length = ('Predict', 'count')).reset_index()
    a.columns = ["Ranks","PredictedMean","PredictedLength"]
    b.columns = ["Ranks","YMean","YLength"]

    c = pd.merge(a, b, on='Ranks', how='left')
    c['Diff'] = np.abs(100*(c['PredictedMean']-c['YMean'])/c['YMean'])
    
    CalibrationTable = pd.DataFrame(dict(rank=c['Ranks'],
                                         Calibration=c['YMean']/c['PredictedMean'],
                                         YMean=c['YMean'],
                                         length=c['YLength']))
    Calibration = pd.merge(Calibration,CalibrationTable,on='rank', how='left')
    Calibration.loc[Calibration['Calibration'] == float("inf"),'Calibration'] = 1
    Calibration.loc[np.isnan(Calibration['Calibration']),'Calibration'] = np.median(Calibration['Calibration'].dropna())

    del a,b,c,CalibrationTable,pred,predTop

    ###Second calibration (Smoothing the calibration)
    Calibration['Calibration2'] = Calibration['Calibration'].rolling(window=3, min_periods=1).mean()

    ## Current Time-------------------------------------------------------------------------------------
    now = datetime.now()
    CreateModelDate = now.strftime("%Y-%m-%d %H:%M:%S")
    #print("Current Time =", CreateModelDate)

    # Save The Models -----------------------------------------------------    
    Path = 'Models Before Check - Python Pickle/LTV - New Script - Model Checking - Segment.pckl'
    Path = Path.replace('Segment', Segment, 1)    
    f = open(Path, 'wb')
    pickle.dump([YMC_Factor_Dictionary_List,
                 YMC_Dictionary_Numeric_List,TotalYMean,TotalYMedian,
                 TotalYQuantile,TotalYSd,
                 PCAColnames,pca,Normalize,
                 VariablesToGLMModel,glmModel,GLMNormalizeFunction,
                 UpperBorder,UpperLTVValue,Calibration,LMModel,
                 QrModel,
                 CreateModelDate], f)
    f.close()
    del f,Path,CreateModelDate
    
    df=pd.DataFrame(data={'Account_Holder_ID': DataModel['Account_Holder_ID'],
                          'PrimaryVertical': DataModel['PrimaryVertical']})

    return df
    

# -----------------------------------------------------------------------------------------------------------------------------
# Running the model ------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
## Files Delete in Data Pickle/DataModel--------------------------------------------------------------------------------------------
for filename in os.listdir('Models Before Check - Python Pickle/'):
    os.remove(os.getcwd() + '/Models Before Check - Python Pickle/' + filename)
   
## Start Time
StartModel = datetime.now()


##Taking all the Data Pickles
AllModelSegments = os.listdir('Data Pickle/DataModelChecking/')
AllModelSegments=[*map(lambda x: x.replace('.pckl',"", 1).replace('LTV - DataModel - ',"", 1), AllModelSegments)]
AllModelSegments = pd.DataFrame(data={'Segments': AllModelSegments})



DPCM = dd.from_pandas(AllModelSegments, npartitions=10)
# NumberOfCores = 20
#client = Client(n_workers=1, threads_per_worker=NumberOfCores, memory_limit='40GB')
ModelOut = DPCM.groupby('Segments').apply(RunModelParallel,meta={'Account_Holder_ID':'str','PrimaryVertical':'str'}).compute(num_workers=NumberOfCores)


## End Time
EndModel = datetime.now()
print(StartModel.strftime("%Y-%m-%d %H:%M:%S"))
print(EndModel.strftime("%Y-%m-%d %H:%M:%S"))
print(EndModel - StartModel)##3:20:26.949673


# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------- Read The Data ----------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
##Read ------------------------------------------------------------------------------------------------------------------------

sql = "select Account_Holder_ID,Season,PrimaryVertical,Seniority,Country_Name from [Dev_Predict_DB].[LTV].[DataPanel_Model_TrainTest] where Is_Train=0"
# conn = pyodbc.connect('DRIVER={SQL Server};SERVER=HRZ-DWHSS-PRD;DATABASE=Predict_DB;Trusted_Connection=yes')
conn = pyodbc.connect('SERVER=HRZ-DWHSS-PRD;DRIVER={SQL Server};DATABASE=Dev_Predict_DB;UID=bipredict;PWD=CokeZero!')
DataPredictToSplit = pd.DataFrame(pd.read_sql(sql, conn))
conn.close()
len(DataPredictToSplit)# 3759598
del(sql,conn)

##Segment Construction---------------------------------------------------------------------------------------------------------
DataPredictToSplit['PrimaryVertical'] = DataPredictToSplit['PrimaryVertical'].astype(str)
DataPredictToSplit['PrimaryVertical'] = DataPredictToSplit['PrimaryVertical'].apply(lambda x: re.sub("[^a-zA-Z0-9 -]", "", x))
DataPredictToSplit['SeniorityFactor'] = np.where(DataPredictToSplit['Seniority'] <= 30, "Seniority_30",
                                             np.where(DataPredictToSplit['Seniority'] <= 90, "Seniority_30-90",
                                                      np.where(DataPredictToSplit['Seniority'] <= 180, "Seniority_90-180","Seniority_180+")))
##Segment RegistrationVertical+Country+SeniorityFactor
DataPredictToSplit['Segment'] = [*map(' '.join, zip(DataPredictToSplit['PrimaryVertical'],
                                             DataPredictToSplit['Country_Name'],
                                             DataPredictToSplit['SeniorityFactor']))]

##For small segments we will change it to Other
ModelsInDirectory = np.array(os.listdir("Models Before Check - Python Pickle/"))[np.array(list(map(lambda x: '.pckl' in x, os.listdir('Models Before Check - Python Pickle/'))))]
ModelsInDirectory = np.array(list(map(lambda x: x.replace('LTV - New Script - Model Checking - ', '', 1).replace('.pckl', '', 1), ModelsInDirectory)))



##Segment PrimaryVertical+SeniorityFactor
DataPredictToSplit.loc[:, 'Segment'] = np.where(list(map(lambda x: x not in ModelsInDirectory, DataPredictToSplit['Segment'])),
                                              [*map(' '.join, zip(DataPredictToSplit['PrimaryVertical'],
                                                                  DataPredictToSplit['SeniorityFactor']))],
                                              DataPredictToSplit['Segment'])

##Segment PrimaryVertical
DataPredictToSplit.loc[:, 'Segment'] = np.where(list(map(lambda x: x not in ModelsInDirectory, DataPredictToSplit['Segment'])),
                                              DataPredictToSplit['PrimaryVertical'],
                                              DataPredictToSplit['Segment'])

DataPredictToSplit.loc[:, 'Segment'] = np.where(list(map(lambda x: x not in ModelsInDirectory, DataPredictToSplit['Segment'])),
                                              'Other',
                                              DataPredictToSplit['Segment'])


## ---------------------------------------------------------------------------------------------------------------------------------
## Files Delete in Data Pickle/DataPredictChecking ---------------------------------------------------------------------------------
## ---------------------------------------------------------------------------------------------------------------------------------
for filename in os.listdir('Data Pickle/DataPredictChecking/'):
    os.remove(os.getcwd() + '/Data Pickle/DataPredictChecking/' + filename)

## Save The New Data Segments for predictions --------------------------------------------------------------------------------------
Start = datetime.now()
# conn = pyodbc.connect('DRIVER={SQL Server};SERVER=HRZ-DWHSS-PRD;DATABASE=Predict_DB;Trusted_Connection=yes')
conn = pyodbc.connect('SERVER=HRZ-DWHSS-PRD;DRIVER={SQL Server};DATABASE=Dev_Predict_DB;UID=bipredict;PWD=CokeZero!')
cursor = conn.cursor()
for Segment in DataPredictToSplit['Segment'].unique():
    # Segment = 'VIP - Non China Seniority_30-90'
    SegmentDataPredict = DataPredictToSplit.loc[DataPredictToSplit['Segment']==Segment,:]
        
    ## SQL Query to create a temporarytable
    sql = """
            DROP TABLE IF EXISTS #SegmentTable;
            CREATE TABLE #SegmentTable
            (
                Account_Holder_ID INT
                    DEFAULT -1,
                Season INT
                    DEFAULT-1
            );
            
            INSERT INTO #SegmentTable (Account_Holder_ID,Season)
            select -1,-1
            SQLString
         """

    SQLString = 'UNION ALL SELECT ' + SegmentDataPredict['Account_Holder_ID'].astype(str)+',' + SegmentDataPredict['Season'].astype(str)
    SQLString = ' \n '.join(SQLString)
    sql = sql.replace('SQLString', SQLString,1)   

    ## Drop the table and close the connection
    cursor.execute(sql)
    conn.commit()

    ## Read the specific data joined with the temporary table
    DataPredict = pd.DataFrame(pd.read_sql("""select DPMTT.* 
                                            from [Dev_Predict_DB].[LTV].[DataPanel_Model_TrainTest] as DPMTT 
                                            inner join #SegmentTable as SegmentTable
                                            on DPMTT.Account_Holder_ID = SegmentTable.Account_Holder_ID AND
                                            DPMTT.Season = SegmentTable.Season 
                                            where Is_Train=0 """, conn))
    #len(DataPredict)  # 5230765
    
    ## Write the table
    Path = 'Data Pickle/DataPredictChecking/LTV - DataPredict - Segment.pckl'
    Path = Path.replace('Segment', Segment, 1)
    f = open(Path, 'wb')
    pickle.dump(DataPredict, f)
    f.close()
    del f,Path
    
    if len(SegmentDataPredict)!=len(DataPredict):
        print(Segment + ": "  + 'False')

## Closing The connection
conn.close()

## Calculating the time duration of segment saving
End = datetime.now()
print(End-Start)##1:51:13.782012

## Remove not used Variables
del sql,conn,DataPredictToSplit,cursor,DataPredict,Segment,SegmentDataPredict,SQLString


# -----------------------------------------------------------------------------------------------------------------------------
# Running the Predictions-----------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
##Prediction function
def RunPredictionsParallel(Data):
    #print(Data.iloc[0,0])
    Segment = Data.iloc[0,0]
    
    os.chdir("E:/LTV - New Script")


    ## Load The pickle data ----------------------------------------------------
    Path = 'Data Pickle/DataPredictChecking/LTV - DataPredict - Segment.pckl'
    # Segment = 'Affiliates Bangladesh Seniority_180+'
    Path = Path.replace('Segment', Segment, 1)
    f = open(Path, 'rb')
    obj = pickle.load(f)
    f.close()
    DataPredict=obj
    del obj,f,Path
    
    # Load The Model -----------------------------------------------------    
    Path = 'Models Before Check - Python Pickle/LTV - New Script - Model Checking - Segment.pckl'
    Path = Path.replace('Segment', Segment, 1)
    f = open(Path, 'rb')
    obj = pickle.load(f)
    f.close()
    
    #YMC_Factor_Dictionary_List,YMC_Dictionary_Numeric_List,TotalYMean,TotalYMedian,TotalYQuantile,TotalYSd,AllModelSegmentspca,Normalize,VariablesToGLMModel,glmModel,GLMNormalizeFunction,UpperBorder,UpperLTVValue,Calibration,LMModel,QrModel,CreateModelDate = obj
    #YMC_Factor_Dictionary_List,YMC_Dictionary_Numeric_List,TotalYMean,TotalYMedian,TotalYQuantile,TotalYSd,pca,Normalize,VariablesToGLMModel,glmModel,GLMNormalizeFunction,UpperBorder,UpperLTVValue,Calibration,LMModel,QrModel,CreateModelDate = obj
    [YMC_Factor_Dictionary_List,
     YMC_Dictionary_Numeric_List,TotalYMean,TotalYMedian,
     TotalYQuantile,TotalYSd,
     PCAColnames,pca,Normalize,
     VariablesToGLMModel,glmModel,GLMNormalizeFunction,
     UpperBorder,UpperLTVValue,Calibration,LMModel,
     QrModel,
     CreateModelDate] = obj
    del f,Path,obj

    
    ## Inserting the YMC Values from the dictionaries ------------------------------------------------------------
    for VariableName in YMC_Factor_Dictionary_List:
        # VariableName="FirstDevice"
        DataPredict.loc[:, VariableName] = DataPredict[VariableName].astype(str)
        
        YMC_Dictionary = YMC_Factor_Dictionary_List[VariableName]
        YMC_Dictionary = YMC_Dictionary.drop(columns=['NumberOfObservation'])
        YMC_Dictionary.columns = [VariableName, 
                                  VariableName+"_Mean_YMC",
                                  VariableName+"_Median_YMC",
                                  VariableName+"_Quantile_YMC",
                                  VariableName+"_Sd_YMC"]
    
        DataPredict = DataPredict.join(YMC_Dictionary.set_index([VariableName]), how='left', on=[VariableName])
        DataPredict.loc[:, VariableName+"_Mean_YMC"] = DataPredict[VariableName+"_Mean_YMC"].fillna(TotalYMean)
        DataPredict.loc[:, VariableName+"_Median_YMC"] = DataPredict[VariableName+"_Median_YMC"].fillna(TotalYMedian)
        DataPredict.loc[:, VariableName+"_Quantile_YMC"] = DataPredict[VariableName+"_Quantile_YMC"].fillna(TotalYQuantile)
        DataPredict.loc[:, VariableName+"_Sd_YMC"] = DataPredict[VariableName+"_Sd_YMC"].fillna(TotalYSd)
    
    
    del YMC_Dictionary
    del VariableName 
        
    
    # Churn Prediction --------------------------------------------------------
    DataPredict['Churn90DaysPredict']=glmModel.predict_proba(GLMNormalizeFunction.transform(DataPredict.loc[:, VariablesToGLMModel]))[:,1]

    ## PCA Model---------------------------------------------------------------------
    SubPCADataPredict = DataPredict.loc[:, PCAColnames].astype(float)
    PcaData = pca.transform(Normalize.transform(SubPCADataPredict))
    ColnamesInPca = np.array(list(map(lambda x: round(x, 4), 100 * pca.explained_variance_ratio_.cumsum()))) <= 80
    PcaData = pd.DataFrame(PcaData[:, ColnamesInPca]).reset_index(drop=True)
    PcaData.columns = list(map(lambda x: 'PC_' + x, np.array(range(1, sum(ColnamesInPca) + 1)).astype(str)))

    ## Adding pca to DataPredict -------------------------------------------------
    DataPredict = pd.concat([DataPredict.reset_index(drop=True), PcaData.reset_index(drop=True)], axis=1)

    ## Insert the YMC to the DataPredict --------------------------------------------------------
    Numeric_YMC = pd.DataFrame(data={'Account_Holder_ID': DataPredict['Account_Holder_ID'], 'Season': DataPredict['Season']})
    if len(PcaData.columns) != 0:
        for VariableToConvert in PcaData.columns:
            Variable = pd.DataFrame(data={'Account_Holder_ID': DataPredict['Account_Holder_ID'], 'Season': DataPredict['Season'], VariableToConvert: DataPredict[VariableToConvert]})
            Variable.loc[:,VariableToConvert] = Variable[VariableToConvert].fillna(0)

            # Inserting the numeric dictionary into VariableDictionary
            VariableDictionary = YMC_Dictionary_Numeric_List[VariableToConvert]

            # Adding All the YMC
            VariableDictionary.index = pd.IntervalIndex.from_arrays(VariableDictionary['lag_value'],
                                                                    VariableDictionary['value'],
                                                                    closed='left')
            V = Variable[VariableToConvert]
            Variable[['Mean_YMC', 'Median_YMC', 'Sd_YMC', 'Quantile99_YMC']] = VariableDictionary.loc[V][['Mean_YMC','Median_YMC','Sd_YMC','Quantile99_YMC']].reset_index(drop=True)
            
            # Adding to YMC Table
            YMC = pd.DataFrame(data={'VariableToConvert_Mean': Variable['Mean_YMC'],
                                     'VariableToConvert_Median': Variable['Median_YMC'],
                                     'VariableToConvert_Sd': Variable['Sd_YMC'],
                                     'VariableToConvert_Quantile99': Variable['Quantile99_YMC']})
            
            Numeric_YMC = pd.concat([Numeric_YMC, YMC], axis=1)
            Numeric_YMC.columns = list(map(lambda x: x.replace('VariableToConvert', (VariableToConvert + '_YMC'), 1), Numeric_YMC.columns))

        DataPredict = DataPredict.join(Numeric_YMC.set_index(['Account_Holder_ID', 'Season']), how='left', on=['Account_Holder_ID', 'Season'])
    else:
        pass
    del Numeric_YMC,YMC,Variable,V,VariableDictionary
    
    ##Remove All NA coefficients And reducing the number of variables in the model -------------------------------------------------------------------
    DataPredict['LMPredict'] = LMModel.predict(DataPredict)
    DataPredict['LMPredict'] = np.maximum(0,DataPredict['LMPredict'])


    # Prediction for calibration----------------------------------------------------------
    DataPredict['Predict'] = QrModel.predict(DataPredict)
    DataPredict['Predict'] = np.maximum(1,DataPredict['Predict'])
    
    ##LTV Upper Cut Point
    DataPredict['Predict'] = np.where(DataPredict['Predict']>=UpperBorder,UpperLTVValue,DataPredict['Predict'])

            
    # Calibration ----------------------------------------------------------
    Calibration.index = pd.IntervalIndex.from_arrays(Calibration['lag_value'],
                                                    Calibration['value'],
                                                    closed='left')
    DataPredict['Calibration'] = Calibration.loc[DataPredict['Predict']]['Calibration'].reset_index(drop=True)
    DataPredict['Predict_calibrated'] = DataPredict['Calibration'] * DataPredict['Predict']
    

    ## Output -------------------------------------------------------------
    OutPredictions = pd.DataFrame(data={'Account_Holder_ID': DataPredict['Account_Holder_ID'].astype(int),
                             'Model': 'LTV - New Script',
                             'Segment': Segment,
                             'Season': DataPredict['Season'].astype(str),

                             'Seniority': DataPredict['Seniority'],
                             'Country': DataPredict['Country_Name'].astype(str),
                             'PrimaryVertical': DataPredict['PrimaryVertical'].astype(str),
                             
                             'Predict': DataPredict['Predict'],
                             'Predict_calibrated': DataPredict['Predict_calibrated'],
                             'LMPredict': DataPredict['LMPredict'],
                             #'LMPredict2': DataPredict['LMPredict2'],
                             'ThisYearLTV': DataPredict['ThisYearLTV'],
                             
                             'Churn90DaysPredict': DataPredict['Churn90DaysPredict'],
                             'Actual': DataPredict['Y']})
    
    ## Debugging -------------------------------------------------------------
    Debugging = 1
    Path = 'Debugging/LTV - DataModel - Segment.pckl'
    Path = Path.replace('Segment', Segment, 1)
    f = open(Path, 'wb')
    pickle.dump(Debugging, f)
    f.close()
    
    return OutPredictions
    

# -----------------------------------------------------------------------------------------------------------------------------
# Running the Predictions -----------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
## Start Time
StartPredictions = datetime.now()


##Taking all the Data Pickles
os.chdir("E:/LTV - New Script")
os.getcwd()
AllPredictionsSegments = os.listdir('Data Pickle/DataPredictChecking/')
AllPredictionsSegments = [*map(lambda x: x.replace('.pckl',"", 1).replace('LTV - DataPredict - ',"", 1), AllPredictionsSegments)]
AllPredictionsSegments = pd.DataFrame(data={'Segments': AllPredictionsSegments})
#AllPredictionsSegments = AllPredictionsSegments.head(10)

 
DaskSegments = dd.from_pandas(AllPredictionsSegments, npartitions=10)
#client = Client(n_workers=1, threads_per_worker=NumberOfCores, memory_limit='40GB')
Output = DaskSegments.groupby('Segments').apply(RunPredictionsParallel,meta={'Account_Holder_ID': 'int',
                                                                             'Model': 'str',
                                                                             'Segment': 'str',
                                                                             'Season': 'str',

                                                                             'Seniority': 'float',
                                                                             'Country': 'str',
                                                                             'PrimaryVertical':'str',
                                                                                                      
                                                                             'Predict': 'float',
                                                                             'Predict_calibrated': 'float',
                                                                             'LMPredict': 'float',
                                                                             'ThisYearLTV': 'float',
                                                                                                      
                                                                             'Churn90DaysPredict': 'float',
                                                                             'Actual': 'float'
                                                                             }).compute(num_workers=NumberOfCores)


## End Time
EndPredictions = datetime.now()
print(StartPredictions.strftime("%Y-%m-%d %H:%M:%S"))
print(EndPredictions.strftime("%Y-%m-%d %H:%M:%S"))
print(EndPredictions - StartPredictions)##0:23:35.034289

del StartPredictions,EndPredictions


## ---------------------------------------------------------------------------------------------------------------------------------------
##  ---------------------------------------- Check The Model ------------------------------------------------------------------------------
##  ---------------------------------------------------------------------------------------------------------------------------------------
# Check Per Vertical-----------------------------------------------------------------------
CheckingTheModel = pd.DataFrame()
for PrimaryVertical in ('All',*Output['PrimaryVertical'].unique()):
    # print(PrimaryVertical)
    
    ##Sub Data per Vertical
    if PrimaryVertical=='All' :
      SubData = Output.reset_index()
    else:
      SubData = Output.loc[Output['PrimaryVertical'] == PrimaryVertical, :].reset_index()

    ## Skip if the data panel too small
    if len(SubData)<=500 or sum(SubData['Actual'])<=0:
        continue
        
        
    ##Ranks for Predicted and Actual
    Dictionary = Ranks_Dictionary(RJitter(RJitter(SubData['Predict_calibrated'],0.0001),0.00001),100)
    Dictionary.index = pd.IntervalIndex.from_arrays(Dictionary['lag_value'],
                                                    Dictionary['value'],
                                                    closed='left')

    
    #Insert The rank for Actual and Predicted
    SubData['PredictedRank'] = Dictionary.loc[SubData['Predict_calibrated']]['rank'].reset_index(drop=True)

    #Group Mappe
    GroupMappe = SubData.groupby('PredictedRank')[['Predict_calibrated','Actual']].apply(np.mean).reset_index()
    GroupMappe.columns = ["Rank","Mean_Predict_calibrated","Mean_Actual"]
    GroupMappe = np.mean(100*(np.abs(GroupMappe['Mean_Predict_calibrated']-GroupMappe['Mean_Actual'])+1)/(GroupMappe['Mean_Actual']+1))
      
    ##RMSE
    RMSE = np.sqrt(np.mean((SubData['Actual'] - SubData['Predict_calibrated'])**2))
    
    ##MSE without Outliers
    RMSE_WithoutOutliers = np.sqrt(np.mean((SubData['Actual'][SubData['Actual']<=np.quantile(SubData['Actual'],0.9999)]-SubData['Predict_calibrated'][SubData['Actual']<=np.quantile(SubData['Actual'],0.9999)])**2))

    ##MedAE
    MedAE = np.median(abs(SubData['Actual'] - SubData['Predict_calibrated']))
    
    
    ##AUC
    OrderPrediction = np.argsort(-SubData['Predict_calibrated'])##minus for decreasing
    Predicted = SubData['Predict_calibrated'][OrderPrediction]
    Y_predict = SubData['Actual'][OrderPrediction]
    
    CumnsumY_predict = np.cumsum(Y_predict)
    CumnsumPredicted = np.cumsum(Predicted)
    
    ##Full AUC
    AUC_P = sum(np.diff(100*(np.arange(1, len(CumnsumPredicted)+1, 1))/len(CumnsumPredicted))*(100*CumnsumPredicted/sum(Predicted)).tail(-1))/(100*100)##AUC=0.8942668
    AUC_A = sum(np.diff(100*(np.arange(1, len(CumnsumPredicted)+1, 1))/len(CumnsumPredicted))*(100*CumnsumY_predict/sum(Y_predict)).tail(-1))/(100*100)##AUC=0.8942668
    AUC_Optimal = sum(np.diff(100*(np.arange(1, len(SubData.Actual)+1, 1))/len(SubData.Actual))*(100*np.cumsum(SubData.Actual[np.argsort(-SubData.Actual)])/sum(SubData.Actual)).tail(-1))/(100*100)##AUC=0.8942668

    ##60% Difference
    Difference60Percent = np.mean((((SubData['Predict_calibrated']+1)/(SubData['Actual']+1))<=0.6)  | (((SubData['Predict_calibrated']+1)/(SubData['Actual']+1))>=1.6))
    
    ##Personal Mappe
    PersonalMappe = np.mean((abs(SubData['Predict_calibrated']-SubData['Actual'])+1)/(abs(SubData['Actual'])+1))
    
    ##Concat All
    CheckingTheModel = pd.concat([CheckingTheModel,
        pd.DataFrame(data={'PrimaryVertical':PrimaryVertical,
                           'Difference60Percent': Difference60Percent,
                           'PersonalMappe':PersonalMappe,
                           'RMSE':RMSE,
                           'RMSE_WithoutOutliers':RMSE_WithoutOutliers,
                           'MedAE':MedAE,
                           'SumPredicted': round(sum(SubData['Predict_calibrated']), 4),
                           'SumActual': round(sum(SubData['Actual']), 4),
                           'PercentPreciesment': round(100*sum(SubData['Predict_calibrated'])/sum(SubData['Actual']), 4),
                           'GroupMappe': round(GroupMappe, 4),
                           'Correlation': round(np.corrcoef(SubData['Predict_calibrated'], SubData['Actual'])[0][1], 4),

                           'AUC_P': round(AUC_P, 4),
                           'AUC_A': round(AUC_A, 4),
                           'AUC_Optimal': round(AUC_Optimal, 4),
                           
                           'AUC_A_20': 1.0,
                           'AUC_P_20': 1.0,
                           'AUC_A_40': 1.0,
                           'AUC_P_40': 1.0,
                           'AUC_A_60': 1.0,
                           'AUC_P_60': 1.0,
                           'AUC_A_80': 1.0,
                           'AUC_P_80': 1.0,
                           
                           'PercentActualFromTotal': round(100*sum(SubData['Actual'])/sum(Output['Actual']), 4),
                           'Lift10': round(100*np.nanquantile(np.cumsum(SubData['Actual'][np.argsort(-SubData['Predict_calibrated'])]), 0.1)/sum(SubData['Actual']), 4),
                           'Lift20': round(100*np.nanquantile(np.cumsum(SubData['Actual'][np.argsort(-SubData['Predict_calibrated'])]), 0.2)/sum(SubData['Actual']), 4),
                           'Pareto10': round(100*np.nanquantile(np.cumsum(SubData['Actual'][np.argsort(-SubData['Actual'])]), 0.1)/sum(SubData['Actual']), 4),
                           'Pareto20': round(100*np.nanquantile(np.cumsum(SubData['Actual'][np.argsort(-SubData['Actual'])]), 0.2)/sum(SubData['Actual']), 4),
                           'OptimalLift10': round(100*np.nanquantile(np.cumsum(SubData['Actual'][np.argsort(-SubData['Predict_calibrated'])]), 0.1)/np.nanquantile(np.cumsum(SubData['Actual'][np.argsort(-SubData['Actual'])]), 0.1), 4),
                           'OptimalLift20': round(100*np.nanquantile(np.cumsum(SubData['Actual'][np.argsort(-SubData['Predict_calibrated'])]), 0.2)/np.nanquantile(np.cumsum(SubData['Actual'][np.argsort(-SubData['Actual'])]), 0.2), 4)
                           }, index=[0])])
    
CheckingTheModel = CheckingTheModel.iloc[np.argsort(-CheckingTheModel['SumPredicted']),:]
CheckingTheModel.to_clipboard(excel=True, sep=None)

##The Data to Output
CheckingTheModel['CurrentDate'] = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
CheckingTheModel.CurrentDate = CheckingTheModel.CurrentDate.astype('datetime64[ns]')
CheckingTheModel.PrimaryVertical = CheckingTheModel.PrimaryVertical.str.slice(start=0, stop=139).astype(str)


conn = pyodbc.connect('SERVER=HRZ-DWHSS-PRD;DRIVER={SQL Server};DATABASE=Dev_Predict_DB;UID=bipredict;PWD=CokeZero!')
cursor = conn.cursor()


def getEngine(server='', database='', winAuth=False):
    connection_string = "DRIVER={SQL Server};Database=%s;SERVER=%s;" % (database, server)
    
    if not winAuth:
        connection_string += "UID=" + 'bipredict' + ";PWD=" + 'CokeZero!'
    else:
        connection_string += "Trusted_Connection='yes'"
        
    connection_string = urllib.parse.quote_plus(connection_string)
    connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string
    return sqlalchemy.create_engine(connection_string)

## TempOutputLTV
# CheckingTheModel.to_sql(name='TrainTestChecking',
#                      schema='LTV',index = False,
#                      dtype={'PrimaryVertical': sqlalchemy.types.VARCHAR(length=140),
#                             'CurrentDate': sqlalchemy.types.DateTime()},
#                      con=getEngine(server='HRZ-DWHSS-PRD', database='Dev_Predict_DB'), 
#                      if_exists='append')

# -----------------------------------------------------------------------------------------------------------------------------
# Output the Predictions ------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
# df = pd.DataFrame(data={'Account_Holder_ID': Output['Account_Holder_ID'].astype(int),
#                          'PrimaryVertical': Output['PrimaryVertical'],                  
#                          'Segment': Output['Segment'],
#                          'Season': Output['Season'],                  

#                          'LMPredict': Output['LMPredict'],
#                          'Predict': Output['Predict'],
#                          'Predict_calibrated': Output['Predict_calibrated'],
#                          'Actual': Output['Actual']})
# df.to_csv('Output.csv')

