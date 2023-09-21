
# Load Packages
import re
from datetime import datetime
from datetime import timedelta 
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

sql = "select Account_Holder_ID,Season,PrimaryVertical,Seniority,Country_Name from [Dev_Predict_DB].[LTV].[DataPanel_Prediction]"
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
ModelsInDirectory = np.array(os.listdir("Models - Python Pickle/"))[np.array(list(map(lambda x: '.pckl' in x, os.listdir('Models - Python Pickle/'))))]
ModelsInDirectory = np.array(list(map(lambda x: x.replace('LTV - New Script - Model - ', '', 1).replace('.pckl', '', 1), ModelsInDirectory)))



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
## Files Delete in Data Pickle/DataPredict -----------------------------------------------------------------------------------------
## ---------------------------------------------------------------------------------------------------------------------------------
for filename in os.listdir('Data Pickle/DataPredict/'):
    os.remove(os.getcwd() + '/Data Pickle/DataPredict/' + filename)
    

## Save The New Data Segments for predictions ---------------------------------------------------------------------------------------
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

    ## execute
    cursor.execute(sql)
    conn.commit()

    ## Read the specific data joined with the temporary table
    DataPredict = pd.DataFrame(pd.read_sql("""select DPP.* 
                                            from [Dev_Predict_DB].[LTV].[DataPanel_Prediction] as DPP
                                            inner join #SegmentTable as SegmentTable
                                            on DPP.Account_Holder_ID = SegmentTable.Account_Holder_ID AND
                                            DPP.Season = SegmentTable.Season""", conn))
    #len(DataPredict)  # 5230765
    
    ## Write the table
    Path = 'Data Pickle/DataPredict/LTV - DataPredict - Segment.pckl'
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
    Path = 'Data Pickle/DataPredict/LTV - DataPredict - Segment.pckl'
    # Segment = 'Affiliates Bangladesh Seniority_180+'
    Path = Path.replace('Segment', Segment, 1)
    f = open(Path, 'rb')
    obj = pickle.load(f)
    f.close()
    DataPredict=obj
    del obj,f,Path
    
    # Load The Model -----------------------------------------------------    
    Path = 'Models - Python Pickle/LTV - New Script - Model - Segment.pckl'
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
    
    # CreateModelDate ----------------------------------------------------------
    DataPredict['CreateModelDate'] = CreateModelDate
    
    ## Output -------------------------------------------------------------
    OutPredictions = pd.DataFrame(data={'Account_Holder_ID': DataPredict['Account_Holder_ID'].astype(int),
                             'Model': 'LTV - New Script',
                             'Segment': Segment,
                             'Season': DataPredict['Season'].astype(str),
                             'CreateModelDate': DataPredict['CreateModelDate'],

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
    

    return OutPredictions
    

# -----------------------------------------------------------------------------------------------------------------------------
# Running the Predictions -----------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
## Start Time
StartPredictions = datetime.now()


##Taking all the Data Pickles
os.chdir("E:/LTV - New Script")
os.getcwd()
AllPredictionsSegments = os.listdir('Data Pickle/DataPredict/')
AllPredictionsSegments = [*map(lambda x: x.replace('.pckl',"", 1).replace('LTV - DataPredict - ',"", 1), AllPredictionsSegments)]
AllPredictionsSegments = pd.DataFrame(data={'Segments': AllPredictionsSegments})
#AllPredictionsSegments = AllPredictionsSegments.head(10)

 
DaskSegments = dd.from_pandas(AllPredictionsSegments, npartitions=10)
#client = Client(n_workers=1, threads_per_worker=NumberOfCores, memory_limit='40GB')
Output = DaskSegments.groupby('Segments').apply(RunPredictionsParallel,meta={'Account_Holder_ID': 'int',
                                                                             'Model': 'str',
                                                                             'Segment': 'str',
                                                                             'Season': 'str',
                                                                             'CreateModelDate': 'datetime64[ns]'',
                                                                             
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

Output = Output.reset_index()
## End Time
EndPredictions = datetime.now()
print(StartPredictions.strftime("%Y-%m-%d %H:%M:%S"))
print(EndPredictions.strftime("%Y-%m-%d %H:%M:%S"))
print(EndPredictions - StartPredictions)##0:23:35.034289

del StartPredictions,EndPredictions



# -----------------------------------------------------------------------------------------------------------------------------
# Output the Predictions ------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------
conn = pyodbc.connect('SERVER=HRZ-DWHSS-PRD;DRIVER={SQL Server};DATABASE=Dev_Predict_DB;UID=bipredict;PWD=CokeZero!')
cursor = conn.cursor()

## Drop the table and close the connection
cursor.execute("""DROP TABLE IF Exists [Dev_Predict_DB].[LTV].[TempOutputLTV]""")
conn.commit()

## Prepare the temporary table
TempOutputLTV = pd.DataFrame(
    data={'ID': Output['Account_Holder_ID'].astype(int),
          'SeniorityYears': (Output['Seniority']/365).astype(float),
          'SeniorityDays': Output['Seniority'].astype(int),
          'Model': np.where(Output['Segment']=='Other','Other',
                            [*map(' '.join, zip(Output['Country'],
                                                np.where(Output['Seniority']<=30,"Seniority_30",
                                                         np.where(Output['Seniority']<=90,"Seniority_30-90",
                                                                  np.where(Output['Seniority']<=180,"Seniority_90-180","Seniority_180+")))))]).astype(str),
          'PrimaryVertical': Output['PrimaryVertical'].astype(str),
          'PredictDelta': Output['Predict_calibrated'].astype(float),
          'PredictDelta2': Output['Predict_calibrated'].astype(float),
          'LocalRank': 1,
          'GlobalRank': 1,
          'Churn': Output['Churn90DaysPredict'],
          'ChurnCategory': 'Not Calculated',
          'CurrentDate': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          'UpdateDate': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          'PredictionForDate': (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S"),
          'CreateModelDate': Output['CreateModelDate'].astype('datetime64[ns]')
          })
##Change the type of some variables and slice them that they will fit to varchar(30)
TempOutputLTV.Model = TempOutputLTV.Model.str.slice(start=0, stop=29).astype(str)
TempOutputLTV.PrimaryVertical = TempOutputLTV.PrimaryVertical.str.slice(start=0, stop=29).astype(str)
TempOutputLTV.ChurnCategory = TempOutputLTV.ChurnCategory.str.slice(start=0, stop=29).astype(str)
TempOutputLTV.LocalRank = TempOutputLTV.LocalRank.astype(int)
TempOutputLTV.GlobalRank = TempOutputLTV.GlobalRank.astype(int)
TempOutputLTV.CurrentDate = TempOutputLTV.CurrentDate.astype('datetime64[ns]')
TempOutputLTV.UpdateDate = TempOutputLTV.UpdateDate.astype('datetime64[ns]')
TempOutputLTV.PredictionForDate = TempOutputLTV.PredictionForDate.astype('datetime64[ns]')


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
TempOutputLTV.to_sql(name='TempOutputLTV',
                     schema='LTV',index = False,
                     dtype={'Model': sqlalchemy.types.VARCHAR(length=30),
                            'PrimaryVertical': sqlalchemy.types.VARCHAR(length=30),
                            'ChurnCategory': sqlalchemy.types.VARCHAR(length=30),
                            'CurrentDate': sqlalchemy.types.DateTime(),
                            'UpdateDate': sqlalchemy.types.DateTime(),
                            'PredictionForDate': sqlalchemy.types.DateTime(),
                            'CreateModelDate': sqlalchemy.types.DateTime()},
                     con=getEngine(server='HRZ-DWHSS-PRD', database='Dev_Predict_DB'), 
                     if_exists='replace')
    
## Insert Into [Dev_Predict_DB].[dbo].[Output] 
sql = """
        BEGIN TRANSACTION
        
        insert into [Dev_Predict_DB].[dbo].[Output] 
        ([ID],[SeniorityYears],[SeniorityDays],[Model],[PrimaryVertical],
        [PredictDelta],[PredictDelta2],[LocalRank],[GlobalRank],
        [Churn],[ChurnCategory],[CurrentDate],[UpdateDate],[PredictionForDate],[CreateModelDate])
        select TempOutputLTV.* from [Dev_Predict_DB].[LTV].[c] as TempOutputLTV
        
        COMMIT TRANSACTION
     """
## insert TempOutputLTV to [Dev_Predict_DB].[dbo].[Output] 
cursor.execute(sql)
conn.commit()
  
## Drop the table and close the connection
cursor.execute("""DROP TABLE IF Exists [Dev_Predict_DB].[LTV].[TempOutputLTV]""")
conn.commit()  

## Closing The connection
conn.close()

## Output to CSV ----------------------------------------------------------------------
# df = pd.DataFrame(data={'Account_Holder_ID': Output['Account_Holder_ID'].astype(int),
#                          'PrimaryVertical': Output['PrimaryVertical'],                  
#                          'Segment': Output['Segment'],
#                          'Season': Output['Season'],                  

#                          'LMPredict': Output['LMPredict'],
#                          'Predict': Output['Predict'],
#                          'Predict_calibrated': Output['Predict_calibrated'],
#                          'Actual': Output['Actual']})
# df.to_csv('Output.csv')

