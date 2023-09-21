tryCatch(
  {
    ##Production Server package loader -------------------------------------------------------------------------
    .libPaths(new = "C:/Users/HRZ_ETL/Documents/R/win-library/3.5")
    
    # ##Develop Server package loader -------------------------------------------------------------------------
    # #.libPaths("C:/Users/dimaha/Documents/R/win-library/3.5/")
    # .libPaths("C:/Users/TEMP.PAYONEER/Documents/R/win-library/3.5")
    # #packageVersion("quantreg")
    # if(!("RODBC" %in% installed.packages()[,"Package"])){
    #   install.packages("RODBC", version='1.3.16',lib = "C:/Users/TEMP.PAYONEER/Documents/R/win-library/3.5")
    # }
    # if(!("plyr" %in% installed.packages()[,"Package"])){
    #   install.packages("plyr",version='1.8.6',lib = "C:/Users/TEMP.PAYONEER/Documents/R/win-library/3.5")
    # }
    # if(!("parallel" %in% installed.packages()[,"Package"])){
    #   install.packages("parallel",version='3.5.2',lib = "C:/Users/TEMP.PAYONEER/Documents/R/win-library/3.5")
    # }
    # if(!("doSNOW" %in% installed.packages()[,"Package"])){
    #   install.packages("doSNOW",version='1.0.16',lib = "C:/Users/TEMP.PAYONEER/Documents/R/win-library/3.5")
    # }
    # if(!("snow" %in% installed.packages()[,"Package"])){
    #   install.packages("snow",version='0.4.3',lib = "C:/Users/TEMP.PAYONEER/Documents/R/win-library/3.5")
    # }
    # if(!("survival" %in% installed.packages()[,"Package"])){
    #   install.packages("survival",version='2.43.3',lib = "C:/Users/TEMP.PAYONEER/Documents/R/win-library/3.5")
    # }
    # if(!("gains" %in% installed.packages()[,"Package"])){
    #   install.packages("gains",version='1.2',lib = "C:/Users/TEMP.PAYONEER/Documents/R/win-library/3.5")
    # }
    # if(!("pROC" %in% installed.packages()[,"Package"])){
    #   install.packages("pROC",version='1.16.2',lib = "C:/Users/TEMP.PAYONEER/Documents/R/win-library/3.5")
    # }
    # if(!("gtools" %in% installed.packages()[,"Package"])){
    #   install.packages("gtools",version='3.8.2',lib = "C:/Users/TEMP.PAYONEER/Documents/R/win-library/3.5")
    # }
    # if(!("quantreg" %in% installed.packages()[,"Package"])){
    #   install.packages("quantreg",version='5.55',lib = "C:/Users/TEMP.PAYONEER/Documents/R/win-library/3.5")
    # }
    library("RODBC")
    library("quantreg")
    library("gtools")
    library("plyr")
    
    ##Number of threads we will work with
    NumberOfCores<-round(parallel:::detectCores()/8)+1
    #NumberOfCores<-2
    
    setwd("E:/LTV - New Script/")
    options(scipen=30)
    
    Ranks_Dictionary<- function(temp_data,ranks_num) {
      
      quantile_seq<-seq(1/ranks_num, 1,1/ranks_num)
      overall_quantile<-round(unname(quantile(temp_data, probs = quantile_seq)),6)
      overall_quantile<-data.frame(quantile=quantile_seq,value=overall_quantile)
      lag_value<-c(NA,overall_quantile$value[-nrow(overall_quantile)])
      overall_quantile<-cbind(overall_quantile,lag_value)
      overall_quantile$lag_value<-ifelse(overall_quantile$quantile==1/ranks_num,-Inf,overall_quantile$lag_value)
      overall_quantile$value<-ifelse(round(overall_quantile$quantile,8)==1,Inf,overall_quantile$value)
      overall_quantile$rank<-as.integer(as.character(overall_quantile$quantile*ranks_num))
      overall_quantile<-subset(overall_quantile,value!=lag_value)
      return(overall_quantile)
    }
    
    estimate_mode <- function(x) {
      if(length(x)<=100){
        Mode<-median(x)
      } else{
        z<-log(abs(x)+1)
        d <- density(x = z,from = 0,to = max(z))
        LogMode<-d$x[which.max(d$y)]
        Mode<-exp(LogMode)-1
        if(Mode==Inf){
          Mode<-median(x)
        }
      }
      return(Mode)
    }
    
    ##### Geometric Mean
    GeometricMean <- function(x) {
      x[x<=1]<-1
      GeometricMean<-exp(mean(log(x)))
      if(GeometricMean==Inf){
        GeometricMean<-mean(x)
      }
      return(GeometricMean)
    }
    
    ##Create the YMC dictionaries 
    NumericYMC<-function(Variable,Y,NumberOfGroups=30){
      
      ##Create Dictionary for Variable 
      Dictionary<-Ranks_Dictionary(jitter(x = Variable,factor = 0.00001),ranks_num=NumberOfGroups)
      
      ##Convert Each value in variable to rank
      Variable<-data.frame(Variable,Y)
      Variable$rank<-Dictionary[findInterval(x = Variable$Variable, vec = Dictionary$lag_value,left.open = T),]$rank
      # Variable$rank<-sapply(X = Variable$Variable,FUN = function(x,tab=Dictionary){
      #   tab[which(x>=tab$lag_value & x<tab$value),]$rank
      # })
      
      ##Create The aggregation for each rank
      AggregationTable<-as.data.frame(as.list(aggregate(Variable$Y~Variable$rank,FUN = function(x){c(mean=mean(x,na.rm = TRUE),
                                                                                                     median=median(x,na.rm = TRUE),
                                                                                                     sd=sd(x,na.rm = TRUE),
                                                                                                     quantile99=quantile(x,probs = 0.99),
                                                                                                     length=length(x))})))
      colnames(AggregationTable)<-c("rank","Mean_YMC","Median_YMC","Sd_YMC","Quantile99_YMC","NumberOfObservation")
      
      ##Merge to The Dictionary
      Dictionary<-merge(x = Dictionary,y = AggregationTable,by = "rank",all.x = TRUE,all.y = FALSE)
      
      Dictionary$Mean_YMC<-ifelse(is.na(Dictionary$Mean_YMC),
                                  mean(Y,na.rm = TRUE),
                                  Dictionary$Mean_YMC)
      Dictionary$Median_YMC<-ifelse(is.na(Dictionary$Median_YMC),
                                    median(Y,na.rm = TRUE),
                                    Dictionary$Median_YMC)
      Dictionary$Sd_YMC<-ifelse(is.na(Dictionary$Sd_YMC),
                                sd(Y,na.rm = TRUE),
                                Dictionary$Sd_YMC)
      Dictionary$Quantile99_YMC<-ifelse(is.na(Dictionary$Quantile99_YMC),
                                        quantile(x = Y,probs = 0.99),
                                        Dictionary$Quantile99_YMC)
      
      Dictionary$NumberOfObservation<-ifelse(is.na(Dictionary$NumberOfObservation),0,Dictionary$NumberOfObservation)
      
      return(Dictionary)
    }
    
    
    
    ###----------------------------------------------------------------------------------------------------------------------------------------------------------
    ###----------------------------------------------------------------------------------------------------------------------------------------------------------
    ###-------------------------------------------------------------  Prediction on the test Data  --------------------------------------------------------------
    ###----------------------------------------------------------------------------------------------------------------------------------------------------------
    ###----------------------------------------------------------------------------------------------------------------------------------------------------------
    # Read the Data Panel -------------------------------------------------------
    channel<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
    DataPanelPredict<-sqlQuery(channel = channel,query = "select * from  [Predict_DB].[LTV].[DataPanel_FTL_Prediction]")
    odbcClose(channel)
    
    
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###---------------------------------------------------- Classify Seniority to Groups -------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ##Create Segments
    DataPanelPredict$PrimaryVertical<-as.character(DataPanelPredict$PrimaryVertical)
    DataPanelPredict$PrimaryVertical<-gsub("[^a-zA-Z0-9 -]", "", DataPanelPredict$PrimaryVertical)##Removing characters that gives us problem to save RData
    DataPanelPredict$SeniorityFactor<-as.character(ifelse(DataPanelPredict$Seniority<=30,"Seniority_30",
                                                          ifelse(DataPanelPredict$Seniority<=90,"Seniority_30-90",
                                                                 ifelse(DataPanelPredict$Seniority<=180,"Seniority_90-180","Seniority_180+"))))
    DataPanelPredict$Segment<-paste(DataPanelPredict$PrimaryVertical,DataPanelPredict$Country_Name,DataPanelPredict$SeniorityFactor)
    DataPanelPredict$Segment<-ifelse(DataPanelPredict$Segment %in% gsub(list.files(path = "Models/"),pattern = "LTV - Updated Script - |.RData",replacement=""),
                                     DataPanelPredict$Segment,
                                     paste(DataPanelPredict$PrimaryVertical,DataPanelPredict$SeniorityFactor))
    DataPanelPredict$Segment<-ifelse(DataPanelPredict$Segment %in% gsub(list.files(path = "Models/"),pattern = "LTV - Updated Script - |.RData",replacement=""),
                                     DataPanelPredict$Segment,
                                     DataPanelPredict$PrimaryVertical)
    DataPanelPredict$Segment<-ifelse(DataPanelPredict$Segment %in% gsub(list.files(path = "Models/"),pattern = "LTV - Updated Script - |.RData",replacement=""),
                                     DataPanelPredict$Segment,
                                     'Other')
    
    ##Delete All DataPredictFTL for the new one --------------------------------------------------------------------------------------
    file.remove(list.files(path = "Data/DataPredictFTL/",full.names = TRUE))
    
    ##Saving DataPanelPredict -----------------------------------------------------------------------------------------------------
    for(Segment in unique(DataPanelPredict$Segment)){
      DataPredict<-DataPanelPredict[which(DataPanelPredict$Segment==Segment),]
      save(DataPredict,compress = FALSE,
           file = gsub(x =  "Data/DataPredictFTL/LTV - DataPredict - Segment.RData",pattern = "Segment",replacement = Segment))
      
    }
    
    ##All Segments to work predict
    AllPredictSegments<-unique(DataPanelPredict$Segment)
    rm(DataPanelPredict)
    
    doSNOW::registerDoSNOW(cl<-snow::makeSOCKcluster(NumberOfCores))
    Output<-ddply(.data = data.frame(Segment=AllPredictSegments),
                  .variables = "Segment",
                  .progress =  "text",
                  .parallel=TRUE,
                  .inform = TRUE,
                  .paropts=list(.export=c("Ranks_Dictionary"),
                                .packages=c("quantreg")),
                  .fun = function(Segment){
                    
                    load(file = gsub(x =  "Data/DataPredictFTL/LTV - DataPredict - Segment.RData",pattern = "Segment",replacement = as.character(Segment$Segment[1])))
                    #load(file = gsub(x =  "Data/DataPredictFTL/LTV - DataPredict - Segment.RData",pattern = "Segment",replacement = Segment))
                    
                    
                    Segment<-unique(DataPredict$Segment)
                    
                    # Load All Dictionaries for the Segment ----------------------------------------------------------------------------------------------------------
                    TryLoad<-try(load(file = gsub(x =  "Models/LTV - Updated Script - Segment.RData",pattern = "Segment",replacement = Segment)),silent = TRUE)
                    if(class(TryLoad)=="try-error"){
                      load(file = gsub(x =  "Models/LTV - Updated Script - Segment.RData",pattern = "Segment",replacement = 'Other'))
                    }
                    
                    YMC_Factor_Dictionary_List<-NewEnvironment$YMC_Factor_Dictionary_List
                    TotalYMean<-NewEnvironment$TotalYMean
                    TotalYMedian<-NewEnvironment$TotalYMedian
                    TotalYQuantile<-NewEnvironment$TotalYQuantile
                    TotalYSd<-NewEnvironment$TotalYSd
                    PCA<-NewEnvironment$PCA
                    Dictionary_List<-NewEnvironment$Dictionary_List
                    glmModel<-NewEnvironment$glmModel
                    Model<-NewEnvironment$Model
                    UpperBorder<-NewEnvironment$UpperBorder
                    UpperLTVValue<-NewEnvironment$UpperLTVValue
                    Calibration<-NewEnvironment$Calibration
                    LMModel<-NewEnvironment$LMModel
                    CreateModelDate<-NewEnvironment$CreateModelDate
                    
                    
                    ### Inserting the YMC Values from the dictionaries ------------------------------------------------------------
                    for(VariableName in names(YMC_Factor_Dictionary_List)){
                      
                      ##VariableName<-"FirstDevice"
                      YMC_Dictionary<-YMC_Factor_Dictionary_List[[VariableName]]
                      
                      ##merging the YMC Factor into the data panel
                      DataPredict<-merge(x = DataPredict,
                                         y = YMC_Dictionary,
                                         by.x = VariableName,
                                         by.y = "Variable",
                                         all.x = TRUE,
                                         all.y = FALSE)
                      try(expr = {DataPredict$YMC_Mean_Variable[is.na(DataPredict$YMC_Mean_Variable)]<-TotalYMean},silent = TRUE)
                      try(expr = {DataPredict$YMC_Median_Variable[is.na(DataPredict$YMC_Median_Variable)]<-TotalYMedian},silent = TRUE)
                      try(expr = {DataPredict$YMC_Quantile_Variable[is.na(DataPredict$YMC_Quantile_Variable)]<-TotalYQuantile},silent = TRUE)
                      try(expr = {DataPredict$YMC_Sd_Variable[is.na(DataPredict$YMC_Sd_Variable)]<-TotalYSd},silent = TRUE)
                      
                      
                      colnames(DataPredict)[ncol(DataPredict)-3]<-paste0(VariableName,"_Mean_YMC")
                      colnames(DataPredict)[ncol(DataPredict)-2]<-paste0(VariableName,"_Median_YMC")
                      colnames(DataPredict)[ncol(DataPredict)-1]<-paste0(VariableName,"_Quantile_YMC")
                      colnames(DataPredict)[ncol(DataPredict)]<-paste0(VariableName,"_Sd_YMC")
                      
                    }
                    rm(YMC_Dictionary)
                    
                    # Churn Prediction -------------------------------------------------------------------------------------------------------------------------------
                    DataPredict$Churn90DaysPredict<-predict(object = glmModel,newdata = DataPredict,type="response")
                    
                    # PCA  Predict -----------------------------------------------------------------------------------------------------------------------------------
                    PcaData<-predict(object = PCA,newdata = DataPredict)
                    expl.var <- round(PCA$sdev^2/sum(PCA$sdev^2)*100,4) # percent explained variance
                    ColnamesInPca<-cumsum(expl.var)<=80
                    PcaData<-as.data.frame(PcaData[,ColnamesInPca,drop=FALSE])
                    DataPredict<-cbind(DataPredict,PcaData)
                    
                    ##Insert the YMC to the Numeric variables --------------------------------------------------------------------------------------------------------
                    YMC<-data.frame(Account_Holder_ID=DataPredict$Account_Holder_ID,Season=DataPredict$Season)
                    for(VariableToConvert in names(Dictionary_List)){
                      
                      ##Creating variable to merge it to YMC
                      Variable<-subset(DataPredict,select = c(VariableToConvert),drop=FALSE)
                      colnames(Variable)<-"VariableToConvert"
                      Variable$VariableToConvert[is.na(Variable$VariableToConvert)]<-0
                      
                      ##Adding All the YMC
                      Dictionary<-Dictionary_List[[VariableToConvert]]
                      Variable<-cbind(Variable,Dictionary[findInterval(x = Variable$VariableToConvert, vec = Dictionary$lag_value,left.open = T),c("Mean_YMC","Median_YMC","Sd_YMC","Quantile99_YMC")])
                      # Variable<-cbind(Variable,do.call(what = "rbind",args = lapply(X = Variable$VariableToConvert,FUN = function(x,tab=Dictionary_List[[VariableToConvert]]){
                      #   out<-tab[which(x>=tab$lag_value & x<tab$value),c("Mean_YMC","Median_YMC","Sd_YMC","Quantile99_YMC")]
                      #   return(out)
                      # })))
                      
                      ##Adding to YMC Table
                      YMC$VariableToConvert_Mean<-Variable$Mean_YMC
                      YMC$VariableToConvert_Median<-Variable$Median_YMC
                      YMC$VariableToConvert_Sd<-Variable$Sd_YMC
                      YMC$VariableToConvert_Quantile99<-Variable$Quantile99_YMC
                      
                      colnames(YMC)<-gsub(x = colnames(YMC),pattern = "VariableToConvert",replacement = paste0(VariableToConvert,"_YMC"))
                    }
                    DataPredict<-merge(x = DataPredict,y = YMC,by = c("Account_Holder_ID","Season"),all.x = TRUE)
                    
                    #######Prediction ------------------------------------------------------------------------------------------------------------------------------
                    ##LM Predict
                    DataPredict$LMPredict<-predict.lm(object = LMModel,newdata=DataPredict)
                    DataPredict$LMPredict[DataPredict$LMPredict<0]<-0
                    
                    
                    #Quantreg Predict
                    DataPredict$Predict<-predict.rq(object = Model,newdata=DataPredict)
                    DataPredict$Predict<-pmax(DataPredict$Predict,1)
                    
                    DataPredict$Predict<-round(DataPredict$Predict,digits = 2)
                    
                    ##LTV Upper Cut Point
                    DataPredict$Predict<-ifelse(DataPredict$Predict>=UpperBorder,UpperLTVValue,DataPredict$Predict)
                    
                    
                    # Calibration for prediction the Amount of Volume that will be loaded in 1 months -------------------------------------------------------------
                    # DataPredict$Calibration<-unlist(sapply(DataPredict$Predict,FUN = function(x,tab=Calibration){
                    #   Calibration<-tab[which(x>=tab$lag_value & x<tab$value),]$Calibration
                    #   return(Calibration)
                    # }))
                    DataPredict$Calibration<-Calibration[findInterval(x = DataPredict$Predict, vec = Calibration$lag_value,left.open = T),c("Calibration")]
                    DataPredict$Predict_calibrated<-DataPredict$Predict*DataPredict$Calibration
                    
                    # Output-------------------------------------------------------------
                    Out<-data.frame(Account_Holder_ID=DataPredict$Account_Holder_ID,
                                    Model="LTV - Updated Script",
                                    Segment=DataPredict$Segment,
                                    
                                    Seniority=DataPredict$Seniority,
                                    Country=DataPredict$Country_Name,
                                    PrimaryVertical=DataPredict$PrimaryVertical,
                                    
                                    Predict=DataPredict$Predict,
                                    Predict_calibrated=DataPredict$Predict_calibrated,
                                    
                                    Churn90DaysPredict=DataPredict$Churn90DaysPredict,
                                    Actual=DataPredict$Y,
                                    
                                    CreateModelDate=CreateModelDate)
                    
                    # Debugging -------------------------------------------------------------
                    sink(file = gsub("Debugging/Segment.txt",pattern = "Segment",replacement = unique(DataPredict$Segment)))
                    sink()
                    
                    return(Out)
                  })
    snow::stopCluster(cl) 
    
    # ----------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------- Delete Data --------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    file.remove(list.files(path = "Data/DataPredictFTL/",full.names = TRUE))
    
    ##Export FTL Conversion -------------------------------------------------
    ##Production Server;
    channel<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
    
    sqlQuery(channel,"IF OBJECT_ID('[Predict_DB].[dbo].[TempOutputLTV_FTL]', 'U') IS NOT NULL
             DROP TABLE [Predict_DB].[dbo].[TempOutputLTV_FTL]")
    
    TempOutputLTV_FTL<-data.frame(ID=Output$Account_Holder_ID,
                                  SeniorityYears=Output$Seniority/365,
                                  SeniorityDays=Output$Seniority,
                                  Model=ifelse(Output$Segment=='Other','Other',
                                               paste(Output$Country,
                                                     as.character(ifelse(Output$Seniority<=30,"Seniority_30",
                                                                         ifelse(Output$Seniority<=90,"Seniority_30-90",
                                                                                ifelse(Output$Seniority<=180,"Seniority_90-180","Seniority_180+")))))),
                                  PrimaryVertical=as.character(Output$PrimaryVertical),
                                  PredictDelta=Output$Predict_calibrated,
                                  PredictDelta2=Output$Predict_calibrated,
                                  LocalRank=1,
                                  GlobalRank=1,
                                  Churn=Output$Churn90DaysPredict,
                                  ChurnCategory='Not Calculated',
                                  CurrentDate=as.POSIXct(Sys.time()),
                                  UpdateDate=as.POSIXct(Sys.time()),
                                  PredictionForDate=as.POSIXct(Sys.Date()+365),
                                  CreateModelDate=Output$CreateModelDate)
    
    
    
    varTypes = c(ID="int",SeniorityYears="float",SeniorityDays="int",Model="varchar(30)",PrimaryVertical="varchar(30)",
                 PredictDelta="float",PredictDelta2="float",
                 LocalRank="int",GlobalRank="int",
                 Churn="float",ChurnCategory="varchar(30)",
                 CurrentDate="datetime",UpdateDate="datetime",PredictionForDate="datetime",CreateModelDate="datetime")
    sqlSave(channel,TempOutputLTV_FTL,rownames=F,varTypes = varTypes)
    
    
    
    sqlQuery(channel, "
             BEGIN TRANSACTION
             
             insert into [Predict_DB].[dbo].[Output] 
             ([ID],[SeniorityYears],[SeniorityDays],[Model],[PrimaryVertical],
             [PredictDelta],[PredictDelta2],[LocalRank],[GlobalRank],
             [Churn],[ChurnCategory],[CurrentDate],[UpdateDate],[PredictionForDate],[CreateModelDate])
             select TempOutputLTV_FTL.* from [Predict_DB].[dbo].[TempOutputLTV_FTL] as TempOutputLTV_FTL
             
             COMMIT TRANSACTION
             ")
    
    sqlQuery(channel,"IF OBJECT_ID('[Predict_DB].[dbo].[TempOutputLTV_FTL]', 'U') IS NOT NULL
             DROP TABLE [Predict_DB].[dbo].[TempOutputLTV_FTL]")
    
    close(channel)
    
    #successful Alerts -------------------------------------------------------
    Alerts_conn<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
    QueryString<-"INSERT INTO [Predict_DB].[dbo].Alerts (MasterModel, Model, Step,Description,Successful,DateRunnigTheStep)
    VALUES ('LTV', 'LTV - New Script - FTL', 'Output the predictions', 'Output the predictions script ran successfully',1,'2017-01-02 00:00:00.000');"
    sqlQuery(channel = Alerts_conn,
             query = gsub(x = QueryString,pattern = '2017-01-02 00:00:00.000',replacement = as.character.Date(Sys.time())))
    odbcClose(Alerts_conn)
  }
  ,error=function(err){
    # Not successful Alerts ---------------------------------------------------
    
    # If The DataPanel For Prediction is empty --------------------------------
    Predict_DB_conn<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
    DataForPredictions<-sqlQuery(channel = Predict_DB_conn,
                                 query = "select * from  [Predict_DB].[LTV].[DataPanel_FTL_Prediction]")
    odbcClose(Predict_DB_conn)
    
    if(nrow(DataForPredictions)==0){
      Alerts_conn<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
      QueryString<-"INSERT INTO [Predict_DB].[dbo].Alerts (MasterModel, Model, Step,Description,Successful,DateRunnigTheStep)
    VALUES ('LTV', 'LTV - New Script - FTL', 'Output the predictions', 'No Prediction to give - DataPanel is empty',1,'2017-01-02 00:00:00.000')"
      sqlQuery(channel = Alerts_conn,
               query = gsub(x = QueryString,pattern = '2017-01-02 00:00:00.000',replacement = as.character.Date(Sys.time())))
      odbcClose(Alerts_conn)
    }else{
      # Not successful Alerts -------------------------------------------------------
      Alerts_conn<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
      QueryString<-"INSERT INTO [Predict_DB].[dbo].Alerts (MasterModel, Model, Step,Description,Successful,DateRunnigTheStep)
    VALUES ('LTV', 'LTV - New Script - FTL', 'Output the predictions', 'Output the predictions script not ran successfully',0,'2017-01-02 00:00:00.000')"
      sqlQuery(channel = Alerts_conn,
               query = gsub(x = QueryString,pattern = '2017-01-02 00:00:00.000',replacement = as.character.Date(Sys.time())))
      odbcClose(Alerts_conn)
    }
  }
)
