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
    #library("quantregForest")
    
    
    
    ##Number of threads we will work with
    NumberOfCores<-round(parallel:::detectCores()/8)+1
    #NumberOfCores<-12
    
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
    
    
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###---------------------------------------------------- Read The Data Panel Model ----------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    #channel<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
    channel<-odbcDriverConnect('SERVER=HRZ-DWHSS-PRD;DRIVER={SQL Server};DATABASE=Dev_Predict_DB;UID=bipredict;PWD=CokeZero!')
    DataPanelModel<-sqlQuery(channel = channel,query = "select * from [Dev_Predict_DB].[LTV].[DataPanel_Model_TrainTest] where Is_Train=1")
    odbcClose(channel)
    nrow(DataPanelModel)##3658987
    
    ###------------------------------------------------------------------------------------------------------------------------------
    ###------------------------------------------------------------------------------------------------------------------------------
    ###----------------------------------------------------  Read The Data Panel Prediction  ----------------------------------------
    ###------------------------------------------------------------------------------------------------------------------------------
    ###------------------------------------------------------------------------------------------------------------------------------
    
    #channel<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
    channel<-odbcDriverConnect('SERVER=HRZ-DWHSS-PRD;DRIVER={SQL Server};DATABASE=Dev_Predict_DB;UID=bipredict;PWD=CokeZero!')
    DataPanelPredict<-sqlQuery(channel = channel,query = "select * from [Dev_Predict_DB].[LTV].[DataPanel_Model_TrainTest] where Is_Train=0")
    odbcClose(channel)
    nrow(DataPanelPredict)##1568135
    
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###------------------------------------------- Classify Seniority to Groups - Train Data ---------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ##Create a Segment 
    DataPanelModel$PrimaryVertical<-as.character(DataPanelModel$PrimaryVertical)
    DataPanelModel$PrimaryVertical<-gsub("[^a-zA-Z0-9 -]", "", DataPanelModel$PrimaryVertical)##Removing characters that gives us problem to save RData
    DataPanelModel$SeniorityFactor<-as.character(ifelse(DataPanelModel$Seniority<=30,"Seniority_30",
                                                        ifelse(DataPanelModel$Seniority<=90,"Seniority_30-90",
                                                               ifelse(DataPanelModel$Seniority<=180,"Seniority_90-180","Seniority_180+"))))
    DataPanelModel$Segment<-paste(DataPanelModel$PrimaryVertical,DataPanelModel$Country_Name,DataPanelModel$SeniorityFactor)
    DataPanelModel$Segment<-ifelse(DataPanelModel$Segment %in% names(table(DataPanelModel$Segment))[table(DataPanelModel$Segment)<=2000],
                                   paste(DataPanelModel$PrimaryVertical,DataPanelModel$SeniorityFactor),
                                   DataPanelModel$Segment)
    DataPanelModel$Segment<-ifelse(DataPanelModel$Segment %in% names(table(DataPanelModel$Segment))[table(DataPanelModel$Segment)<=2000],
                                   DataPanelModel$PrimaryVertical,
                                   DataPanelModel$Segment)
    DataPanelModel$Segment<-ifelse(DataPanelModel$Segment %in% names(table(DataPanelModel$Segment))[table(DataPanelModel$Segment)<=2000],'Other',DataPanelModel$Segment)
    
    #Enrich Other Segment
    # Temp<-DataPanelModel[sample(1:nrow(DataPanelModel),size = 7000,replace = FALSE),]
    # Temp$Segment<-'Other'
    # Temp$Season<-99
    # DataPanelModel<-rbind(DataPanelModel,Temp)
    # rm(Temp)

    ##Delete All DataModelChecking before creating new  -----------------------------------------------------------------------------------------------------
    file.remove(list.files(path = "Data/DataModelChecking/",full.names = TRUE))
    
    ##Saving DataPanelModel -----------------------------------------------------------------------------------------------------
    for(Segment in as.character(unique(DataPanelModel$Segment))){
      DataModel<-DataPanelModel[which(DataPanelModel$Segment==Segment),]
      save(DataModel,compress = FALSE,
           file = gsub(x =  "Data/DataModelChecking/LTV - DataModel - Segment.RData",pattern = "Segment",replacement = Segment))
    }
    try(rm(DataModel,Segment),silent = TRUE)
    
    ##All Segments to work on
    AllModelSegments<-gsub(list.files(path = "Data/DataModelChecking/"),pattern = "LTV - DataModel - |.RData",replacement="")
    rm(DataPanelModel)
    
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###------------------------------------------- Classify Seniority to Groups - Test Data ---------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ##Create Segments
    DataPanelPredict$PrimaryVertical<-as.character(DataPanelPredict$PrimaryVertical)
    DataPanelPredict$PrimaryVertical<-gsub("[^a-zA-Z0-9 -]", "", DataPanelPredict$PrimaryVertical)##Removing characters that gives us problem to save RData
    DataPanelPredict$SeniorityFactor<-as.character(ifelse(DataPanelPredict$Seniority<=30,"Seniority_30",
                                                          ifelse(DataPanelPredict$Seniority<=90,"Seniority_30-90",
                                                                 ifelse(DataPanelPredict$Seniority<=180,"Seniority_90-180","Seniority_180+"))))
    DataPanelPredict$Segment<-paste(DataPanelPredict$PrimaryVertical,DataPanelPredict$Country_Name,DataPanelPredict$SeniorityFactor)
    DataPanelPredict$Segment<-ifelse(DataPanelPredict$Segment %in% gsub(list.files(path = "Data/DataModelChecking/"),pattern = "LTV - DataModel - |.RData",replacement=""),
                                     DataPanelPredict$Segment,
                                     paste(DataPanelPredict$PrimaryVertical,DataPanelPredict$SeniorityFactor))
    DataPanelPredict$Segment<-ifelse(DataPanelPredict$Segment %in% gsub(list.files(path = "Data/DataModelChecking/"),pattern = "LTV - DataModel - |.RData",replacement=""),
                                     DataPanelPredict$Segment,
                                     DataPanelPredict$PrimaryVertical)
    DataPanelPredict$Segment<-ifelse(DataPanelPredict$Segment %in% gsub(list.files(path = "Data/DataModelChecking/"),pattern = "LTV - DataModel - |.RData",replacement=""),
                                     DataPanelPredict$Segment,
                                     'Other')
    
    
    ##Delete All DataPredictChecking before creating new -----------------------------------------------------------------------------------------------------
    file.remove(list.files(path = "Data/DataPredictChecking/",full.names = TRUE))
    
    
    ##Saving DataPanelPredict -----------------------------------------------------------------------------------------------------
    for(Segment in unique(DataPanelPredict$Segment)){
      DataPredict<-DataPanelPredict[which(DataPanelPredict$Segment==Segment),]
      save(DataPredict,compress = FALSE,
           file = gsub(x =  "Data/DataPredictChecking/LTV - DataPredict - Segment.RData",pattern = "Segment",replacement = Segment))
      
    }
    try(rm(DataPredict,Segment),silent = TRUE)
    
    ##All predict Segments to work on
    AllPredictSegments<-gsub(list.files(path = "Data/DataPredictChecking/"),pattern = "LTV - DataPredict - |.RData",replacement="")
    rm(DataPanelPredict)
    
    
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###--------------------------------------------------------- Model -------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    
    Start<-Sys.time()
    
    ##Delete All Models before creating new
    file.remove(list.files(path = "Models Before Check/",full.names = TRUE))
    
    ##Create the Models
    doSNOW::registerDoSNOW(cl<-snow::makeCluster(NumberOfCores))
    out<-ddply(.data = data.frame(Segment=AllModelSegments),
               .variables = "Segment",
               .progress =  "text",
               .parallel = TRUE,
               .inform = TRUE,
               .paropts=list(.export=c("NumericYMC","Ranks_Dictionary","GeometricMean","estimate_mode"),
                             .packages=c("quantreg")),
               .fun = function(Segment){
                 
                 load(file = gsub(x =  "Data/DataModelChecking/LTV - DataModel - Segment.RData",pattern = "Segment",replacement = as.character(Segment$Segment[1])))
                 #load(file = gsub(x =  "Data/DataModelChecking/LTV - DataModel - Segment.RData",pattern = "Segment",replacement = Segment))
                 
                 # # Debugging -------------------------------------------------------------
                 # sink(file = gsub("Debugging/Segment.txt",pattern = "Segment",replacement = as.character(Segment$Segment[1])))
                 # sink()
                 
                 
                 ##Samp the Data for Checking
                 #Segment <- 'Freelance India Seniority_90-180'
                 #Segment<-'MLM United States of America Seniority_180+'
                 #DataModel<-DataPanelModel[which(DataPanelModel$Segment==Segment),]
                 #DataModel<-DataModel[1:nrow(DataModel) %% 10 ==0,]
                 
                 # if(nrow(DataModel)>=10000){
                 #   DataModel<-DataModel[1:nrow(DataModel) %% 10 ==0,]
                 # }
                 # if(nrow(DataModel)>=10000){
                 #   DataModel<-DataModel[1:nrow(DataModel) %% 5 ==0,]
                 # }
                 # if(nrow(DataModel)>=10000){
                 #   DataModel<-DataModel[1:nrow(DataModel) %% 2 ==0,]
                 # }
                 #If there is a lot of Data we will use only the last Season
                 # if(nrow(DataModel[which(DataModel$Season==1),])>=5000){
                 #   DataModel<-DataModel[which(DataModel$Season==1),]
                 # }
                 
                 if(nrow(DataModel)<=200){
                   return(data.frame())
                 }
                 
                 DataModel$Y[DataModel$Y<0]<-0
                 DataModel$Y<-ifelse(DataModel$Y==0,dither(x = DataModel$Y,type = 'right',value = 0.01),DataModel$Y)
                 DataModel$Y<-ifelse(DataModel$Y<=0.1,dither(x = DataModel$Y,type = 'right',value = 0.001),DataModel$Y)
                 DataModel$Y<-ifelse(DataModel$Y<=0.1,dither(x = DataModel$Y,type = 'right',value = 0.0001),DataModel$Y)
                 DataModel$Y<-ifelse(DataModel$Y<=0.1,dither(x = DataModel$Y,type = 'right',value = 0.00001),DataModel$Y)
                 DataModel$Y<-ifelse(DataModel$Y<=0.1,dither(x = DataModel$Y,type = 'right',value = 0.000001),DataModel$Y)
                 DataModel$Y<-ifelse(DataModel$Y<=0.1,dither(x = DataModel$Y,type = 'right',value = 0.0000001),DataModel$Y)
                 
                 Segment<-as.character(unique(DataModel$Segment))
                 
                 ###YMC Trick for categorical Data-----------------------------------------------------------------------------------------------------------------------------
                 FactorVariables<-c("Account_Holder_Language","MainDomain","Country_Name",
                                    "Nationality_Country","City","RegistrationCompletionMonth","RegistrationCompletionWeekday",
                                    "FTL_Month","FTL_Weekday","UTM_Source","UTM_Medium",
                                    "UTM_Campaign","UTM","First_TM","First_TM_Flow","Account_Holder_Reg_Program",
                                    "RegistrationVertical","IsCoreVertical",
                                    "ValueSegment","Tier",
                                    
                                    "FirstBrowser","FirstDevice","FirstPlatform","PrimaryBrowser","NumberOfBrowsers",
                                    "PrimaryDevice","NumberOfDevices","PrimaryPlatform","NumberOfPlatforms",
                                    
                                    "PrimaryInquiry","NumberOfDistinctInquiry",
                                    
                                    "PrimaryVertical","PrimaryVolumeType","PrimaryVolumeTypeCategory",
                                    "PrimaryVolumeTypeSubCategory","PrimaryLoader",
                                    
                                    "PrimaryRevenueLoaderName","PrimaryRevenueVerticalName","PrimaryRevenueItem","PrimaryRevenueType",
                                    "PrimaryRevenureSubCategory","PrimaryRevenueCategory","PrimaryRevenueActivity",
                                    "PrimaryUsageType","PrimaryUsageTypeSubCategory","PrimaryUsageTypeCategory",
                                    "PrimaryUsageCurrency","PrimaryMerchent","IsLastLoadAfterLastUsage",
                                    "ActivitySegment","FLC","RFClassPredict_30","RFClassPredict_90","RFClassPredict_180",
                                    
                                    "NumberOfDistinctUsageEndCountry","NumberOfDistinctExternalBankName",
                                    "NumberOfUsageTypeCategory","NumberOfUsageTypeSubCategory","NumberOfUsageType",
                                    "NumberOfRevenueLoaderName","NumberOfRevenueItems","NumberOfRevenueTypes",
                                    "NumberOfRevenueSubCategories","NumberOfRevenueCategories","NumberOfRevenueActivities",
                                    "NumberOfVerticals","NumberOfDistinctFirstDepartment","NumberOfLoadersName",
                                    
                                    "NumberOfBearingEntityID_UntilNow","NumberOfBearingCustomerCategoryID_UntilNow",
                                    "NumberOfDistinctTransactionCode_Recently","NumberOfMerchentCategory",
                                    "NumberOfOfCurrencies",
                                    
                                    "PrimaryOpportunitiesStageDescription",
                                    "PrimaryOpportunitiesTypeDescription",
                                    "PrimaryOpportunitiesDiligenceStatuses"
                 )
                 
                 ### creating YMC Dictionaries for Factors ------------------------------------------------------------
                 YMC_Factor_Dictionary_List<-list()
                 for(VariableToConvert in FactorVariables){
                   
                   ##VariableToConvert<-"MainDomain"
                   
                   ##Creating variable to merge it to YMC
                   Variable<-subset(DataModel,select = c("Account_Holder_ID","Season","Y",VariableToConvert))
                   colnames(Variable)<-c("Account_Holder_ID","Season","Y","VariableToConvert")
                   Variable$VariableToConvert<-as.character(Variable$VariableToConvert)
                   Variable$VariableToConvert[is.na(Variable$VariableToConvert)]<-'NULL'
                   
                   ##Group all the Not Frequent Factor to one factor group
                   NotFrequentFactorGroup<-as.data.frame(as.list(aggregate(Variable$Y~Variable$VariableToConvert,FUN = function(x){if(length(x)<=50){return('Rare')}else{return('Frequent')}})))
                   colnames(NotFrequentFactorGroup)<-c("VariableName","SmallGroupOrNot")
                   Variable$VariableToConvert<-ifelse(Variable$VariableToConvert %in% as.character(NotFrequentFactorGroup[which(NotFrequentFactorGroup$SmallGroupOrNot=='Frequent'),]$VariableName)
                                                      ,Variable$VariableToConvert,"Not Frequent Factor")
                   
                   ##Dictionary
                   Dictionary_Variable_YMC<-as.data.frame(as.list(aggregate(Variable$Y~Variable$VariableToConvert,
                                                                            FUN = function(x){if(length(x)<=0){return(0)}else{return(c(mean=mean(x),median=median(x),quantile(x,0.99),sd=sd(x)))}}
                   )))
                   colnames(Dictionary_Variable_YMC)<-c("Variable","YMC_Mean_Variable","YMC_Median_Variable","YMC_Quantile_Variable","YMC_Sd_Variable")
                   Dictionary_Variable_YMC<-Dictionary_Variable_YMC[order(Dictionary_Variable_YMC$YMC_Mean_Variable,decreasing = TRUE),]
                   
                   ##Creating Dictionaries for all not frequent factors 
                   try(expr = {
                     NotFrequentFactor_Dictionary<-data.frame(Variable=as.character(NotFrequentFactorGroup[which(NotFrequentFactorGroup$SmallGroupOrNot=='Rare'),]$VariableName),
                                                              YMC_Mean_Variable=unique(Dictionary_Variable_YMC[which(Dictionary_Variable_YMC$Variable=='Not Frequent Factor'),]$YMC_Mean_Variable),
                                                              YMC_Median_Variable=unique(Dictionary_Variable_YMC[which(Dictionary_Variable_YMC$Variable=='Not Frequent Factor'),]$YMC_Median_Variable),
                                                              YMC_Quantile_Variable=unique(Dictionary_Variable_YMC[which(Dictionary_Variable_YMC$Variable=='Not Frequent Factor'),]$YMC_Quantile_Variable),
                                                              YMC_Sd_Variable=unique(Dictionary_Variable_YMC[which(Dictionary_Variable_YMC$Variable=='Not Frequent Factor'),]$YMC_Sd_Variable))
                   },silent = TRUE)
                   
                   if(!exists("NotFrequentFactor_Dictionary")){
                     NotFrequentFactor_Dictionary<-data.frame(Variable='Not Frequent Factor',YMC_Mean_Variable=0,YMC_Median_Variable=0,YMC_Quantile_Variable=0,YMC_Sd_Variable=0)
                   }
                   
                   ##Binding the 2 dictionaries
                   Dictionary_Variable_YMC<-rbind(Dictionary_Variable_YMC,NotFrequentFactor_Dictionary)
                   
                   ##Inserting the dictionary into a list
                   YMC_Factor_Dictionary_List[[VariableToConvert]]<-Dictionary_Variable_YMC
                   
                 }
                 TotalYMean<-mean(DataModel$Y)##We will insert the Total Y Mean for all the new predictions that are not in the Data Panel Model
                 TotalYMedian<-median(DataModel$Y)##We will insert the Total Y Mean for all the new predictions that are not in the Data Panel Model
                 TotalYQuantile<-quantile(DataModel$Y,0.99)##We will insert the Total Y Mean for all the new predictions that are not in the Data Panel Model
                 TotalYSd<-sd(DataModel$Y)##We will insert the Total Y Mean for all the new predictions that are not in the Data Panel Model
                 
                 
                 ### Inserting the YMC Values from the dictionaries ------------------------------------------------------------
                 for(VariableName in names(YMC_Factor_Dictionary_List)){
                   
                   ##VariableName<-"FirstDevice"
                   YMC_Dictionary<-YMC_Factor_Dictionary_List[[VariableName]]
                   
                   ##merging the YMC Factor into the data panel
                   DataModel<-merge(x = DataModel,
                                    y = YMC_Dictionary,
                                    by.x = VariableName,
                                    by.y = "Variable",
                                    all.x = TRUE,
                                    all.y = FALSE)
                   try(expr = {DataModel$YMC_Mean_Variable[is.na(DataModel$YMC_Mean_Variable)]<-TotalYMean},silent = TRUE)
                   try(expr = {DataModel$YMC_Median_Variable[is.na(DataModel$YMC_Median_Variable)]<-TotalYMedian},silent = TRUE)
                   try(expr = {DataModel$YMC_Quantile_Variable[is.na(DataModel$YMC_Quantile_Variable)]<-TotalYQuantile},silent = TRUE)
                   try(expr = {DataModel$YMC_Sd_Variable[is.na(DataModel$YMC_Sd_Variable)]<-TotalYSd},silent = TRUE)
                   
                   
                   colnames(DataModel)[ncol(DataModel)-3]<-paste0(VariableName,"_Mean_YMC")
                   colnames(DataModel)[ncol(DataModel)-2]<-paste0(VariableName,"_Median_YMC")
                   colnames(DataModel)[ncol(DataModel)-1]<-paste0(VariableName,"_Quantile_YMC")
                   colnames(DataModel)[ncol(DataModel)]<-paste0(VariableName,"_Sd_YMC")
                   
                 }
                 rm(YMC_Dictionary)
                 
                 # Numeric YMC -------------------------------------------------------------------------------------------------------------
                 NumericVariables<-c("Hours_From_RegComp_to_FTL","Hours_From_RegStarted_to_FTL","Minutes_RegStarted_to_Complete","Age",
                                     "Seniority","TimeBetweenRegistrationCompleteToCutPoint","NumberOfEnteringToAccount",
                                     "LastLogAndFirstLogDifference","LastLogCutPointDifference","NumberOfBrowsers",
                                     "NumberOfDevices","NumberOfPlatforms","SumMailSent","SumMailOpened","SDMailOpened",
                                     "SumMailClicked","LastMailDateCutPointDifference","DaysFromLastIncident",
                                     "NumberOfIncidents","NumberOfDistinctInquiry","NumberOfDistinctFirstSubjectDescription",
                                     "NumberOfDistinctFirstSubSubjectDescription","NumberOfDistinctFirstDepartment",
                                     "NumberOfMostFrequentInquiry","HoursBetweenLastLoadToCutPoint","HoursBetweenFirstLoadToCutPoint_InVolumeDays",
                                     "SumVolume_InVolumeDays","SdVolume_InVolumeDays","GeometricMeanVolume_InVolumeDays","CV_InVolumeDays",
                                     "SumOfCrossCountryVolume_InVolumeDays","MaxVolume_InVolumeDays","NumberOfLoads_InVolumeDays",
                                     "DistinctMonthsActivity_InVolumeDays","BillingServices_InVolumeDays","MAP_InVolumeDays",
                                     "MassPayout_InVolumeDays","PaymentService_InVolumeDays","Private_InVolumeDays",
                                     "Unknown_InVolumeDays","SumVolumeMap_InVolumeDays","NumberOfVolumeMap_InVolumeDays",
                                     "SumVolumePaymentRequest_InVolumeDays","NumberOfVolumePaymentRequest_InVolumeDays","NumberOfVerticals",
                                     "NumberOfLoadersName","SumVolume_UntilNow","SdVolume_UntilNow","GeometricMeanVolume_UntilNow",
                                     "CV_UntilNow","MaxVolume_UntilNow","SumOfCrossCountryVolume_UntilNow","NumberOfLoads_UntilNow",
                                     "DistinctMonthsActivity_UntilNow","SumVolumeMap_UntilNow",
                                     "NumberOfVolumeMap_UntilNow","SumVolumePaymentRequest_UntilNow","NumberOfVolumePaymentRequest_UntilNow",
                                     "NumberOfLoadsInFirst24H","VolumeAmountInFirst24H","NumberOfLoads_Volume1","SumVolume_Volume1",
                                     "SumVolumeMAP_Volume1","NumberOfVolumeMAP_Volume1","SdVolume_Volume1","MaxVolume_Volume1","AmountOfPaymentRequest_Volume1",
                                     "GeometricMeanVolume_Volume1","SumOfCrossCountryVolume_Volume1","NumberOfLoads_Volume2","SumVolume_Volume2",
                                     "SumVolumeMAP_Volume2","NumberOfVolumeMAP_Volume2","SdVolume_Volume2","MaxVolume_Volume2",
                                     "AmountOfPaymentRequest_Volume2","GeometricMeanVolume_Volume2",
                                     "SumOfCrossCountryVolume_Volume2","NumberOfLoads_Volume3","SumVolume_Volume3",
                                     "SumVolumeMAP_Volume3","NumberOfVolumeMAP_Volume3","SdVolume_Volume3","MaxVolume_Volume3",
                                     "AmountOfPaymentRequest_Volume3","GeometricMeanVolume_Volume3","SumOfCrossCountryVolume_Volume3","SumVolume_Volume4",
                                     "SumVolume_Volume5","SumVolume_Volume6","Payee_InternalMovementAmount","Payee_NumberOfInternalMovementAmount",
                                     "SumManageCurrencies","GeometricMeanManageCurrencies","NumberOfManageCurrencies",
                                     "DaysBetweenFirstRevenueToCutPoint","DaysFromLastRevenueToCutPoint","NumberOfDistinctTransactionCode_UntilNow","NumberOfRevnueTransactions_UntilNow",
                                     "PercentOfFX_UntilNow","NumberOfFX_UntilNow","PercentOfCrossCountry_UntilNow",
                                     "NumberOfCrossCountry_UntilNow","SumRevenue_UntilNow","GeometricMeanRevenue_UntilNow","SdRevenue_UntilNow","MeanRevenue_UntilNow",
                                     "MaxRevenue_UntilNow","SumRevenueWorkingCapital_UntilNow","SumRevenueUsage_UntilNow",
                                     "SumRevenueOther_UntilNow","SumRevenueVolume_UntilNow","SumRevenueInternalMovements_UntilNow","SumRevenueFX_UntilNow","SumRevenueCrossCountry_UntilNow",
                                     "SumRevenueChargedOffline_UntilNow","SumRevenueBearingEntity_UntilNow","GeometricMeanRevenueWorkingCapital_UntilNow",
                                     "GeometricMeanRevenueUsage_UntilNow","GeometricMeanRevenueOther_UntilNow","GeometricMeanRevenueVolume_UntilNow",
                                     "GeometricMeanRevenueInternalMovements_UntilNow","GeometricMeanFX_UntilNow","GeometricMeanCrossCountry_UntilNow",
                                     "NumberOfBearingEntityID_UntilNow","NumberOfBearingCustomerCategoryID_UntilNow","NumberOfDistinctTransactionCode_Recently",
                                     "NumberOfRevnueTransactions_Recently","PercentOfFX_Recently","NumberOfFX_Recently",
                                     "PercentOfCrossCountry_Recently","NumberOfCrossCountry_Recently","SumRevenue_Recently",
                                     "GeometricMeanRevenue_Recently","SdRevenue_Recently","MeanRevenue_Recently","MaxRevenue_Recently",
                                     "SumRevenueWorkingCapital_Recently","SumRevenueUsage_Recently","SumRevenueOther_Recently","SumRevenueVolume_Recently",
                                     "SumRevenueInternalMovements_Recently","SumRevenueFX_Recently","SumRevenueCrossCountry_Recently",
                                     "SumRevenueChargedOffline_Recently","SumRevenueBearingEntity_Recently",
                                     "GeometricMeanRevenueWorkingCapital_Recently","GeometricMeanRevenueUsage_Recently",
                                     "GeometricMeanRevenueOther_Recently","GeometricMeanRevenueVolume_Recently",
                                     "GeometricMeanRevenueInternalMovements_Recently","GeometricMeanFX_Recently","GeometricMeanCrossCountry_Recently",
                                     "NumberOfBearingEntityID_Recently","NumberOfBearingCustomerCategoryID_Recently",
                                     "NumberOfRevenueLoaderName","NumberOfRevenueItems","NumberOfRevenueTypes",
                                     "NumberOfRevenueSubCategories","NumberOfRevenueCategories","NumberOfRevenueActivities",
                                     "SumUsage_UntilTotal","NumberOfUsage_UntilTotal","LastLoadLastUsageDiff",
                                     
                                     "DaysBetweenFirstUsageToCutPoint","DaysFromLastUsageToCutPoint","SumIsFX",
                                     "SumIsCrossCountry","SumIsAutoWithdrawal","NumberOfMerchentCategory",
                                     "DiversityOfMerchentCategory","NumberOfOfCurrencies","SumCNP",
                                     "NumberOfUsage","SumUsage","GeometricMeanUsage","MaxUsage",
                                     "SdUsage","MeanUsage","SumUsage_FX","SumUsage_NotInUSD",
                                     "SumUsage_CNP","SumUsage_CrossCountry","SumUsage_MAP","SumUsage_ATM_POS",
                                     "SumUsage_Withdrawal","MeanUsage_MAP","MaxUsage_MAP","MeanUsage_ATM_POS",
                                     "MaxUsage_ATM_POS","MeanUsage_Withdrawal","MaxUsage_Withdrawal",
                                     "NumberOfStores","NumberOfStoresRecently","LastStoreCreationToCutPointDaysDifference",
                                     "FirstStoreCreationToCutPointDaysDifference",
                                     
                                     "LTV","ChurnFromLTVModel","FLW",
                                     "RFProbsPredict_30","RFProbsPredict_90","RFProbsPredict_180",
                                     
                                     "Payer_InternalMovementAmount","Payer_NumberOfInternalMovementAmount",
                                     
                                     "OpportunityProbability", 
                                     "OpportunityMonthlyVolumeCommitmentAmountUSD",
                                     "OpportunityExpectedAnnualVolumeAmountUSD",
                                     "OpportunityExpectedAnnualRevenueAmountUSD",
                                     "OpportunityForecastAnnualRevenueAmountUSD"
                 )
                 # Churn Prediction --------------------------------------------------------
                 ##Correlation
                 TempData<-DataModel[,colnames(DataModel) %in% c(colnames(DataModel)[grep(x = colnames(DataModel),pattern = "_YMC")],NumericVariables),drop=FALSE]
                 TempData<-TempData[,apply(X = TempData,MARGIN =  2,FUN =  function(x){return(round(var(x),4))})!=0,drop=FALSE]
                 CorMat<-cor(TempData,method = "pearson")
                 CorMat[upper.tri(CorMat)] <- 0
                 diag(CorMat) <- 0
                 TempData <- TempData[,!apply(X = CorMat,MARGIN = 2,FUN = function(x) any(abs(x) > 0.9))]
                 
                 ##Churn
                 glmFormula<-formula(paste0("as.factor(Churn90Days)~",paste(colnames(TempData),collapse = "+")))
                 if(sum(DataModel$Churn90Days)<=100){
                   DataModel$Churn90Days<-sample(x = c(0,1),size = nrow(DataModel),replace = TRUE)
                 }
                 glmModel<-glm(formula = glmFormula,
                               data = DataModel,
                               family = binomial(link="logit"))
                 rm(TempData,CorMat)
                 
                 if(!glmModel$converged){
                   ##Correlation
                   TempData<-DataModel[,colnames(DataModel) %in% c(colnames(DataModel)[grep(x = colnames(DataModel),pattern = "_YMC")],NumericVariables),drop=FALSE]
                   TempData<-TempData[,apply(X = TempData,MARGIN =  2,FUN =  function(x){return(round(var(x),4))})!=0,drop=FALSE]
                   CorMat<-cor(TempData,method = "pearson")
                   CorMat[upper.tri(CorMat)] <- 0
                   diag(CorMat) <- 0
                   TempData <- TempData[,!apply(X = CorMat,MARGIN = 2,FUN = function(x) any(abs(x) > 0.85))]
                   
                   ##Churn
                   glmFormula<-formula(paste0("as.factor(Churn90Days)~",paste(colnames(TempData),collapse = "+")))
                   if(sum(DataModel$Churn90Days)<=100){
                     DataModel$Churn90Days<-sample(x = c(0,1),size = nrow(DataModel),replace = TRUE)
                   }
                   glmModel<-glm(formula = glmFormula,
                                 data = DataModel,
                                 family = binomial(link="logit"))
                   rm(TempData,CorMat)
                 }
                 
                 if(!glmModel$converged){
                   ##Correlation
                   TempData<-DataModel[,colnames(DataModel) %in% c(colnames(DataModel)[grep(x = colnames(DataModel),pattern = "_YMC")],NumericVariables),drop=FALSE]
                   TempData<-TempData[,apply(X = TempData,MARGIN =  2,FUN =  function(x){return(round(var(x),4))})!=0,drop=FALSE]
                   CorMat<-cor(TempData,method = "pearson")
                   CorMat[upper.tri(CorMat)] <- 0
                   diag(CorMat) <- 0
                   TempData <- TempData[,!apply(X = CorMat,MARGIN = 2,FUN = function(x) any(abs(x) > 0.5))]
                   
                   ##Churn
                   glmFormula<-formula(paste0("as.factor(Churn90Days)~",paste(colnames(TempData),collapse = "+")))
                   if(sum(DataModel$Churn90Days)<=100){
                     DataModel$Churn90Days<-sample(x = c(0,1),size = nrow(DataModel),replace = TRUE)
                   }
                   glmModel<-glm(formula = glmFormula,
                                 data = DataModel,
                                 family = binomial(link="logit"),
                                 control = list(maxit = 100))
                   rm(TempData,CorMat)
                 }
                 
                 ##Check the Lifts
                 ##gains::gains(actual = glmModel$y,predicted = glmModel$fitted.values,groups = 10,optimal=TRUE,percents=TRUE)
                 
                 DataModel$Churn90DaysPredict<-glmModel$fitted.values
                 NumericVariables<-c(NumericVariables,"Churn90DaysPredict")
                 
                 ##Reducing Memory For glmModel
                 glmModel$data <- NULL
                 glmModel$y <- NULL
                 glmModel$linear.predictors <- NULL
                 glmModel$weights <- NULL
                 glmModel$fitted.values <- NULL
                 glmModel$model <- NULL
                 glmModel$prior.weights <- NULL
                 glmModel$residuals <- NULL
                 glmModel$effects <- NULL
                 glmModel$qr$qr <- NULL
                 
                 
                 # PCA Model---------------------------------------------------------------------
                 SubPCAData<-subset(x = DataModel,select = c(NumericVariables,colnames(DataModel)[grep(x = colnames(DataModel),pattern = "_YMC")]))
                 SubPCAData<-SubPCAData[,apply(X = SubPCAData,MARGIN =  2,FUN =  function(x){return(round(var(x),4))})!=0]
                 PCA<-prcomp(x =SubPCAData,scale. = TRUE,center = TRUE, retx = FALSE)
                 expl.var <- round(PCA$sdev^2/sum(PCA$sdev^2)*100,4) # percent explained variance
                 ColnamesInPca<-cumsum(expl.var)<=80
                 PcaData<-as.data.frame(cbind(Y=DataModel$Y,predict(object = PCA,newdata = DataModel)[,ColnamesInPca,drop=FALSE]))
                 
                 
                 # Adding pca to DataModel -------------------------------------------------
                 DataModel<-cbind(DataModel,subset(x = PcaData,select = setdiff(x = colnames(PcaData),y = "Y")))
                 
                 # Data Manipulation --------------------------------------------------------
                 ###Y - the predicted Variable
                 Y<-DataModel$Y
                 
                 #Create a YMC dictionary's
                 Dictionary_List<-list()
                 for(VariableToConvert in setdiff(x = colnames(PcaData),y = "Y")){
                   Variable<-subset(x = DataModel,select = VariableToConvert)[,1]
                   Variable[is.na(Variable)]<-0
                   Dictionary_List[[VariableToConvert]]<-NumericYMC(Variable = Variable,Y = Y,NumberOfGroups = max(7,round(nrow(DataModel)/600)))
                 }
                 
                 
                 ##Insert the YMC to the DataModel
                 YMC<-data.frame(Account_Holder_ID=DataModel$Account_Holder_ID,Season=DataModel$Season)
                 for(VariableToConvert in names(Dictionary_List)){
                   
                   ##Creating variable to merge it to YMC
                   Variable<-subset(DataModel,select = c(VariableToConvert),drop=FALSE)
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
                 DataModel<-merge(x = DataModel,y = YMC,by = c("Account_Holder_ID","Season"),all.x = TRUE)
                 rm(YMC)
                 
                 ##Remove All NA coefficients And reducing the number of variables in the model -------------------------------------------------------------------
                 VariablesToTheModel<-colnames(DataModel)[grep(x = colnames(DataModel),pattern = "_YMC")]
                 
                 ##Removing Correlations
                 TempData<-DataModel[,colnames(DataModel) %in% VariablesToTheModel,drop=FALSE]
                 TempData<-TempData[,apply(X = TempData,MARGIN =  2,FUN =  function(x){return(round(var(x),4))})!=0,drop=FALSE]
                 CorMat<-cor(TempData,method = "pearson")
                 CorMat[upper.tri(CorMat)] <- 0
                 diag(CorMat) <- 0
                 TempData <- TempData[,!apply(X = CorMat,MARGIN = 2,FUN = function(x) any(abs(x) > 0.85))]
                 
                 VariablesToTheModel<-colnames(TempData)
                 rm(TempData,CorMat)
                 
                 
                 # ##Creating the formula
                 if(length(VariablesToTheModel)==0){
                   form<-formula("Y~")
                 }else{
                   form<-formula(paste0("Y~",paste(VariablesToTheModel,collapse = "+")))
                 }
                 
                 ##Removing NA's (rq model have a problem when there are NA in the formula model)
                 LMModel <- lm(formula = form,data = DataModel)
                 DataModel$LMModel <- predict(LMModel,DataModel)
                 
                 # #Python
                 # PythonLMModel <- lm(formula = form,data = PythonData)
                 # PythonData$LMModel <- predict(PythonLMModel,PythonData)
                 
                 if(any(is.na(coef(LMModel)))){
                   # cf <- coef(LMModel)
                   # fmla <- drop.terms(terms(LMModel), which(is.na(cf)) - 1, keep.response = TRUE)
                   # LMModel <- lm(formula(fmla), data = DataModel)
                   
                   NewCoef <- coef(LMModel)
                   NewCoef<-names(NewCoef[!is.na(NewCoef)])[-1]
                   NewFormula<-formula(paste("Y ~ ",
                                             paste(NewCoef,collapse = "+")))
                   LMModel<-lm(formula = NewFormula,data = DataModel)
                 }
                 
                 ###Reduce the model again  (Removing all the not Significant pValues variables)-------------------------------------------------------------------
                 ReducedModel<-data.frame(Variable=names(summary(LMModel)$coefficients[,4]),PValue=as.numeric(summary(LMModel)$coefficients[,4]))
                 ReducedModel<-as.character(ReducedModel[which(ReducedModel$PValue<=0.1),]$Variable)##Removing variables with not significant variables
                 
                 
                 # Formula -------------------------------------------------------------------------------------------------------------------------------
                 ## Adding YMC Variables
                 VariablesToTheModel<-ReducedModel[grep(x = ReducedModel,pattern = "YMC")]
                 
                 ## Adding raw numeric variables
                 VariablesToTheModel<-c(VariablesToTheModel,c("SumRevenue_Recently","LTV","SumUsage","SumVolume_InVolumeDays","ThisYearLTV"))
                 
                 ## Adding Transformation
                 VariablesToTheModel<-c(VariablesToTheModel,c("I(SumRevenue_UntilNow/(abs(SumVolume_UntilNow)+1))",
                                                              "I(SumUsage/(abs(SumRevenue_Recently)+1))",
                                                              "I(HoursBetweenLastLoadToCutPoint/(abs(Seniority)+1))",
                                                              "I((abs(LTV)+1)/(abs(ThisYearLTV)+1))",
                                                              "I(ThisYearLTV/(abs(Seniority)+1))"))
                 
                 ## Creating the Formula
                 if(length(VariablesToTheModel)==0){
                   form<-formula("Y~1")
                 }else{
                   form<-formula(paste0("Y~",paste(VariablesToTheModel,collapse = "+")))
                 }
                 
                 ##Removing NA's (rq model have a problem when there are NA in the formula model)
                 LMModel <- lm(formula = form,data = DataModel,model = FALSE)
                 if(any(is.na(coef(LMModel)))){
                   # cf <- coef(LMModel)
                   # fmla <- drop.terms(terms(LMModel), which(is.na(cf)) - 1, keep.response = TRUE)
                   # LMModel <- lm(formula(fmla), data = DataModel)
                   
                   NewCoef <- coef(LMModel)
                   NewCoef<-names(NewCoef[!is.na(NewCoef)])[-1]
                   NewFormula<-formula(paste("Y ~ ",
                                             paste(NewCoef,collapse = "+")))
                   LMModel<-lm(formula = NewFormula,data = DataModel)
                 }
                 LMModel$fitted.values<-NULL
                 LMModel$data <- NULL
                 LMModel$y <- NULL
                 LMModel$linear.predictors <- NULL
                 LMModel$weights <- NULL
                 LMModel$fitted.values <- NULL
                 LMModel$model <- NULL
                 LMModel$prior.weights <- NULL
                 LMModel$residuals <- NULL
                 LMModel$effects <- NULL
                 LMModel$qr$qr <- NULL
                 
                 QuantileFormula<-formula(LMModel)
                 #rm(TempModel)
                 
                 # Creating the model ---------------------------------------------------------------------------------------
                 try(rm(list = "Model"),silent = TRUE)
                 tau<-0.5
                 
                 #########sfn method type
                 if(!exists("Model")){
                   try({
                     Model<-rq(formula = QuantileFormula,data = DataModel,tau = tau,method = "sfn",model = FALSE)
                   },silent = TRUE)
                 }
                 
                 #########pfn method type
                 if(!exists("Model")){
                   try({
                     Model<-rq(formula = QuantileFormula,data = DataModel,tau = tau,method = "pfn",model = FALSE)
                   },silent = TRUE)
                 }
                 
                 #########fn method type
                 if(!exists("Model")){
                   try({
                     Model<-rq(formula = QuantileFormula,data = DataModel,tau = tau,method = "fn",model = FALSE)
                   },silent = TRUE)
                 }
                 
                 #########lasso method type
                 if(!exists("Model")){
                   try({
                     Model<-rq(formula = QuantileFormula,data = DataModel,tau = tau,method = "lasso",model = FALSE)
                   },silent = TRUE)
                 }
                 
                 #########br method type
                 if(!exists("Model")){
                   try({
                     Model<-rq(formula = QuantileFormula,data = DataModel,tau = tau,method = "br",model = FALSE)
                   },silent = TRUE)
                 }
                 
                 #########br method type
                 if(!exists("Model")){
                   try({
                     Model<-rq(formula = formula("Y ~ 1"),data = DataModel,tau = tau,method = "br",model = FALSE)
                   },silent = TRUE)
                 }
                 
                 Model$residuals<-NULL
                 Model$fitted.values<-NULL
                 
                 # Prediction for calibration----------------------------------------------------------
                 DataModel$Predict<-predict.rq(object = Model,newdata = DataModel)
                 # DataModel$Predict<-pmax(DataModel$Predict,0)
                 DataModel$Predict<-pmax(DataModel$Predict,1)
                 
                 ##LTV Upper Cut Point
                 UpperBorder<-quantile(DataModel$Predict,0.999)
                 UpperLTVValue<-mean(DataModel$Predict[DataModel$Predict>=UpperBorder])
                 DataModel$Predict<-ifelse(DataModel$Predict>=UpperBorder,UpperLTVValue,DataModel$Predict)
                 
                 # Calibration ----------------------------------------------------------
                 pred<-DataModel$Predict[DataModel$Predict<quantile(DataModel$Predict,0.9)]
                 predTop<-DataModel$Predict[DataModel$Predict>=quantile(DataModel$Predict,0.9)]
                 
                 if(length(pred)==0 | length(predTop)==0){
                   DataModel$Predict<-dither(x = DataModel$Predict,type = 'right',value = 0.001)
                   DataModel$Predict<-dither(x = DataModel$Predict,type = 'right',value = 0.0001)
                   DataModel$Predict<-dither(x = DataModel$Predict,type = 'right',value = 0.00001)
                   DataModel$Predict<-dither(x = DataModel$Predict,type = 'right',value = 0.000001)
                   DataModel$Predict<-dither(x = DataModel$Predict,type = 'right',value = 0.0000001)
                   DataModel$Predict<-dither(x = DataModel$Predict,type = 'right',value = 0.0000001)
                   DataModel$Predict<-dither(x = DataModel$Predict,type = 'right',value = 0.0000001)
                   
                   pred<-DataModel$Predict[DataModel$Predict<quantile(DataModel$Predict,0.9)]
                   predTop<-DataModel$Predict[DataModel$Predict>=quantile(DataModel$Predict,0.9)]
                 }
                 
                 # Calibration1<-Ranks_Dictionary(pred,max(10,round(length(pred)/700)))
                 # Calibration2<-Ranks_Dictionary(predTop,max(5,round(length(predTop)/400)))
                 
                 Calibration1<-Ranks_Dictionary(round(pred),max(10,round(length(pred)/700)))
                 Calibration2<-Ranks_Dictionary(round(predTop),max(5,round(length(predTop)/400)))
                 
                 Calibration1$value[Calibration1$value==Inf]<-Calibration2$value[Calibration2$rank==1]
                 Calibration2<-Calibration2[which(Calibration2$rank>1),]
                 
                 Calibration<-rbind(Calibration1,Calibration2)
                 Calibration$rank<-1:nrow(Calibration)
                 
                 # DataModel$PredictedRank<-sapply(DataModel$Predict,FUN = function(x,tab=Calibration){
                 #   tab[which(x>=tab$lag_value & x<tab$value),]$rank
                 # })
                 DataModel$PredictedRank<-Calibration[findInterval(x = DataModel$Predict, vec = Calibration$lag_value,left.open = T),c("rank")]
                 
                 a<-as.data.frame(as.list(aggregate(DataModel$Predict~DataModel$PredictedRank,FUN = function(x) c(mean = mean(x),length = length(x)))))
                 b<-as.data.frame(as.list(aggregate(DataModel$Y~DataModel$PredictedRank,FUN = function(x) c(mean = mean(x),length = length(x)))))
                 colnames(a)<-c("Ranks","PredictedMean","PredictedLength")
                 colnames(b)<-c("Ranks","YMean","YLength")
                 
                 c<-merge(x=a,y = b,by = "Ranks",all.x = TRUE,all.y = FALSE)
                 c$Diff<-abs(100*(c$PredictedMean-c$YMean)/c$YMean)
                 
                 CalibrationTable<-data.frame(rank=c$Ranks,Calibration=c$YMean/c$PredictedMean,YMean=c$YMean,length=c$YLength)
                 Calibration<-merge(x = Calibration,CalibrationTable,by ="rank",all.x = TRUE,all.y = FALSE)
                 Calibration$Calibration[is.infinite(Calibration$Calibration)]<-1
                 Calibration$Calibration[is.na(Calibration$Calibration)]<-median(Calibration$Calibration,na.rm = TRUE)
                 
                 # DataModel$Calibration<-sapply(DataModel$Predict,FUN = function(x,tab=Calibration){
                 #   tab[which(x>=tab$lag_value & x<tab$value),]$Calibration
                 # })
                 # DataModel$Predict_calibrated<-DataModel$Predict*DataModel$Calibration
                 # sqrt(mean((DataModel$Predict_calibrated-DataModel$Y)^2))##4094.748
                 
                 ###Second calibration (Smoothing the calibration)
                 MA<-c()
                 for(Row in 1:nrow(Calibration)){
                   if(Row==1){
                     ma<-(Calibration$Calibration[Row]+Calibration$Calibration[Row+1])/2
                   }else if(Row==nrow(Calibration)){
                     ma<-(Calibration$Calibration[Row-1]+Calibration$Calibration[Row])/2
                   }else{
                     ma<-(Calibration$Calibration[Row-1]+Calibration$Calibration[Row]+Calibration$Calibration[Row+1])/3
                   }
                   MA<-c(MA,ma)
                 }
                 Calibration$Calibration2<-MA
                 
                 # DataModel$Calibration2<-sapply(DataModel$Predict,FUN = function(x,tab=Calibration){
                 #   tab[which(x>=tab$lag_value & x<tab$value),]$Calibration
                 # })
                 # 
                 # DataModel$Predict_calibrated2<-DataModel$Predict*DataModel$Calibration2
                 
                 
                 #LM Model 2 --------------------------------------------------------------
                 # ## Adding factorial variables
                 # FactoVariableList<-data.frame()
                 # for(FV in paste0(FactorVariables,"_Mean_YMC")){
                 #   InModelOrNot = as.numeric(sum(table(DataModel[,FV])>50)==length(unique(DataModel[,FV])) & length(table(DataModel[,FV]))>1)
                 #   FactoVariableList<-rbind(FactoVariableList,data.frame(FV,
                 #                                                         FactorName = paste0(paste0("factor(",FV),")"),
                 #                                                         InModelOrNot = InModelOrNot,
                 #                                                         length = length(table(DataModel[,FV]))))
                 # }
                 # VariablesToTheModel<-as.character(FactoVariableList[FactoVariableList$InModelOrNot>0,]$FactorName)
                 # Form2<-formula(paste("Y ~ ",paste(c(VariablesToTheModel,NumericVariables),collapse = "+")))
                 # LMModel2 <- lm(formula = Form2,data = DataModel,model = FALSE)
                 # # if(any(is.na(coef(LMModel2)))){
                 # #   NewCoef <- coef(LMModel2)
                 # #   NewCoef<-names(NewCoef[!is.na(NewCoef)])[-1]
                 # #   NewFormula<-formula(paste("Y ~ ",paste(NewCoef,collapse = "+")))
                 # #   LMModel2<-lm(formula = NewFormula,data = DataModel)
                 # # }
                 # LMModel2$fitted.values<-NULL
                 # LMModel2$data <- NULL
                 # LMModel2$y <- NULL
                 # LMModel2$linear.predictors <- NULL
                 # LMModel2$weights <- NULL
                 # LMModel2$fitted.values <- NULL
                 # LMModel2$model <- NULL
                 # LMModel2$prior.weights <- NULL
                 # LMModel2$residuals <- NULL
                 # LMModel2$effects <- NULL
                 # LMModel2$qr$qr <- NULL
                 # #sqrt(mean((LMModel2$fitted.values-LMModel2$model$Y)^2))##3142.875
                 # #summary(LMModel2)#0.6431
                 
                 
                 # Save The Model -------------------------------------------------------------               
                 NewEnvironment<-new.env()
                 NewEnvironment$YMC_Factor_Dictionary_List<-YMC_Factor_Dictionary_List
                 NewEnvironment$TotalYMean<-TotalYMean
                 NewEnvironment$TotalYMedian<-TotalYMedian
                 NewEnvironment$TotalYQuantile<-TotalYQuantile
                 NewEnvironment$TotalYSd<-TotalYSd
                 NewEnvironment$PCA<-PCA
                 NewEnvironment$Dictionary_List<-Dictionary_List
                 NewEnvironment$glmModel<-glmModel
                 NewEnvironment$Model<-Model
                 NewEnvironment$UpperBorder<-UpperBorder
                 NewEnvironment$UpperLTVValue<-UpperLTVValue
                 NewEnvironment$Calibration<-Calibration
                 NewEnvironment$LMModel<-LMModel
                 #NewEnvironment$LMModel2<-LMModel2
                 NewEnvironment$CreateModelDate<-Sys.time()
                 
                 
                 rm(list=setdiff(ls(), c("NewEnvironment","Segment")))
                 #gc()
                 
                 save(NewEnvironment,compress = FALSE,
                      file = gsub(x =  "Models Before Check/LTV - New Script - Model Checking - Segment.RData",pattern = "Segment",replacement = Segment))
                 
                 
                 return(data.frame())
                 
               })
    snow::stopCluster(cl)
    End<-Sys.time()
    End-Start##2.299898 
    
    # ##Delete All Files for keeping space
    #file.remove(list.files(path = "Data/DataModelChecking/",full.names = TRUE))
    
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-------------------------------------------------------   Prediction   ------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    ###-----------------------------------------------------------------------------------------------------------------------------
    
    # ##Delete All Files In debugging
    file.remove(list.files(path = "Debugging/",full.names = TRUE))
    
    ##Run the prediction
    doSNOW::registerDoSNOW(cl<-snow::makeSOCKcluster(NumberOfCores))
    Output<-ddply(.data = data.frame(Segment=AllPredictSegments),
                  .variables = "Segment",
                  .progress =  "text",
                  .parallel=TRUE,
                  .inform = TRUE,
                  .paropts=list(.export=c("Ranks_Dictionary"),
                                .packages=c("quantreg")),
                  .fun = function(Segment){
                    
                    load(file = gsub(x =  "Data/DataPredictChecking/LTV - DataPredict - Segment.RData",pattern = "Segment",replacement = as.character(Segment$Segment[1])))
                    #load(file = gsub(x =  "Data/DataPredictChecking/LTV - DataPredict - Segment.RData",pattern = "Segment",replacement = Segment))
                    
                    if(nrow(DataPredict)==0){
                      return(data.frame())
                    }
                    
                    Segment<-as.character(unique(DataPredict$Segment))
                    
                    #Segment<-'VIP - Greater China China Seniority_180+'
                    #DataPredict<-DataPanelPredict[which(DataPanelPredict$Segment==Segment),]
                    #DataPredict<-DataPredict[1:nrow(DataPredict) %% 2==0,]
                    
                    # Load All Dictionaries for the Segment ----------------------------------------------------------------------------------------------------------
                    TryLoad<-try(load(file = gsub(x =  "Models Before Check/LTV - New Script - Model Checking - Segment.RData",pattern = "Segment",replacement = Segment)),silent = TRUE)
                    if(class(TryLoad)=="try-error"){
                      load(file = gsub(x =  "Models Before Check/LTV - New Script - Model Checking - Segment.RData",pattern = "Segment",replacement = 'Other'))
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
                    #LMModel2<-NewEnvironment$LMModel2
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
                    
                    
                    ##Ratio Variables
                    # DataPredict$Ratio1<-with(expr = I(SumRevenue_UntilNow/(abs(SumVolume_UntilNow) + 1)),data = DataPredict)
                    # DataPredict$Ratio2<-with(I(SumUsage/(abs(SumRevenue_Recently) + 1)),data = DataPredict)  
                    # DataPredict$Ratio3<-with(I(HoursBetweenLastLoadToCutPoint/(abs(Seniority) + 1)) ,data = DataPredict)
                    # DataPredict$Ratio4<-with(I((abs(LTV) + 1)/(abs(ThisYearLTV) + 1)),data = DataPredict)
                    # DataPredict$Ratio5<-with(I(ThisYearLTV/(abs(Seniority) + 1)),data = DataPredict) 
                    
                    #######Prediction ------------------------------------------------------------------------------------------------------------------------------
                    ##LM Predict
                    DataPredict$LMPredict<-predict.lm(object = LMModel,newdata=DataPredict)
                    DataPredict$LMPredict[DataPredict$LMPredict<0]<-0
                    
                    ##LM Predict 2
                    # DataPredict$LMPredict2<-predict.lm(object = LMModel2,newdata=DataPredict)
                    # DataPredict$LMPredict2[DataPredict$LMPredict2<0]<-0
                    
                    #Quantreg Predict
                    #DataPredict$Predict<-exp(predict.rq(object = Model,newdata=DataPredict))-1
                    DataPredict$Predict<-predict.rq(object = Model,newdata=DataPredict)
                    #DataPredict$Predict[DataPredict$Predict<=0.5]<-0
                    DataPredict$Predict<-pmax(DataPredict$Predict,1)
                    #DataPredict$Predict<-pmax(DataPredict$Predict,0.1)
                    
                    DataPredict$Predict<-round(DataPredict$Predict,digits = 2)
                    
                    ##LTV Upper Cut Point
                    DataPredict$Predict<-ifelse(DataPredict$Predict>=UpperBorder,UpperLTVValue,DataPredict$Predict)
                    
                    
                    # Calibration for prediction the Amount of Volume that will be loaded in 1 months -------------------------------------------------------------
                    # DataPredict$Calibration<-unlist(sapply(DataPredict$Predict,FUN = function(x,tab=Calibration){
                    #   Calibration<-tab[which(x>=tab$lag_value & x<tab$value),]$Calibration
                    #   return(Calibration)
                    # }))
                    DataPredict$Calibration<-Calibration[findInterval(x = DataPredict$Predict, vec = Calibration$lag_value,left.open = T),c("Calibration")]
                    
                    # DataPredict$Predict_calibrated<-ifelse(DataPredict$Predict<=1,
                    #                                        pmax(DataPredict$Predict*DataPredict$Calibration-DataPredict$Calibration,0),
                    #                                        DataPredict$Predict*DataPredict$Calibration)
                    DataPredict$Predict_calibrated<-DataPredict$Predict*DataPredict$Calibration
                    
                    # Output-------------------------------------------------------------
                    Out<-data.frame(Account_Holder_ID=DataPredict$Account_Holder_ID,
                                    Model="LTV - New Script",
                                    Segment=DataPredict$Segment,
                                    Season=DataPredict$Season,
                                    
                                    Seniority=DataPredict$Seniority,
                                    Country=DataPredict$Country_Name,
                                    PrimaryVertical=DataPredict$PrimaryVertical,
                                    
                                    Predict=DataPredict$Predict,
                                    Predict_calibrated=DataPredict$Predict_calibrated,
                                    LMPredict=DataPredict$LMPredict,
                                    #LMPredict2=DataPredict$LMPredict2,
                                    ThisYearLTV=DataPredict$ThisYearLTV,
                                    
                                    Churn90DaysPredict=DataPredict$Churn90DaysPredict,
                                    Actual=DataPredict$Y)
                    
                    
                    # Debugging -------------------------------------------------------------
                    sink(file = gsub("Debugging/Segment.txt",pattern = "Segment",replacement = unique(DataPredict$Segment)))
                    sink()
                    
                    return(Out)
                  })
    snow::stopCluster(cl) 
    End<-Sys.time()
    End-Start##2.445124
    
    # ----------------------------------------------------------------------------------------------------------------------
    # -----------------------------------------------Delete All Files ------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    # ##Delete All Files for keeping space
    file.remove(list.files(path = "Models Before Check",full.names = TRUE))
    file.remove(list.files(path = "Data/DataPredictChecking/",full.names = TRUE))
    
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------- Cumulative Volume percentage -----------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    OrderPrediction<-order(Output$Predict_calibrated,decreasing = TRUE)
    Predicted<-Output$Predict_calibrated[OrderPrediction]
    Y_predict<-Output$Actual[OrderPrediction]
    
    CumnsumY_predict<-cumsum(Y_predict)
    CumnsumPredicted<-cumsum(Predicted)
    
    
    ##Predicted
    plot(x = 100*(1:length(CumnsumPredicted))/length(CumnsumPredicted),
         y = 100*CumnsumPredicted/sum(Predicted),col="blue",
         type = "l", 
         xlab = "Percent Of AH",
         ylab = "Camulative Revenue percentage",main = "LTV Model")
    ##Optimal
    lines(x = 100*(1:length(Output$Actual[order(Output$Actual,decreasing = TRUE)])/length(Output$Actual[order(Output$Actual,decreasing = TRUE)])),
          y = 100*cumsum(Output$Actual[order(Output$Actual,decreasing = TRUE)])/sum(Output$Actual[order(Output$Actual,decreasing = TRUE)]),col="green")
    ##Actual
    lines(x = 100*(1:length(CumnsumY_predict)/length(CumnsumY_predict)),y = 100*CumnsumY_predict/sum(Y_predict),col="red")
    
    
    legend(
      "bottomright", 
      col=c("blue", "red","green"), 
      legend = c("Blue=Predicted","Red=Actual","green=Optimal"),
      cex =0.9,lwd = 2
    )
    # ----------------------------------------------------------------------------------------------------------------------
    # -------------------------------------------------- Area Under the curve ----------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    sum(diff(100*(1:length(CumnsumPredicted))/length(CumnsumPredicted))*(100*CumnsumPredicted/sum(Predicted))[-1])##9129.885
    sum(diff(100*(1:length(CumnsumPredicted))/length(CumnsumPredicted))*(100*CumnsumPredicted/sum(Predicted))[-1])/(100*100)##AUC=0.9129885
    sum(diff(100*(1:length(CumnsumPredicted))/length(CumnsumPredicted))*(100*CumnsumY_predict/sum(Y_predict))[-1])/(100*100)##AUC=0.911627
    
    
    
    #  ---------------------------------------------------------------------------------------------------------------------------------------
    #  ---------------------------------------- Check The Model ------------------------------------------------------------------------------
    #  ---------------------------------------------------------------------------------------------------------------------------------------
    # Check Per Vertical-----------------------------------------------------------------------
    CheckingTheModel<-data.frame()
    for(PrimaryVertical in c("All",unique(as.character(Output$PrimaryVertical)))){
      
      ##Sub Data per Vertical
      if(PrimaryVertical=='All'){
        SubData<-Output
      }else{
        SubData<-Output[which(Output$PrimaryVertical==PrimaryVertical),]
      }
      
      if(nrow(SubData)<=500){
        next()
      }
      
      ##Ranks for Predicted and Actual
      Dictionary<-Ranks_Dictionary((jitter(jitter(SubData$Predict_calibrated,0.0001),0.00001)),100)
      
      #Insert The rank for Actual and Predicted
      # SubData$PredictedRank<-sapply(SubData$Predict_calibrated,FUN = function(x,tab=Dictionary){
      #   tab[which(x>=tab$lag_value & x<tab$value),]$rank
      # })
      SubData$PredictedRank<-Dictionary[findInterval(x = SubData$Predict_calibrated, vec = Dictionary$lag_value,left.open = T),c("rank")]
      
      #Group Mappe
      GroupMappe<-data.frame(as.list(aggregate(cbind(Predict_calibrated,Actual)~PredictedRank,data = SubData,FUN = "mean")))
      colnames(GroupMappe)<-c("Rank","Mean_Predict_calibrated","Mean_Actual")
      GroupMappe<-100*(abs(mean(GroupMappe$Mean_Predict_calibrated)-mean(GroupMappe$Mean_Actual))+1)/(abs(mean(GroupMappe$Mean_Actual))+1)
      
      ##AUC
      OrderPrediction<-order(SubData$Predict_calibrated,decreasing = TRUE)
      Predicted<-SubData$Predict_calibrated[OrderPrediction]
      Y_predict<-SubData$Actual[OrderPrediction]
      
      CumnsumY_predict<-cumsum(Y_predict)
      CumnsumPredicted<-cumsum(Predicted)
      
      ##Full AUC
      AUC_P<-sum(diff(100*(1:length(CumnsumPredicted))/length(CumnsumPredicted))*(100*CumnsumPredicted/sum(Predicted))[-1])/(100*100)##AUC=0.8942668
      AUC_A<-sum(diff(100*(1:length(CumnsumPredicted))/length(CumnsumPredicted))*(100*CumnsumY_predict/sum(Y_predict))[-1])/(100*100)##AUC=0.8639656
      AUC_Optimal<-sum(diff(100*(1:length(SubData$Actual))/length(SubData$Actual))*(100*cumsum(SubData$Actual[order(SubData$Actual,decreasing = TRUE)])/sum(SubData$Actual))[-1])/(100*100)##AUC=0.8639656
      
      ##Partial AUC Predicted
      x<-diff(100*(1:length(CumnsumPredicted))/length(CumnsumPredicted))*(100*CumnsumPredicted/sum(Predicted))[-1]
      
      ##20 percent
      sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
      AUC_P_20<-sum(unlist(sp[[1]]))/(100*100)
      
      ##40 percent
      sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
      AUC_P_40<-sum(unlist(sp[[1]]),sp[[2]])/(100*100)
      
      ##60 percent
      sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
      AUC_P_60<-sum(unlist(sp[[1]]),sp[[2]],sp[[3]])/(100*100)
      
      ##80 percent
      sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
      AUC_P_80<-sum(unlist(sp[[1]]),sp[[2]],sp[[3]],sp[[4]])/(100*100)
      
      
      ##Partial AUC Predicted
      x<-diff(100*(1:length(CumnsumPredicted))/length(CumnsumPredicted))*(100*CumnsumY_predict/sum(Y_predict))[-1]
      
      ##20 percent
      sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
      AUC_A_20<-sum(unlist(sp[[1]]))/(100*100)
      
      ##40 percent
      sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
      AUC_A_40<-sum(unlist(sp[[1]]),sp[[2]])/(100*100)
      
      ##60 percent
      sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
      AUC_A_60<-sum(unlist(sp[[1]]),sp[[2]],sp[[3]])/(100*100)
      
      ##80 percent
      sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
      AUC_A_80<-sum(unlist(sp[[1]]),sp[[2]],sp[[3]],sp[[4]])/(100*100)
      
      ##RBind All
      CheckingTheModel<-rbind(CheckingTheModel,data.frame(PrimaryVertical,
                                                          SumPredicted=round(sum(SubData$Predict_calibrated),4),
                                                          SumActual=round(sum(SubData$Actual),4),
                                                          PercentPreciesment=round(100*sum(SubData$Predict_calibrated)/sum(SubData$Actual),4),
                                                          GroupMappe=round(GroupMappe,4),
                                                          Correlation=round(cor(SubData$Predict_calibrated,SubData$Actual,method = "spearman"),4),
                                                          AUC_P = round(AUC_P,4),
                                                          AUC_A = round(AUC_A,4),
                                                          AUC_Optimal = round(AUC_Optimal,4),
                                                          AUC_A_20 = round(AUC_A_20,4),
                                                          AUC_P_20 = round(AUC_P_20,4),
                                                          AUC_A_40 = round(AUC_A_40,4),
                                                          AUC_P_40 = round(AUC_P_40,4),
                                                          AUC_A_60 = round(AUC_A_60,4),
                                                          AUC_P_60 = round(AUC_P_60,4),
                                                          AUC_A_80 = round(AUC_A_80,4),
                                                          AUC_P_80 = round(AUC_P_80,4),
                                                          PercentActualFromTotal=round(100*sum(SubData$Actual)/sum(Output$Actual),4),
                                                          Lift10=round(100*quantile(cumsum(SubData$Actual[order(SubData$Predict_calibrated,decreasing = TRUE)]),probs = 0.1)/sum(SubData$Actual),4),
                                                          Lift20=round(100*quantile(cumsum(SubData$Actual[order(SubData$Predict_calibrated,decreasing = TRUE)]),probs = 0.2)/sum(SubData$Actual),4),
                                                          Pareto10=round(100*quantile(cumsum(SubData$Actual[order(SubData$Actual,decreasing = TRUE)]),probs = 0.1)/sum(SubData$Actual),4),
                                                          Pareto20=round(100*quantile(cumsum(SubData$Actual[order(SubData$Actual,decreasing = TRUE)]),probs = 0.2)/sum(SubData$Actual),4),
                                                          OptimalLift10=round(100*quantile(cumsum(SubData$Actual[order(SubData$Predict_calibrated,decreasing = TRUE)]),probs = 0.1)/quantile(cumsum(SubData$Actual[order(SubData$Actual,decreasing = TRUE)]),probs = 0.1),4),
                                                          OptimalLift20=round(100*quantile(cumsum(SubData$Actual[order(SubData$Predict_calibrated,decreasing = TRUE)]),probs = 0.2)/quantile(cumsum(SubData$Actual[order(SubData$Actual,decreasing = TRUE)]),probs = 0.2),4)
      ))
    }
    CheckingTheModel<-CheckingTheModel[order(CheckingTheModel$SumPredicted,decreasing = TRUE),]
    row.names(CheckingTheModel)<-NULL
    CheckingTheModel
    
    # --------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------- Export PredictedVsActualOutput--------------------------------
    # --------------------------------------------------------------------------------------------------------------
    ##Production Server
    channel<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
    
    ##The Data to Output
    CheckingTheModel$CurrentDate<-Sys.time()
    
    
    varTypes = c(PrimaryVertical="varchar(140)",
                 SumPredicted="float",
                 SumActual="float",
                 PercentPreciesment="float",
                 GroupMappe="float",
                 Correlation="float",        
                 AUC_P="float",AUC_A="float",AUC_Optimal="float",
                 AUC_A_20="float",AUC_P_20="float",AUC_A_40="float",AUC_P_40="float",AUC_A_60="float",AUC_P_60="float",AUC_A_80="float",AUC_P_80="float",
                 PercentActualFromTotal="float",
                 Lift10="float",Lift20="float",Pareto10="float",Pareto20="float",OptimalLift10="float",OptimalLift20="float",
                 CurrentDate="datetime")
    
    sqlSave(channel,dat = CheckingTheModel,tablename = 'LTV.TrainTestChecking',rownames=F,varTypes = varTypes,append = T)
    
    odbcClose(channel)
    # #  ---------------------------------------------------------------------------------------------------------------------------------------
    # #  ---------------------------------------- Predicted Vs Actual (After Year) -------------------------------------------------------------
    # #  ---------------------------------------------------------------------------------------------------------------------------------------
    # 
    # Query1<-"
    # IF OBJECT_ID('tempdb..#LTV') IS NOT NULL
    # DROP TABLE #LTV
    # 
    # SELECT Account_Holder_ID = LTV.ID,
    # LTV = LTV.PredictDelta,
    # PrimaryVertical = LTV.PrimaryVertical,
    # CurrentDate = LTV.CurrentDate
    # INTO #LTV
    # FROM
    # (
    # SELECT [Output].*,
    # ROW_NUMBER() OVER (PARTITION BY [Output].ID ORDER BY [Output].CurrentDate DESC) AS RowNum
    # FROM [Predict_DB].[dbo].[Output] AS [Output]
    # WHERE CurrentDate <= GETDATE() - 365 AND CurrentDate>=(GETDATE() - 365-120)
    # ) AS LTV
    # WHERE LTV.RowNum = 1;
    # "
    # 
    # Query2<-" 
    # SELECT LTV.Account_Holder_ID,
    # PrimaryVertical = LTV.PrimaryVertical,
    # Predicted = LTV.LTV,
    # Actual = isnull(Rev.Actual,0)
    # FROM #LTV AS LTV
    # LEFT JOIN
    # (
    # SELECT Revenue.Involved_Party_ID,
    # Actual = SUM(Revenue.Revenue_Amount_USD)
    # FROM [DW_Main].[dbo].[syn_active_Fact_Revenue] AS Revenue
    # INNER JOIN #LTV AS LTV
    # ON LTV.Account_Holder_ID = Revenue.Involved_Party_ID
    # WHERE (
    # Transaction_Code != 10103
    # OR Transaction_Code != 10214
    # OR Transaction_Code != 10104
    # OR Transaction_Code != 10105
    # OR Transaction_Code != 10106
    # OR Transaction_Code != 5010
    # OR Transaction_Code != 10107
    # OR Transaction_Code IS NULL
    # )
    # AND Revenue.Involved_Party_Type = 1
    # AND Revenue.Transaction_Datetime BETWEEN LTV.CurrentDate AND LTV.CurrentDate + 365
    # GROUP BY Revenue.Involved_Party_ID
    # ) AS Rev
    # ON Rev.Involved_Party_ID = LTV.Account_Holder_ID;
    # "
    # #channel<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
    # channel<-odbcDriverConnect('SERVER=HRZ-DWHSS-PRD;DRIVER={SQL Server};DATABASE=Dev_Predict_DB;UID=bipredict;PWD=CokeZero!')
    # 
    # PredictedVsActualData<-sqlQuery(channel = channel,query = Query1)
    # PredictedVsActualData<-sqlQuery(channel = channel,query = Query2)
    # odbcClose(channel)
    # 
    # 
    # #aggregate(cbind(Predicted,Actual)~PrimaryVertical,data = PredictedVsActualData,FUN = "mean")
    # 
    # 
    # # Check Per Vertical-----------------------------------------------------------------------
    # PredictedVsActualOutput<-data.frame()
    # for(PrimaryVertical in c("All",unique(as.character(PredictedVsActualData$PrimaryVertical)))){
    #   
    #   ##Sub Data per Vertical
    #   if(PrimaryVertical=='All'){
    #     SubData<-PredictedVsActualData
    #   }else{
    #     SubData<-PredictedVsActualData[which(PredictedVsActualData$PrimaryVertical==PrimaryVertical),]
    #   }
    #   
    #   if(nrow(SubData)<=500){
    #     next()
    #   }
    #   
    #   ##right jitter to data that we will have 10 Ranks
    #   SubData$Jitter_Predict_calibrated<-quantreg::dither(SubData$Predicted,value = 0.0001,type = "right")
    #   SubData$Jitter_Predict_calibrated<-quantreg::dither(SubData$Jitter_Predict_calibrated,value = 0.00001,type = "right")
    #   SubData$Jitter_Predict_calibrated<-quantreg::dither(SubData$Jitter_Predict_calibrated,value = 0.000001,type = "right")
    #   SubData$Jitter_Predict_calibrated<-quantreg::dither(SubData$Jitter_Predict_calibrated,value = 0.0000001,type = "right")
    #   SubData$Jitter_Predict_calibrated<-quantreg::dither(SubData$Jitter_Predict_calibrated,value = 0.00000001,type = "right")
    #   
    #   
    #   ##Ranks for Predicted and Actual
    #   Dictionary<-Ranks_Dictionary(SubData$Predicted,100)
    #   
    #   #Insert The rank for Actual and Predicted
    #   SubData$PredictedRank<-sapply(SubData$Predicted,FUN = function(x,tab=Dictionary){
    #     tab[which(x>=tab$lag_value & x<tab$value),]$rank
    #   })
    #   
    #   #Group Mappe
    #   GroupMappe<-data.frame(as.list(aggregate(cbind(Predicted,Actual)~PredictedRank,data = SubData,FUN = "mean")))
    #   colnames(GroupMappe)<-c("Rank","Mean_Predict_calibrated","Mean_Actual")
    #   GroupMappe<-100*(abs(mean(GroupMappe$Mean_Predict_calibrated)-mean(GroupMappe$Mean_Actual))+1)/(abs(mean(GroupMappe$Mean_Actual))+1)
    #   
    #   ##AUC
    #   OrderPrediction<-order(SubData$Predicted,decreasing = TRUE)
    #   Predicted<-SubData$Predicted[OrderPrediction]
    #   Y_predict<-SubData$Actual[OrderPrediction]
    #   
    #   CumnsumY_predict<-cumsum(Y_predict)
    #   CumnsumPredicted<-cumsum(Predicted)
    #   
    #   ##Full AUC
    #   AUC_P<-sum(diff(100*(1:length(CumnsumPredicted))/length(CumnsumPredicted))*(100*CumnsumPredicted/sum(Predicted))[-1])/(100*100)##AUC=0.8942668
    #   AUC_A<-sum(diff(100*(1:length(CumnsumPredicted))/length(CumnsumPredicted))*(100*CumnsumY_predict/sum(Y_predict))[-1])/(100*100)##AUC=0.8639656
    #   AUC_Optimal<-sum(diff(100*(1:length(SubData$Actual))/length(SubData$Actual))*(100*cumsum(SubData$Actual[order(SubData$Actual,decreasing = TRUE)])/sum(SubData$Actual))[-1])/(100*100)##AUC=0.8639656
    #   
    #   ##Partial AUC Predicted
    #   x<-diff(100*(1:length(CumnsumPredicted))/length(CumnsumPredicted))*(100*CumnsumPredicted/sum(Predicted))[-1]
    #   
    #   ##20 percent
    #   sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
    #   AUC_P_20<-sum(unlist(sp[[1]]))/(100*100)
    #   
    #   ##40 percent
    #   sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
    #   AUC_P_40<-sum(unlist(sp[[1]]),sp[[2]])/(100*100)
    #   
    #   ##60 percent
    #   sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
    #   AUC_P_60<-sum(unlist(sp[[1]]),sp[[2]],sp[[3]])/(100*100)
    #   
    #   ##80 percent
    #   sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
    #   AUC_P_80<-sum(unlist(sp[[1]]),sp[[2]],sp[[3]],sp[[4]])/(100*100)
    #   
    #   
    #   ##Partial AUC Predicted
    #   x<-diff(100*(1:length(CumnsumPredicted))/length(CumnsumPredicted))*(100*CumnsumY_predict/sum(Y_predict))[-1]
    #   
    #   ##20 percent
    #   sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
    #   AUC_A_20<-sum(unlist(sp[[1]]))/(100*100)
    #   
    #   ##40 percent
    #   sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
    #   AUC_A_40<-sum(unlist(sp[[1]]),sp[[2]])/(100*100)
    #   
    #   ##60 percent
    #   sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
    #   AUC_A_60<-sum(unlist(sp[[1]]),sp[[2]],sp[[3]])/(100*100)
    #   
    #   ##80 percent
    #   sp<-split(x, floor(5 * seq.int(0, length(x) - 1) / length(x)))
    #   AUC_A_80<-sum(unlist(sp[[1]]),sp[[2]],sp[[3]],sp[[4]])/(100*100)
    #   
    #   ##RBind All
    #   PredictedVsActualOutput<-rbind(PredictedVsActualOutput,data.frame(PrimaryVertical,
    #                                                                     SumPredicted=round(sum(SubData$Predicted),4),
    #                                                                     SumActual=round(sum(SubData$Actual),4),
    #                                                                     PercentPreciesment=round(100*sum(SubData$Predicted)/sum(SubData$Actual),4),
    #                                                                     GroupMappe=round(GroupMappe,4),
    #                                                                     Correlation=round(cor(SubData$Predicted,SubData$Actual,method = "spearman"),4),
    #                                                                     AUC_P = round(AUC_P,4),
    #                                                                     AUC_A = round(AUC_A,4),
    #                                                                     AUC_Optimal = round(AUC_Optimal,4),
    #                                                                     AUC_A_20 = round(AUC_A_20,4),
    #                                                                     AUC_P_20 = round(AUC_P_20,4),
    #                                                                     AUC_A_40 = round(AUC_A_40,4),
    #                                                                     AUC_P_40 = round(AUC_P_40,4),
    #                                                                     AUC_A_60 = round(AUC_A_60,4),
    #                                                                     AUC_P_60 = round(AUC_P_60,4),
    #                                                                     AUC_A_80 = round(AUC_A_80,4),
    #                                                                     AUC_P_80 = round(AUC_P_80,4),
    #                                                                     PercentActualFromTotal=round(100*sum(SubData$Actual)/sum(PredictedVsActualData$Actual),4),
    #                                                                     Lift10=round(100*quantile(cumsum(SubData$Actual[order(SubData$Predicted,decreasing = TRUE)]),probs = 0.1)/sum(SubData$Actual),4),
    #                                                                     Lift20=round(100*quantile(cumsum(SubData$Actual[order(SubData$Predicted,decreasing = TRUE)]),probs = 0.2)/sum(SubData$Actual),4),
    #                                                                     Pareto10=round(100*quantile(cumsum(SubData$Actual[order(SubData$Actual,decreasing = TRUE)]),probs = 0.1)/sum(SubData$Actual),4),
    #                                                                     Pareto20=round(100*quantile(cumsum(SubData$Actual[order(SubData$Actual,decreasing = TRUE)]),probs = 0.2)/sum(SubData$Actual),4),
    #                                                                     OptimalLift10=round(100*quantile(cumsum(SubData$Actual[order(SubData$Predicted,decreasing = TRUE)]),probs = 0.1)/quantile(cumsum(SubData$Actual[order(SubData$Actual,decreasing = TRUE)]),probs = 0.1),4),
    #                                                                     OptimalLift20=round(100*quantile(cumsum(SubData$Actual[order(SubData$Predicted,decreasing = TRUE)]),probs = 0.2)/quantile(cumsum(SubData$Actual[order(SubData$Actual,decreasing = TRUE)]),probs = 0.2),4)
    #   ))
    # }
    # PredictedVsActualOutput<-PredictedVsActualOutput[order(PredictedVsActualOutput$SumPredicted,decreasing = TRUE),]
    # row.names(PredictedVsActualOutput)<-NULL
    # 
    # 
    # PredictedVsActualOutput$CurrentDate<-Sys.time()
    
    
    #successful Alerts -------------------------------------------------------
    Alerts_conn<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
    QueryString<-"INSERT INTO [Predict_DB].[dbo].Alerts (MasterModel, Model, Step,Description,Successful,DateRunnigTheStep)
    VALUES ('LTV', 'LTV - New Script', 'Checking the Models - LTV New Script', 'Checking the Models ran successfully',1,'2017-01-02 00:00:00.000');"
    sqlQuery(channel = Alerts_conn,
             query = gsub(x = QueryString,pattern = '2017-01-02 00:00:00.000',replacement = as.character.Date(Sys.time())))
    odbcClose(Alerts_conn)
  }
  ,error=function(err){
    # Not successful Alerts -------------------------------------------------------
    Alerts_conn<-odbcDriverConnect('SERVER=localhost;DRIVER={SQL Server};DATABASE=Predict_DB;trusted_connection=true')
    QueryString<-"INSERT INTO [Predict_DB].[dbo].Alerts (MasterModel, Model, Step,Description,Successful,DateRunnigTheStep)
    VALUES ('LTV', 'LTV - New Script', 'Checking the Models - LTV New Script', 'Checking the Models not ran successfully',0,'2017-01-02 00:00:00.000');"
    sqlQuery(channel = Alerts_conn,
             query = gsub(x = QueryString,pattern = '2017-01-02 00:00:00.000',replacement = as.character.Date(Sys.time())))
    odbcClose(Alerts_conn)
  }
)