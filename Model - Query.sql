USE [Dev_Predict_DB]
GO
/****** Object:  StoredProcedure [LTV].[Build_DataPanel_Model]    Script Date: 10/25/2021 10:46:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [LTV].[Build_DataPanel_Model]
AS
    BEGIN

/*	
        DECLARE @CutPoint DATETIME;
        SET @CutPoint = '2016-10-01 04:37:16.590';
 --( SELECT    GETDATE() - 365);
        DECLARE @LTVDaysToPredict INT; 
        SET @LTVDaysToPredict = 365; 
        DECLARE @Season INT;
        SET @Season = 18;
*/

        /******************************************************************Delete All Previous Data Panels******************************************************************/
        TRUNCATE TABLE [Dev_Predict_DB].[LTV].[DataPanel_Model]
		
        /******************************************************************DataPanel 1******************************************************************/

        DECLARE @CutPoint DATETIME;
        SET @CutPoint = ( SELECT    GETDATE() - 1 * 365
                        );--'2016-10-01 00:00:00.000'
						        DECLARE @Season INT;
        SET @Season = 1;
        DECLARE @ActivityDays INT; 
        SET @ActivityDays = 210; 


        EXECUTE [Dev_Predict_DB].[LTV].[Build_DataPanel_Sample_Model] @CutPoint,@Season, @ActivityDays ;


        /******************************************************************DataPanel 2******************************************************************/
        DECLARE @CutPoint2 DATETIME;
        SET @CutPoint2 = ( SELECT   GETDATE() - 1 * 365 - 30
                         );--'2016-10-01 00:00:00.000'
						         DECLARE @Season2 INT;
        SET @Season2 = 2;
        DECLARE @ActivityDays2 INT; 
        SET @ActivityDays2 = 210; 

		
        EXECUTE [Dev_Predict_DB].[LTV].[Build_DataPanel_Sample_Model] @CutPoint2, @Season2,@ActivityDays2;
		

	/******************************************************************DataPanel 3******************************************************************/
        DECLARE @CutPoint3 DATETIME;
        SET @CutPoint3 = ( SELECT   GETDATE() - 1 * 365 - 60
                         );--'2016-10-01 00:00:00.000'
						         DECLARE @Season3 INT;
        SET @Season3 = 3;
        DECLARE @ActivityDays3 INT; 
        SET @ActivityDays3 = 210; 

		
        EXECUTE [Dev_Predict_DB].[LTV].[Build_DataPanel_Sample_Model] @CutPoint3, @Season3,@ActivityDays3;
		
		/******************************************************************DataPanel 4*****************************************************************
        DECLARE @CutPoint4 DATETIME;
        SET @CutPoint4 = ( SELECT   GETDATE() - 1 * 365 - 90
                         );--'2016-10-01 00:00:00.000'
        DECLARE @ActivityDays4 INT; 
        SET @ActivityDays4 = 1 * 365; 
        DECLARE @Season4 INT;
        SET @Season4 = 4;
		
        EXECUTE [Dev_Predict_DB].[dbo].[NewProcedureLTV_Build_DataPanel_Sample_Model] @CutPoint4, @ActivityDays4, @Season4;
		*/
        EXEC [Dev_Predict_DB].[Infra].[Data_Panel_Split_Train_Test] @TableName = '[Dev_Predict_DB].[LTV].[DataPanel_Model]',
            @PartitionByColumn = 'PrimaryVertical', @TrainPCT = 0.70;
		
    END;

