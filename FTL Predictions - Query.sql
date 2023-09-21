USE [Dev_Predict_DB];
GO
/****** Object:  StoredProcedure [LTV].[Build_DataPanel_Sample_Model]    Script Date: 10/25/2021 7:46:04 AM ******/
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

ALTER PROCEDURE [LTV].[Build_DataPanel_FTL_Predictions]
AS
BEGIN

    /* CutPoint */
    DECLARE @CutPoint DATETIME;
    SELECT @CutPoint = GETDATE();

    /* Season */
    DECLARE @Season INT;
    SET @Season = 0;

    /* Load at least one time before @CutPoint */
    DECLARE @ActivityDays INT;
    SET @ActivityDays = 210;

    /**************************************************View_AH_Reg_To_FTL***********************************************/

    IF OBJECT_ID('tempdb..#View_AH_Reg_To_FTL') IS NOT NULL
        DROP TABLE #View_AH_Reg_To_FTL;
    SELECT *
    INTO #View_AH_Reg_To_FTL
    FROM [DW_Main].dbo.View_AH_Reg_To_FTL;
    CREATE CLUSTERED INDEX a ON #View_AH_Reg_To_FTL (AH_ID);


    /************************************************** Volume *****************************************************/
    /************************************ Volume Agregate***********************************************/
    --USE [Dev_Predict_DB]


    IF OBJECT_ID('tempdb..#VolumeAgregate') IS NOT NULL
        DROP TABLE #VolumeAgregate;

    SELECT Account_Holder_ID = Volume.Payee_Involved_Party_ID,
           LastLoadDate = MAX(Volume.Transaction_Datetime),
           HoursBetweenLastLoadToCutPoint = MIN(DATEDIFF(HOUR, Volume.Transaction_Datetime, @CutPoint)),
           HoursBetweenFirstLoadToCutPoint_InVolumeDays = MAX(DATEDIFF(HOUR, Volume.Transaction_Datetime, @CutPoint)),
           SumVolume_InVolumeDays = SUM(Volume.Volume_Amount_USD),
           SdVolume_InVolumeDays = STDEV(Volume.Volume_Amount_USD),
           GeometricMeanVolume_InVolumeDays = EXP(AVG(LOG((Volume.Volume_Amount_USD + 1)))),
           CV_InVolumeDays = ISNULL(STDEV(Volume.Volume_Amount_USD) / (ABS(AVG(Volume.Volume_Amount_USD)) + 1), -1),
           MaxVolume_InVolumeDays = MAX(Volume.Volume_Amount_USD),
           SumOfCrossCountryVolume_InVolumeDays = SUM(   CASE
                                                             WHEN Volume.Payee_Billing_Country_ID <> Volume.Payer_Billing_Country_ID THEN
                                                                 1
                                                             ELSE
                                                                 0
                                                         END
                                                     ),
           NumberOfLoads_InVolumeDays = COUNT(Volume.Volume_Amount_USD),
           DistinctMonthsActivity_InVolumeDays = COUNT(DISTINCT (MONTH(Volume.Transaction_Datetime) + ' '
                                                                 + YEAR(Volume.Transaction_Datetime)
                                                                )
                                                      ),
           BillingServices_InVolumeDays = SUM(   CASE
                                                     WHEN VolumeTypes.Volume_Type_Category = 'Billing Services' THEN
                                                         1
                                                     ELSE
                                                         0
                                                 END
                                             ),
           MAP_InVolumeDays = SUM(   CASE
                                         WHEN VolumeTypes.Volume_Type_Category = 'Make A Payment' THEN
                                             1
                                         ELSE
                                             0
                                     END
                                 ),
           MassPayout_InVolumeDays = SUM(   CASE
                                                WHEN VolumeTypes.Volume_Type_Category = 'Mass Payout' THEN
                                                    1
                                                ELSE
                                                    0
                                            END
                                        ),
           PaymentService_InVolumeDays = SUM(   CASE
                                                    WHEN VolumeTypes.Volume_Type_Category = 'Payment Services' THEN
                                                        1
                                                    ELSE
                                                        0
                                                END
                                            ),
           Private_InVolumeDays = SUM(   CASE
                                             WHEN VolumeTypes.Volume_Type_Category = 'Private' THEN
                                                 1
                                             ELSE
                                                 0
                                         END
                                     ),
           Unknown_InVolumeDays = SUM(   CASE
                                             WHEN VolumeTypes.Volume_Type_Category = 'Unknown' THEN
                                                 1
                                             ELSE
                                                 0
                                         END
                                     ),
           SumVolumeMap_InVolumeDays = SUM(   CASE
                                                  WHEN Volume.In_Network_Payment = 1 THEN
                                                      Volume.Volume_Amount_USD
                                                  ELSE
                                                      0
                                              END
                                          ),
           NumberOfVolumeMap_InVolumeDays = SUM(Volume.In_Network_Payment),
           SumVolumePaymentRequest_InVolumeDays = SUM(   CASE
                                                             WHEN Volume.Payment_Request_ID IS NOT NULL THEN
                                                                 Volume.Volume_Amount_USD
                                                             ELSE
                                                                 0
                                                         END
                                                     ),
           NumberOfVolumePaymentRequest_InVolumeDays = SUM(   CASE
                                                                  WHEN Volume.In_Network_Payment IS NOT NULL THEN
                                                                      1
                                                                  ELSE
                                                                      0
                                                              END
                                                          )
    INTO #VolumeAgregate
    FROM [DW_Main].[dbo].syn_active_Fact_Volume AS Volume

        /*LTV Population*/
        INNER JOIN #View_AH_Reg_To_FTL AS View_AH_Reg_To_FTL
            ON View_AH_Reg_To_FTL.AH_ID = Volume.Payee_Involved_Party_ID

        /*Transaction Categories*/
        LEFT JOIN [DW_Main].[dbo].Ref_Transaction_Categories AS Ref_Transaction_Categories
            ON Volume.Transaction_Status = Ref_Transaction_Categories.StatusCode

        /* Dim Volume Types */
        LEFT JOIN [DW_Main].[dbo].Dim_Volume_Types AS VolumeTypes
            ON Volume.Volume_Type_ID = VolumeTypes.Volume_Type_ID
    WHERE Volume.Volume_Amount_USD > 1
          AND Volume.Payee_Involved_Party_Type = 1
          AND Ref_Transaction_Categories.StatusCategoryCode = 2
          AND Volume.Transaction_Datetime <= @CutPoint
          AND DATEDIFF(DAY, Volume.Transaction_Datetime, @CutPoint) >= 0
          AND DATEDIFF(DAY, Volume.Transaction_Datetime, @CutPoint) <= @ActivityDays
    GROUP BY Volume.Payee_Involved_Party_ID;

    CREATE CLUSTERED INDEX a ON #VolumeAgregate (Account_Holder_ID);


    /************************************************** Base View *****************************************************/

    IF OBJECT_ID('tempdb..#BaseView') IS NOT NULL
        DROP TABLE #BaseView;

    SELECT Account_Holder_ID = InvolvedPartiesSummary.Involved_Party_ID,
           Account_Holder_Language = ISNULL(Language.Language_Name, 'Other'),
           MasterAccount = InvolvedParties.Master_Account_Holder_ID,
           FTL_Date = InvolvedPartiesSummary.First_Transaction_Received_Date,
           RegistrationCompletionDateTime = InvolvedParties.Registration_Completion_DateTime,
           Hours_From_RegComp_to_FTL = ISNULL(
                                                 DATEDIFF(
                                                             HOUR,
                                                             InvolvedParties.Registration_Completion_DateTime,
                                                             InvolvedPartiesSummary.First_Transaction_Received_Date
                                                         ),
                                                 -1
                                             ),
           Hours_From_RegStarted_to_FTL = ISNULL(
                                                    DATEDIFF(
                                                                HOUR,
                                                                InvolvedParties.Registration_DateTime,
                                                                InvolvedPartiesSummary.First_Transaction_Received_Date
                                                            ),
                                                    -1
                                                ),
           Minutes_RegStarted_to_Complete = ISNULL(
                                                      DATEDIFF(
                                                                  MINUTE,
                                                                  InvolvedParties.Registration_DateTime,
                                                                  InvolvedParties.Registration_Completion_DateTime
                                                              ),
                                                      -1
                                                  ),
           MainDomain = LOWER(   CASE
                                     WHEN CHARINDEX(
                                                       '.',
                                                       RIGHT(InvolvedParties.Email, LEN(InvolvedParties.Email)
                                                                                    - CHARINDEX(
                                                                                                   '@',
                                                                                                   InvolvedParties.Email
                                                                                               ))
                                                   ) > 1 THEN
                                         LEFT(RIGHT(InvolvedParties.Email, LEN(InvolvedParties.Email)
                                                                           - CHARINDEX('@', InvolvedParties.Email)),
                                         (
                                             SELECT CHARINDEX(
                                                                 '.',
                                                                 RIGHT(InvolvedParties.Email, LEN(InvolvedParties.Email)
                                                                                              - CHARINDEX(
                                                                                                             '@',
                                                                                                             InvolvedParties.Email
                                                                                                         ))
                                                             ) - 1
                                         ))
                                     ELSE
                                         ''
                                 END
                             ),
           EmailExtension = LOWER(SUBSTRING(
                                               InvolvedParties.Email,
                                               CHARINDEX(
                                                            '.',
                                                            InvolvedParties.Email,
                                                            CHARINDEX('@', InvolvedParties.Email)
                                                        ) + 1,
                                               LEN(InvolvedParties.Email)
                                           )
                                 ),                                                           --new variable
           Country_Name = ISNULL(BillingCountry.Country_Name, 'Other'),
           Nationality_Country = ISNULL(LOWER(InvolvedParties.Nationality_Country), 'Other'),
           City = ISNULL(LOWER(InvolvedParties.Billing_City), 'Other'),
           Date_of_Birth = InvolvedParties.Date_of_Birth,
           Age = ISNULL(   CASE
                               WHEN DATEDIFF(YEAR, InvolvedParties.Date_of_Birth, @CutPoint) >= 120 THEN
                                   -1
                               ELSE
                                   DATEDIFF(YEAR, InvolvedParties.Date_of_Birth, @CutPoint)
                           END,
                           -1
                       ),
           RegistrationCompletionMonth = ISNULL(MONTH(InvolvedParties.Registration_Completion_DateTime), 0),
           RegistrationCompletionWeekday = ISNULL(
                                                     DATENAME(WEEKDAY, InvolvedParties.Registration_Completion_DateTime),
                                                     'Unkown'
                                                 ),
           FTL_Month = ISNULL(MONTH(InvolvedPartiesSummary.First_Transaction_Received_Date), 0),
           FTL_Weekday = ISNULL(DATENAME(WEEKDAY, InvolvedPartiesSummary.First_Transaction_Received_Date), 'Unkown'),
           Seniority = ISNULL(DATEDIFF(DAY, InvolvedPartiesSummary.First_Transaction_Received_Date, @CutPoint), -1),
           TimeBetweenRegistrationCompleteToCutPoint = ISNULL(
                                                                 DATEDIFF(
                                                                             DAY,
                                                                             InvolvedParties.Registration_Completion_Date,
                                                                             @CutPoint
                                                                         ),
                                                                 -1
                                                             ),
           UTM_Source = ISNULL(LOWER(InvolvedParties.UTM_Source), 'NULL'),
           UTM_Medium = ISNULL(LOWER(InvolvedParties.UTM_Medium), 'NULL'),
           UTM_Campaign = ISNULL(InvolvedParties.UTM_Campaign, 'NULL_Source'),
           UTM = ISNULL(LOWER(UTM.UTM_Activity_Type_Description), 'Other'),
           First_TM = ISNULL(TM_Flow.Registration_Transaction_Method, 'NULL'),
           First_TM_Flow = ISNULL(TM_Flow.Registration_Transaction_Method_Flow, 'NULL'),
           Account_Holder_Reg_Program = ISNULL(CorporateVerticals.Generic_Corporate_Name, 'Other'),
           RegistrationVertical = ISNULL(CorporateVerticals.Vertical_Name, 'Other'),
           IsCoreVertical = CorporateVerticals.Is_Core_Vertical,
           PrimaryPhoneCountry = ISNULL(InvolvedParties.Primary_Phone_Country, 'Other'),      --new variable
           PhoneTypes = ISNULL(PhoneTypes.Description, 'Other'),                              --new variable
           FirstSecurityQuestions = ISNULL(InvolvedParties.First_Security_Question_ID, -1),   --new variable
           SecondSecurityQuestions = ISNULL(InvolvedParties.Second_Security_Question_ID, -1), --new variable
           ThirdSecurityQuestions = ISNULL(InvolvedParties.Third_Security_Question_ID, -1)    --new variable
    INTO #BaseView
    FROM DW_Main.dbo.syn_active_Dim_Involved_Parties_Summary AS InvolvedPartiesSummary

        /* LTV population */
        INNER JOIN #View_AH_Reg_To_FTL AS View_AH_Reg_To_FTL
            ON InvolvedPartiesSummary.Involved_Party_ID = View_AH_Reg_To_FTL.AH_ID
               AND InvolvedPartiesSummary.Involved_Party_Type = 1

        /*Involved Parties*/
        LEFT JOIN DW_Main.dbo.syn_active_Dim_Involved_Parties AS InvolvedParties
            ON InvolvedPartiesSummary.Involved_Party_ID = InvolvedParties.Involved_Party_ID
               AND InvolvedPartiesSummary.Involved_Party_Type = InvolvedParties.Involved_Party_Type

        /*Billing Country*/
        LEFT JOIN DW_Main.dbo.Dim_Countries AS BillingCountry
            ON InvolvedParties.Billing_Country = BillingCountry.ISO2

        /*Languages*/
        LEFT JOIN [DW_Main].[dbo].[Ref_Language_Codes] AS [Language]
            ON InvolvedParties.Preferred_Language = [Language].Language_Code

        /*TM Flow*/
        LEFT JOIN DW_Main.dbo.Dim_Registration_Transaction_Method_Flows AS TM_Flow
            ON InvolvedParties.Registration_Transaction_Method_Flow_ID = TM_Flow.Registration_Transaction_Method_Flow_ID

        /*UTM*/
        LEFT JOIN [DW_Main].dbo.Dim_UTM_Activity_Types AS UTM
            ON InvolvedParties.UTM_Activity_Type_ID = UTM.UTM_Activity_Type_ID

        /*Corporate Vertical*/
        LEFT JOIN DW_Main.dbo.Dim_Corporate_Verticals AS CorporateVerticals
            ON InvolvedParties.Registration_Partner_ID = CorporateVerticals.Corporate_ID
               AND CorporateVerticals.Corporate_Type = 2 -- Corporate_Type=2 - Partner

        /*Volume Agregate*/
        INNER JOIN #VolumeAgregate AS VolumeAgregate
            ON InvolvedPartiesSummary.Involved_Party_ID = VolumeAgregate.Account_Holder_ID

        /*Phone Types*/
        LEFT JOIN DW_Main.dbo.Ref_Phone_Types AS PhoneTypes
            ON InvolvedParties.Primary_Phone_Type = PhoneTypes.Phone_Type_ID
    WHERE InvolvedPartiesSummary.Involved_Party_Type = 1 --Only Account Holders
          AND InvolvedParties.Is_Registered_Receiver = 1 --Only Accounts that can receive money
          AND InvolvedPartiesSummary.First_Transaction_Received_Date IS NOT NULL
          --AND InvolvedParties.Registration_Completion_Date >= @CutPoint - 180
          AND InvolvedParties.Registration_Completion_Date <= @CutPoint
          AND DATEDIFF(DAY, InvolvedPartiesSummary.First_Transaction_Received_Date, @CutPoint) <= 90 --no need to give predictions for AH that did their FTL 20 years ago
          AND NOT EXISTS
    (
        SELECT 1
        FROM [Predict_DB].[dbo].[Output] AS [Output]
        WHERE View_AH_Reg_To_FTL.AH_ID = [Output].ID
    ); --never got LTV Prediction


    CREATE CLUSTERED INDEX a ON #BaseView (Account_Holder_ID);

    --SELECT * FROM DW_Main.dbo.Dim_Corporate_Verticals--331635
    --SELECT Involved_Party_Type,* FROM DW_Main.dbo.syn_active_Dim_Involved_Parties WHERE Involved_Party_Type<>1
    /************************************ Value Segment***********************************************/
    IF OBJECT_ID('tempdb..#AHSegment') IS NOT NULL
        DROP TABLE #AHSegment;

    SELECT Account_Holder_ID = Volume.Payee_Involved_Party_ID,
           ValueSegment = ISNULL(MAX(CAST(Volume.Payee_Value_Segemnt AS INT)), -1),
           Tier = ISNULL(MAX(CAST(Volume.Payee_Involved_Party_Tier AS INT)), -1)
    INTO #AHSegment
    FROM [DW_Main].[dbo].syn_active_Fact_Volume AS Volume

        /*Volume Aggregate*/
        INNER JOIN #VolumeAgregate AS VolumeAgregate
            ON Volume.Payee_Involved_Party_ID = VolumeAgregate.Account_Holder_ID
               AND Volume.Transaction_Datetime = VolumeAgregate.LastLoadDate
               AND Volume.Payee_Involved_Party_Type = 1

        /*Transaction Categories*/
        LEFT JOIN [DW_Main].[dbo].Ref_Transaction_Categories AS Ref_Transaction_Categories
            ON Volume.Transaction_Status = Ref_Transaction_Categories.StatusCode
    WHERE Volume.Volume_Amount_USD > 1
          AND Volume.Payee_Involved_Party_Type = 1
          AND Ref_Transaction_Categories.StatusCategoryCode = 2
          AND Volume.Transaction_Datetime <= VolumeAgregate.LastLoadDate
          AND DATEDIFF(DAY, Volume.Transaction_Datetime, VolumeAgregate.LastLoadDate) >= 0
          AND DATEDIFF(DAY, Volume.Transaction_Datetime, VolumeAgregate.LastLoadDate) <= 15
    GROUP BY Volume.Payee_Involved_Party_ID;

    /**************************************************Volume***********************************************/
    IF OBJECT_ID('tempdb..#Volume') IS NOT NULL
        DROP TABLE #Volume;

    SELECT Account_Holder_ID = Volume.Payee_Involved_Party_ID,
           Transaction_Datetime = Volume.Transaction_Datetime,
           LastLoadDate = VolumeAgregate.LastLoadDate,
           Volume_Amount_USD = Volume.Volume_Amount_USD,
           Vertical = CASE
                          WHEN InvolvedParties.VIP_Tier > 0
                               AND
                               (
                                   BillingCountry.Country_Name = 'China'
                                   OR BillingCountry.Country_Name = 'Hong Kong (SAR)'
                               ) THEN
                              'VIP - Greater China'
                          WHEN InvolvedParties.VIP_Tier > 0
                               AND
                               (
                                   BillingCountry.Country_Name <> 'China'
                                   AND BillingCountry.Country_Name <> 'Hong Kong (SAR)'
                               ) THEN
                              'VIP - Non China'
                          WHEN CorporateVerticals.Vertical_Name = 'Sellers'
                               AND
                               (
                                   BillingCountry.Country_Name = 'China'
                                   OR BillingCountry.Country_Name = 'Hong Kong (SAR)'
                               ) THEN
                              'Sellers - Greater China'
                          WHEN CorporateVerticals.Vertical_Name = 'Sellers'
                               AND
                               (
                                   BillingCountry.Country_Name <> 'China'
                                   AND BillingCountry.Country_Name <> 'Hong Kong (SAR)'
                               ) THEN
                              'Sellers - Non China'
                          WHEN CorporateVerticals.Vertical_Name IN ( 'Ads Affiliates', 'Traffic Buyers' ) THEN
                              'Affiliates'
                          ELSE
                              CorporateVerticals.Vertical_Name
                      END,
           Country_Name = ISNULL(BillingCountry.Country_Name, 'Other'),
           LoaderName = CorporateVerticals.Corporate_Name,
           Volume_Type_ID = Volume.Volume_Type_ID,
           Payee_Billing_Country_ID = Volume.Payee_Billing_Country_ID,
           Payer_Billing_Country_ID = Volume.Payer_Billing_Country_ID,
           In_Network_Payment = Volume.In_Network_Payment,
           Payment_Request_ID = Volume.Payment_Request_ID
    INTO #Volume
    FROM [DW_Main].[dbo].syn_active_Fact_Volume AS Volume

        /*BaseView*/
        INNER JOIN #BaseView AS BaseView
            ON Volume.Payee_Involved_Party_ID = BaseView.Account_Holder_ID

        /*Volume Population*/
        INNER JOIN #VolumeAgregate AS VolumeAgregate
            ON Volume.Payee_Involved_Party_ID = VolumeAgregate.Account_Holder_ID

        /*Corporate Verticals*/
        LEFT JOIN [DW_Main].[dbo].Dim_Corporate_Verticals AS CorporateVerticals
            ON Volume.Payer_Involved_Party_Type = CorporateVerticals.Corporate_Type
               AND Volume.Payer_Corporate_ID = CorporateVerticals.Corporate_ID

        /*Transaction Status*/
        LEFT JOIN [DW_Main].[dbo].Ref_Transaction_Categories AS Ref_Transaction_Categories
            ON Volume.Transaction_Status = Ref_Transaction_Categories.StatusCode

        /*Involved Parties*/
        LEFT JOIN [DW_Main].[dbo].syn_active_Dim_Involved_Parties AS InvolvedParties
            ON Volume.Payee_Involved_Party_ID = InvolvedParties.Involved_Party_ID
               AND Volume.Payee_Involved_Party_Type = InvolvedParties.Involved_Party_Type

        /*Billing Country*/
        LEFT JOIN DW_Main.dbo.Dim_Countries AS BillingCountry
            ON BillingCountry.ISO2 = InvolvedParties.Billing_Country
    WHERE Volume.Volume_Amount_USD > 1
          AND Volume.Payee_Involved_Party_Type = 1
          AND Ref_Transaction_Categories.StatusCategoryCode = 2
          AND Volume.Transaction_Datetime <= VolumeAgregate.LastLoadDate;

    CREATE CLUSTERED COLUMNSTORE INDEX a ON #Volume;



    /*First 24 Hours Volume*/
    IF OBJECT_ID('tempdb..#First24HVolume') IS NOT NULL
        DROP TABLE #First24HVolume;

    SELECT Volume.Account_Holder_ID,
           NumberOfLoadsInFirst24H = COUNT(Volume.Account_Holder_ID),
           VolumeAmountInFirst24H = SUM(Volume.Volume_Amount_USD)
    INTO #First24HVolume
    FROM #Volume AS Volume

        /* Base View*/
        LEFT JOIN #BaseView AS BaseView
            ON Volume.Account_Holder_ID = BaseView.Account_Holder_ID
    WHERE BaseView.FTL_Date <= Volume.Transaction_Datetime
          AND DATEDIFF(HOUR, BaseView.FTL_Date, Volume.Transaction_Datetime) <= 24
    GROUP BY Volume.Account_Holder_ID;



    /*Volume - Aggregation Until Now*/
    IF OBJECT_ID('tempdb..#VolumeUntilNow') IS NOT NULL
        DROP TABLE #VolumeUntilNow;

    SELECT Account_Holder_ID = Volume.Account_Holder_ID,
           DaysBetweenFirstLoadToCutPoint_UntilNow = MAX(DATEDIFF(DAY, Volume.Transaction_Datetime, @CutPoint)),
           SumVolume_UntilNow = SUM(Volume.Volume_Amount_USD),
           SdVolume_UntilNow = STDEV(Volume.Volume_Amount_USD),
           GeometricMeanVolume_UntilNow = EXP(AVG(LOG((ABS(Volume.Volume_Amount_USD) + 1)))),
           CV_UntilNow = ISNULL(STDEV(Volume.Volume_Amount_USD) / (ABS(AVG(Volume.Volume_Amount_USD)) + 1), -1),
           MaxVolume_UntilNow = MAX(Volume.Volume_Amount_USD),
           SumOfCrossCountryVolume_UntilNow = SUM(   CASE
                                                         WHEN Volume.Payee_Billing_Country_ID <> Volume.Payer_Billing_Country_ID THEN
                                                             1
                                                         ELSE
                                                             0
                                                     END
                                                 ),
           NumberOfLoads_UntilNow = COUNT(Volume.Volume_Amount_USD),
           DistinctMonthsActivity_UntilNow = COUNT(DISTINCT (MONTH(Volume.Transaction_Datetime) + ' '
                                                             + YEAR(Volume.Transaction_Datetime)
                                                            )
                                                  ),
           SumVolumeMap_UntilNow = SUM(   CASE
                                              WHEN Volume.In_Network_Payment = 1 THEN
                                                  Volume.Volume_Amount_USD
                                              ELSE
                                                  0
                                          END
                                      ),
           NumberOfVolumeMap_UntilNow = SUM(Volume.In_Network_Payment),
           SumVolumePaymentRequest_UntilNow = SUM(   CASE
                                                         WHEN Volume.Payment_Request_ID IS NOT NULL THEN
                                                             Volume.Volume_Amount_USD
                                                         ELSE
                                                             0
                                                     END
                                                 ),
           NumberOfVolumePaymentRequest_UntilNow = SUM(   CASE
                                                              WHEN Volume.In_Network_Payment IS NOT NULL THEN
                                                                  1
                                                              ELSE
                                                                  0
                                                          END
                                                      )
    INTO #VolumeUntilNow
    FROM #Volume AS Volume
    WHERE Volume.Transaction_Datetime <= Volume.LastLoadDate
    GROUP BY Volume.Account_Holder_ID;



    /*Volume - Last 30 days*/
    IF OBJECT_ID('tempdb..#Volume1') IS NOT NULL
        DROP TABLE #Volume1;

    SELECT Account_Holder_ID = Volume.Account_Holder_ID,
           NumberOfLoads_Volume1 = COUNT(Transaction_Datetime),
           SumVolume_Volume1 = SUM(Volume_Amount_USD),
           SumVolumeMAP_Volume1 = SUM(   CASE
                                             WHEN In_Network_Payment = 1 THEN
                                                 Volume_Amount_USD
                                             ELSE
                                                 0
                                         END
                                     ),
           NumberOfVolumeMAP_Volume1 = SUM(   CASE
                                                  WHEN In_Network_Payment = 1 THEN
                                                      Volume_Amount_USD
                                                  ELSE
                                                      0
                                              END
                                          ),
           SdVolume_Volume1 = STDEV(Volume_Amount_USD),
           MaxVolume_Volume1 = MAX(Volume_Amount_USD),
           AmountOfPaymentRequest_Volume1 = SUM(   CASE
                                                       WHEN Payment_Request_ID IS NOT NULL THEN
                                                           1
                                                       ELSE
                                                           0
                                                   END
                                               ),
           GeometricMeanVolume_Volume1 = EXP(AVG(LOG((ABS(Volume_Amount_USD) + 1)))),
           SumOfCrossCountryVolume_Volume1 = SUM(   CASE
                                                        WHEN Payee_Billing_Country_ID <> Payer_Billing_Country_ID THEN
                                                            1
                                                        ELSE
                                                            0
                                                    END
                                                )
    INTO #Volume1
    FROM #Volume AS Volume
    WHERE DATEDIFF(DAY, Transaction_Datetime, @CutPoint) >= 0
          AND DATEDIFF(DAY, Transaction_Datetime, @CutPoint) <= 30
    GROUP BY Volume.Account_Holder_ID;


    /*Volume - last 30-60 days*/
    IF OBJECT_ID('tempdb..#Volume2') IS NOT NULL
        DROP TABLE #Volume2;

    SELECT Account_Holder_ID = Volume.Account_Holder_ID,
           NumberOfLoads_Volume2 = COUNT(Transaction_Datetime),
           SumVolume_Volume2 = SUM(Volume_Amount_USD),
           SumVolumeMAP_Volume2 = SUM(   CASE
                                             WHEN In_Network_Payment = 1 THEN
                                                 Volume_Amount_USD
                                             ELSE
                                                 0
                                         END
                                     ),
           NumberOfVolumeMAP_Volume2 = SUM(   CASE
                                                  WHEN In_Network_Payment = 1 THEN
                                                      Volume_Amount_USD
                                                  ELSE
                                                      0
                                              END
                                          ),
           SdVolume_Volume2 = STDEV(Volume_Amount_USD),
           MaxVolume_Volume2 = MAX(Volume_Amount_USD),
           AmountOfPaymentRequest_Volume2 = SUM(   CASE
                                                       WHEN Payment_Request_ID IS NOT NULL THEN
                                                           1
                                                       ELSE
                                                           0
                                                   END
                                               ),
           GeometricMeanVolume_Volume2 = EXP(AVG(LOG((ABS(Volume_Amount_USD) + 1)))),
           SumOfCrossCountryVolume_Volume2 = SUM(   CASE
                                                        WHEN Payee_Billing_Country_ID <> Payer_Billing_Country_ID THEN
                                                            1
                                                        ELSE
                                                            0
                                                    END
                                                )
    INTO #Volume2
    FROM #Volume AS Volume
    WHERE DATEDIFF(DAY, Transaction_Datetime, @CutPoint) >= 31
          AND DATEDIFF(DAY, Transaction_Datetime, @CutPoint) <= 60
    GROUP BY Volume.Account_Holder_ID;

    /*Volume - last 60-90 days*/
    IF OBJECT_ID('tempdb..#Volume3') IS NOT NULL
        DROP TABLE #Volume3;

    SELECT Account_Holder_ID = Volume.Account_Holder_ID,
           NumberOfLoads_Volume3 = COUNT(Transaction_Datetime),
           SumVolume_Volume3 = SUM(Volume_Amount_USD),
           SumVolumeMAP_Volume3 = SUM(   CASE
                                             WHEN In_Network_Payment = 1 THEN
                                                 Volume_Amount_USD
                                             ELSE
                                                 0
                                         END
                                     ),
           NumberOfVolumeMAP_Volume3 = SUM(   CASE
                                                  WHEN In_Network_Payment = 1 THEN
                                                      Volume_Amount_USD
                                                  ELSE
                                                      0
                                              END
                                          ),
           SdVolume_Volume3 = STDEV(Volume_Amount_USD),
           MaxVolume_Volume3 = MAX(Volume_Amount_USD),
           AmountOfPaymentRequest_Volume3 = SUM(   CASE
                                                       WHEN Payment_Request_ID IS NOT NULL THEN
                                                           1
                                                       ELSE
                                                           0
                                                   END
                                               ),
           GeometricMeanVolume_Volume3 = EXP(AVG(LOG((ABS(Volume_Amount_USD) + 1)))),
           SumOfCrossCountryVolume_Volume3 = SUM(   CASE
                                                        WHEN Payee_Billing_Country_ID <> Payer_Billing_Country_ID THEN
                                                            1
                                                        ELSE
                                                            0
                                                    END
                                                )
    INTO #Volume3
    FROM #Volume AS Volume
    WHERE DATEDIFF(DAY, Transaction_Datetime, @CutPoint) >= 61
          AND DATEDIFF(DAY, Transaction_Datetime, @CutPoint) <= 90
    GROUP BY Volume.Account_Holder_ID;

    /*Volume - last 90-120 days*/
    IF OBJECT_ID('tempdb..#Volume4') IS NOT NULL
        DROP TABLE #Volume4;
    SELECT Volume.Account_Holder_ID,
           SumVolume_Volume4 = SUM(Volume_Amount_USD)
    INTO #Volume4
    FROM #Volume AS Volume
    WHERE DATEDIFF(DAY, Transaction_Datetime, @CutPoint) > 90
          AND DATEDIFF(DAY, Transaction_Datetime, @CutPoint) <= 120
    GROUP BY Volume.Account_Holder_ID;

    /*Volume - last 120-150 days*/
    IF OBJECT_ID('tempdb..#Volume5') IS NOT NULL
        DROP TABLE #Volume5;
    SELECT Volume.Account_Holder_ID,
           SumVolume_Volume5 = SUM(Volume_Amount_USD)
    INTO #Volume5
    FROM #Volume AS Volume
    WHERE DATEDIFF(DAY, Transaction_Datetime, @CutPoint) > 120
          AND DATEDIFF(DAY, Transaction_Datetime, @CutPoint) <= 150
    GROUP BY Volume.Account_Holder_ID;

    /*Volume - last 150-180 days*/
    IF OBJECT_ID('tempdb..#Volume6') IS NOT NULL
        DROP TABLE #Volume6;
    SELECT Volume.Account_Holder_ID,
           SumVolume_Volume6 = SUM(Volume_Amount_USD)
    INTO #Volume6
    FROM #Volume AS Volume
    WHERE DATEDIFF(DAY, Transaction_Datetime, @CutPoint) > 150
          AND DATEDIFF(DAY, Transaction_Datetime, @CutPoint) <= 180
    GROUP BY Volume.Account_Holder_ID;


    /**************************************************Primary Vertical***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryVertical') IS NOT NULL
        DROP TABLE #PrimaryVertical;
        ;WITH PrimaryVertical
        AS (SELECT Vertical.Account_Holder_ID,
                   Vertical = Vertical.Vertical,
                   Vertical.Volume_Amount_USD,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Account_Holder_ID
                                               ORDER BY Vertical.Volume_Amount_USD DESC
                                              ),           --Order the Verticals by their loads
                   NumberOfVerticals = ROW_NUMBER() OVER (PARTITION BY Account_Holder_ID
                                                          ORDER BY Vertical.Volume_Amount_USD ASC
                                                         ) --Count Number Of Verticals
            FROM
            (
                SELECT Volume.Account_Holder_ID,
                       Volume.Vertical,
                       SUM(Volume.Volume_Amount_USD) AS Volume_Amount_USD
                FROM #Volume AS Volume
                WHERE Volume.Transaction_Datetime <= Volume.LastLoadDate
                      AND DATEDIFF(DAY, Volume.Transaction_Datetime, Volume.LastLoadDate) <= @ActivityDays
                GROUP BY Volume.Account_Holder_ID,
                         Volume.Vertical
            ) AS Vertical )
    SELECT PrimaryVertical.Account_Holder_ID,
           PrimaryVertical = CASE
                                 WHEN 100.0 * PrimaryVertical.Volume_Amount_USD / TotalAmount.TotalAmount <= 75 THEN
                                     'Multi Vertical'
                                 ELSE
                                     PrimaryVertical.Vertical
                             END,
           PrimaryVertical.Volume_Amount_USD,
           PrimaryVerticalPercent = (100.0 * PrimaryVertical.Volume_Amount_USD / TotalAmount.TotalAmount),
           NumberOfVerticals = PrimaryVertical.NumberOfVerticals
    INTO #PrimaryVertical
    FROM PrimaryVertical
        LEFT JOIN
        (
            SELECT Account_Holder_ID,
                   SUM(Volume_Amount_USD) AS TotalAmount
            FROM PrimaryVertical
            GROUP BY Account_Holder_ID --Calculate the entire Volume amount
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryVertical.Account_Holder_ID
    WHERE RowNum = 1; --Take only the primary vertical 

    /*
--Statistics about the Primary Verticals
SELECT PrimaryVertical,
       NumberOfAH=COUNT(*),
       [Percent]=100.0 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #PrimaryVertical
       ),
	   MoneyPercent = 100.0*SUM(Volume_Amount_USD)/(SELECT SUM(Volume_Amount_USD) FROM #PrimaryVertical)
FROM #PrimaryVertical
GROUP BY PrimaryVertical
ORDER BY NumberOfAH DESC;
*/


    /**************************************************Primary Loader Name***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryLoader') IS NOT NULL
        DROP TABLE #PrimaryLoader;
    WITH PrimaryLoader
    AS (SELECT PrimaryLoader.Account_Holder_ID,
               PrimaryLoader.LoaderName,
               PrimaryLoader.Volume_Amount_USD,
               RowNum = ROW_NUMBER() OVER (PARTITION BY Account_Holder_ID ORDER BY Volume_Amount_USD DESC), --Order the Loaders by their Volume Amount
               NumberOfLoadersName = ROW_NUMBER() OVER (PARTITION BY Account_Holder_ID ORDER BY Volume_Amount_USD ASC)
        FROM
        (
            SELECT Volume.Account_Holder_ID,
                   Volume.LoaderName AS LoaderName,
                   SUM(Volume.Volume_Amount_USD) AS Volume_Amount_USD
            FROM #Volume AS Volume
            WHERE DATEDIFF(DAY, Volume.Transaction_Datetime, Volume.LastLoadDate) >= 0
                  AND DATEDIFF(DAY, Volume.Transaction_Datetime, Volume.LastLoadDate) <= @ActivityDays
            GROUP BY Volume.Account_Holder_ID,
                     Volume.LoaderName
        ) AS PrimaryLoader )
    SELECT PrimaryLoader.Account_Holder_ID,
           PrimaryLoader.Volume_Amount_USD,
           PrimaryLoader = CASE
                               WHEN (100.00 * PrimaryLoader.Volume_Amount_USD / TotalVolume.TotalVolume) <= 50 THEN
                                   'Multi Loaders'
                               ELSE
                                   PrimaryLoader.LoaderName
                           END,
           PercentOfLoaderVolume = 100.00 * PrimaryLoader.Volume_Amount_USD / TotalVolume.TotalVolume,
           NumberOfLoadersName = PrimaryLoader.NumberOfLoadersName,
           PrimaryLoader.RowNum
    INTO #PrimaryLoader
    FROM PrimaryLoader
        LEFT JOIN
        (
            SELECT Account_Holder_ID,
                   SUM(Volume_Amount_USD) AS TotalVolume
            FROM PrimaryLoader
            GROUP BY Account_Holder_ID
        ) AS TotalVolume
            ON TotalVolume.Account_Holder_ID = PrimaryLoader.Account_Holder_ID
    WHERE PrimaryLoader.RowNum = 1;

    /*
--Statistics about the Primary Loader
SELECT PrimaryLoader,
       NumberOfAH=COUNT(*),
       [Percent]=100.0 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #PrimaryLoader
       ),
	   MoneyPercent = 100.0*SUM(Volume_Amount_USD)/(SELECT SUM(Volume_Amount_USD) FROM #PrimaryLoader)
FROM #PrimaryLoader
GROUP BY PrimaryLoader
ORDER BY MoneyPercent DESC;
*/

    /**************************************************Primary Volume Type***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryVolumeType') IS NOT NULL
        DROP TABLE #PrimaryVolumeType;
    WITH PrimaryVertical
    AS (SELECT Vertical.Account_Holder_ID,
               Vertical.VolumeTypeID,
               PrimaryVolumeType = Vertical.VolumeType,
               PrimaryVolumeTypeCategory = Vertical.VolumeTypeCategory,
               PrimaryVolumeTypeSubCategory = Vertical.VolumeTypeSubCategory,
               Vertical.VerticalAmount,
               RowNum = ROW_NUMBER() OVER (PARTITION BY Account_Holder_ID ORDER BY VerticalAmount DESC),
               NumberOfVolumeTypes = ROW_NUMBER() OVER (PARTITION BY Account_Holder_ID ORDER BY VerticalAmount ASC)
        FROM
        (
            SELECT Volume.Account_Holder_ID,
                   Volume.Volume_Type_ID AS VolumeTypeID,
                   VolumeType = MIN(Volume_Types.Volume_Type),
                   VolumeTypeCategory = MIN(Volume_Types.Volume_Type_Category),
                   VolumeTypeSubCategory = MIN(Volume_Types.Volume_Type_Sub_Category),
                   SUM(Volume.Volume_Amount_USD) AS VerticalAmount
            FROM #Volume AS Volume

                /* Volume Types */
                LEFT JOIN [DW_Main].[dbo].Dim_Volume_Types AS Volume_Types
                    ON Volume_Types.Volume_Type_ID = Volume.Volume_Type_ID
            WHERE DATEDIFF(DAY, Volume.Transaction_Datetime, Volume.LastLoadDate) >= 0
                  AND DATEDIFF(DAY, Volume.Transaction_Datetime, Volume.LastLoadDate) <= @ActivityDays
            GROUP BY Volume.Account_Holder_ID,
                     Volume.Volume_Type_ID
        ) AS Vertical )
    SELECT PrimaryVertical.*
    INTO #PrimaryVolumeType
    FROM PrimaryVertical
    WHERE RowNum = 1;

    /*
--Statistics about the Primary Type
SELECT PrimaryVolumeType,
       NumberOfAH=COUNT(*),
       [Percent]=100.0 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #PrimaryVolumeType
       )
	  -- MoneyPercent = 100.0*SUM(Volume_Amount_USD)/(SELECT SUM(Volume_Amount_USD) FROM #PrimaryVolumeType)
FROM #PrimaryVolumeType
GROUP BY PrimaryVolumeType
ORDER BY NumberOfAH DESC;
*/

    /**************************************************EMail***********************************************/
    IF OBJECT_ID('tempdb..#EMailTable') IS NOT NULL
        DROP TABLE #EMailTable;
    SELECT Account_Holder_ID = BaseView.Account_Holder_ID,
           SumMailSent = SUM(CAST(Contact.Is_Sent AS INT)),
           SumMailOpened = SUM(CAST(Contact.Is_Opened AS INT)),
           SDMailOpened = STDEVP(CAST(Contact.Is_Opened AS INT)),
           SumMailClicked = SUM(CAST(Contact.Is_Clicked AS INT)),
           LastMailDateCutPointDifference = MIN(1.00 * DATEDIFF(HOUR, Contact.SendDate, @CutPoint) / 24.0)
    INTO #EMailTable
    FROM [DW_Main].[dbo].syn_active_Fact_Contact_History AS Contact

        /*Base View*/
        INNER JOIN #BaseView AS BaseView
            ON BaseView.Account_Holder_ID = Contact.Involved_Party_ID
               AND Contact.Involved_Party_Type = 1
    WHERE Contact.SendDate <= @CutPoint
          AND Contact.Is_Sent = 1
    GROUP BY Account_Holder_ID;

    /**************************************************Logs***********************************************/
    IF OBJECT_ID('tempdb..#Logs') IS NOT NULL
        DROP TABLE #Logs;
    SELECT Account_Holder_ID = UserActionLog.Involved_Party_ID,
           UserActionLog.Action_DateTime,
           UserActionLog.Application_ID,
           UserActionLog.Browser_ID,
           Browser = LOWER(Ref_Browsers.Description),
           UserActionLog.Device_ID,
           Device = LOWER(Ref_Devices.Description),
           Platform_Desc = LOWER(MyAccount_Platforms.Platform_Desc),
           Platform_Type_Desc = LOWER(MyAccount_Platforms.Platform_Type_Desc),
           RowNum = ROW_NUMBER() OVER (PARTITION BY UserActionLog.Involved_Party_ID
                                       ORDER BY UserActionLog.Action_DateTime ASC
                                      ),
           NumberOfEnteringToAccount = ROW_NUMBER() OVER (PARTITION BY UserActionLog.Involved_Party_ID
                                                          ORDER BY UserActionLog.Action_DateTime DESC
                                                         ),
           FirstLogDate = MIN(UserActionLog.Action_DateTime) OVER (PARTITION BY UserActionLog.Involved_Party_ID),
           LastLogDate = MAX(UserActionLog.Action_DateTime) OVER (PARTITION BY UserActionLog.Involved_Party_ID)
    INTO #Logs
    FROM [DW_Main].[dbo].[syn_active_Fact_MyAccount_User_Actions] AS UserActionLog

        /* BaseView */
        INNER JOIN #BaseView AS BaseView
            ON BaseView.Account_Holder_ID = UserActionLog.Involved_Party_ID
               AND UserActionLog.Involved_Party_Type = 1

        /* Devices */
        INNER JOIN [DW_Main].[dbo].Ref_Devices AS Ref_Devices
            ON Ref_Devices.Device_ID = UserActionLog.Device_ID

        /* Browsers */
        INNER JOIN [DW_Main].[dbo].Ref_Browsers AS Ref_Browsers
            ON Ref_Browsers.Browser_ID = UserActionLog.Browser_ID

        /* Platforms */
        INNER JOIN [DW_Main].[dbo].Dim_MyAccount_Platforms AS MyAccount_Platforms
            ON MyAccount_Platforms.Platform_ID = UserActionLog.Platform_ID
    WHERE UserActionLog.Involved_Party_Type = 1
          AND UserActionLog.Action_DateTime <= @CutPoint;


    IF OBJECT_ID('tempdb..#LogsTable') IS NOT NULL
        DROP TABLE #LogsTable;

    SELECT Logs.Account_Holder_ID,
           FirstBrowser = ISNULL(Logs.Browser, 'Unknown'),
           FirstDevice = ISNULL(Logs.Device, 'Unknown'),
           FirstPlatform = ISNULL(Logs.Platform_Desc, 'Unknown'),
           NumberOfEnteringToAccount = ISNULL(Logs.NumberOfEnteringToAccount, 0),
           LastLogAndFirstLogDifference = ISNULL(1.00 * DATEDIFF(HOUR, FirstLogDate, LastLogDate) / 24.00, -1),
           LastLogCutPointDifference = ISNULL(1.00 * DATEDIFF(HOUR, LastLogDate, @CutPoint) / 24.00, -1)
    INTO #LogsTable
    FROM #Logs AS Logs
    WHERE RowNum = 1;


    /*  Primary Browser */
    IF OBJECT_ID('tempdb..#PrimaryBrowser') IS NOT NULL
        DROP TABLE #PrimaryBrowser;
        ;
    WITH FrequencyBrowser
        AS (SELECT Logs.Account_Holder_ID,
                   Logs.Browser,
                   FrequencyBrowser = COUNT(Logs.Browser),
                   MostFrequencedBrowser = ROW_NUMBER() OVER (PARTITION BY Logs.Account_Holder_ID
                                                              ORDER BY COUNT(Logs.Browser) DESC
                                                             ),
                   NumberOfBrowsers = ROW_NUMBER() OVER (PARTITION BY Logs.Account_Holder_ID
                                                         ORDER BY COUNT(Logs.Browser) ASC
                                                        )
            FROM #Logs AS Logs
            GROUP BY Logs.Account_Holder_ID,
                     Browser)
    SELECT Account_Holder_ID = Account_Holder_ID,
           PrimaryBrowser = Browser,
           NumberOfBrowsers = NumberOfBrowsers
    INTO #PrimaryBrowser
    FROM FrequencyBrowser
    WHERE MostFrequencedBrowser = 1;


    /* Primary Device */
    IF OBJECT_ID('tempdb..#PrimaryDevice') IS NOT NULL
        DROP TABLE #PrimaryDevice;
        ;
    WITH FrequencyDevice
        AS (SELECT Logs.Account_Holder_ID,
                   Logs.Device,
                   FrequencyBrowser = COUNT(Logs.Device),
                   MostFrequencedDevice = ROW_NUMBER() OVER (PARTITION BY Logs.Account_Holder_ID
                                                             ORDER BY COUNT(Logs.Device) DESC
                                                            ),
                   NumberOfDevices = ROW_NUMBER() OVER (PARTITION BY Logs.Account_Holder_ID
                                                        ORDER BY COUNT(Logs.Device) ASC
                                                       )
            FROM #Logs AS Logs
            GROUP BY Logs.Account_Holder_ID,
                     Device)
    SELECT Account_Holder_ID = Account_Holder_ID,
           PrimaryDevice = Device,
           NumberOfDevices = NumberOfDevices
    INTO #PrimaryDevice
    FROM FrequencyDevice
    WHERE MostFrequencedDevice = 1;


    /* Primary Platform_Desc */
    IF OBJECT_ID('tempdb..#PrimaryPlatform') IS NOT NULL
        DROP TABLE #PrimaryPlatform;
        ;
    WITH FrequencyPlatform
        AS (SELECT Logs.Account_Holder_ID,
                   Logs.Platform_Desc,
                   FrequencyPlatform_Desc = COUNT(Logs.Platform_Desc),
                   MostFrequencedPlatform = ROW_NUMBER() OVER (PARTITION BY Logs.Account_Holder_ID
                                                               ORDER BY COUNT(Logs.Platform_Desc) DESC
                                                              ),
                   NumberOfPlatforms = ROW_NUMBER() OVER (PARTITION BY Logs.Account_Holder_ID
                                                          ORDER BY COUNT(Logs.Platform_Desc) ASC
                                                         )
            FROM #Logs AS Logs
            GROUP BY Logs.Account_Holder_ID,
                     Platform_Desc)
    SELECT Account_Holder_ID = Account_Holder_ID,
           PrimaryPlatform = Platform_Desc,
           NumberOfPlatforms = NumberOfPlatforms --Theoreticly we are checking if the AH have the App or not
    INTO #PrimaryPlatform
    FROM FrequencyPlatform
    WHERE MostFrequencedPlatform = 1;


    /************************************ Incidents **********************************************/
    DROP TABLE IF EXISTS #Incidents;

    SELECT Account_Holder_ID = Fact_ORN_Incidents.Account_Holder_ID,
           Fact_ORN_Incidents.Incident_ID,
           CreationIncidentDateTime = Fact_ORN_Incidents.Incident_Creation_DateTime,       --when the incident Started 
           FirstInquiryDescription = Ref_Manual_Log_Inquiry_Types.[Description],           --How AH contacted to payoneer (chat, EMail, Phone ect...)
           FirstSubjectDescription = Dim_Manual_Log_Inquiry_Subjects.[Description],        --First Subject of Calling
           FirstSubSubjectDescription = Dim_Manual_Log_Inquiry_Sub_Subjects.[Description], --First more described subject of Calling
           FirstDepartmentsDescription = Dim_Manual_Log_Departments.[Description],         --Department (CAD,CC ect)
           RowNum = ROW_NUMBER() OVER (PARTITION BY Fact_ORN_Incidents.Account_Holder_ID
                                       ORDER BY Fact_ORN_Incidents.Incident_Creation_DateTime ASC
                                      )
    INTO #Incidents
    FROM DW_Main.dbo.syn_active_Fact_ORN_Incidents AS Fact_ORN_Incidents
        INNER JOIN #BaseView AS [Population]
            ON Fact_ORN_Incidents.Account_Holder_ID = [Population].Account_Holder_ID

        /*How AH contacted to us (chat, EMail, Phone ect...)*/
        LEFT JOIN [DW_Main].dbo.Ref_Manual_Log_Inquiry_Types AS Ref_Manual_Log_Inquiry_Types
            ON Fact_ORN_Incidents.First_Manual_Log_Inquiry_Type_ID = Ref_Manual_Log_Inquiry_Types.Inquiry_Type_ID

        /*First Subject of Calling*/
        LEFT JOIN DW_Main.dbo.Ref_Manual_Log_Departments AS Dim_Manual_Log_Departments
            ON Fact_ORN_Incidents.First_Communication_Department_Manual_Log_ID = Dim_Manual_Log_Departments.Manual_Log_Department_ID

        /*First more described subject of Calling*/
        LEFT JOIN DW_Main.dbo.Ref_Manual_Log_Inquiry_Subjects AS Dim_Manual_Log_Inquiry_Subjects
            ON Fact_ORN_Incidents.First_Communication_Subject_Manual_Log_ID = Dim_Manual_Log_Inquiry_Subjects.Inquiry_Subject_ID

        /*Department (CAD,CC ect)*/
        LEFT JOIN DW_Main.dbo.Ref_Manual_Log_Inquiry_Sub_Subjects AS Dim_Manual_Log_Inquiry_Sub_Subjects
            ON Fact_ORN_Incidents.First_Communication_Sub_Subject_Manual_Log_ID = Dim_Manual_Log_Inquiry_Sub_Subjects.Sub_Subject_ID
    WHERE (
              Fact_ORN_Incidents.Is_FCR_Incident = 1
              OR Fact_ORN_Incidents.Is_FCR_Incident = 0
          )
          AND Fact_ORN_Incidents.Account_Holder_ID IS NOT NULL
          AND Fact_ORN_Incidents.Is_Internal_Payoneer_AH = 0
          AND Fact_ORN_Incidents.Incident_Initiator_ID = 2 --AH Called to CC
          AND DATEDIFF(DAY, Fact_ORN_Incidents.Incident_Creation_DateTime, @CutPoint) >= 0
          AND DATEDIFF(DAY, Fact_ORN_Incidents.Incident_Creation_DateTime, @CutPoint) <= @ActivityDays;


    /*#Agregated Incidents*/
    DROP TABLE IF EXISTS #AgregatedIncidents;
    SELECT Account_Holder_ID,
           DaysFromLastIncident = MIN(1.00 * DATEDIFF(HOUR, CreationIncidentDateTime, @CutPoint) / 24.0),
           NumberOfIncidents = COUNT(Account_Holder_ID),
           NumberOfDistinctInquiry = COUNT(DISTINCT FirstInquiryDescription),
           NumberOfDistinctFirstSubjectDescription = COUNT(DISTINCT FirstSubjectDescription),
           NumberOfDistinctFirstSubSubjectDescription = COUNT(DISTINCT FirstSubSubjectDescription),
           NumberOfDistinctFirstDepartment = COUNT(DISTINCT FirstDepartmentsDescription)
    INTO #AgregatedIncidents
    FROM #Incidents AS Incidents
    GROUP BY Account_Holder_ID;

    /*#Primary Inquiry*/
    DROP TABLE IF EXISTS #PrimaryInquiry;
    WITH FrequencyInquiry
    AS (SELECT Account_Holder_ID = Incidents.Account_Holder_ID,
               FirstInquiryDescription = Incidents.FirstInquiryDescription,
               NumberofInquiries = COUNT(Incidents.FirstInquiryDescription),
               MostFrequencedDevice = ROW_NUMBER() OVER (PARTITION BY Incidents.Account_Holder_ID
                                                         ORDER BY COUNT(Incidents.FirstInquiryDescription) DESC
                                                        )
        FROM #Incidents AS Incidents
        GROUP BY Incidents.Account_Holder_ID,
                 Incidents.FirstInquiryDescription)
    SELECT Account_Holder_ID = Account_Holder_ID,
           PrimaryInquiry = FirstInquiryDescription,
           NumberOfMostFrequentInquiry = NumberofInquiries
    INTO #PrimaryInquiry
    FROM FrequencyInquiry
    WHERE MostFrequencedDevice = 1;

    /**************************************************Usage***********************************************/
    IF OBJECT_ID('tempdb..#Usage') IS NOT NULL
        DROP TABLE #Usage;

    SELECT Account_Holder_ID = BaseView.Account_Holder_ID,
           UsageDateTime = Usage.Usage_Initiate_DateTime,
           Usage = Usage.Usage_Initiate_Amount_USD,
           CNP = CASE
                     WHEN Usage.CNP_Ind = 0 THEN
                         0
                     ELSE
                         1
                 END,
           Merchant_Category_Code = Usage.Merchant_Category_Code,
           MerchantCategories = ISNULL(   CASE
                                              WHEN Usage.Merchant_Category_Code = -1 THEN
                                                  'No Merchent'
                                              ELSE
                                                  MerchantCategories.Merchant_Category_Desc
                                          END,
                                          'Merchent Not In The List'
                                      ),
           IsAutoWithdrawal = Usage.Is_Auto_Withdrawal,
           IsCrossCountry = Usage.Is_Cross_Country,
           IsFX = Usage.Is_FX,
           TypeOfCurrency = Usage.Payer_Transaction_Method_Currency,
           UsageType = UsageTypes.Usage_Type,
           UsageTypeCategory = UsageTypes.Usage_Type_Category,
           UsageTypeSubCategory = UsageTypes.Usage_Type_Sub_Category,
           UsageEndCountry = DimCountries.ISO2,
           ExternalBankName = External_TM.External_Bank_Name,
           BankNumber = External_TM.Bank_Number,
           BankCode = External_TM.Bank_Code,
           Routing = External_TM.Routing
    INTO #Usage
    FROM [DW_Main].[dbo].[syn_active_Fact_Usage] AS Usage

        /* Base View */
        INNER JOIN #BaseView AS BaseView
            ON BaseView.Account_Holder_ID = Usage.Payer_Involved_Party_ID
               AND Usage.Payer_Involved_Party_Type = 1

        /* Transaction Categories */
        LEFT JOIN [DW_Main].[dbo].Ref_Transaction_Categories AS Ref_Transaction_Categories
            ON Usage.Usage_Status = Ref_Transaction_Categories.StatusCode

        /* Usage Types */
        LEFT JOIN DW_Main.dbo.Dim_Usage_Types AS UsageTypes
            ON Usage.Usage_Type_ID = UsageTypes.Usage_Type_ID

        /* Merchant Categories */
        LEFT JOIN DW_Main.dbo.Dim_Merchant_Categories AS MerchantCategories
            ON Usage.Merchant_Category_Code = MerchantCategories.Merchant_Category_ID

        /*Dim Countries*/
        LEFT JOIN DW_Main.dbo.Dim_Countries AS DimCountries
            ON Usage.Usage_End_Country_ID = DimCountries.Country_ID

        /*External Bank*/
        LEFT JOIN DW_Main.dbo.syn_active_Dim_Transaction_Methods AS External_TM
            ON Usage.Payer_External_Transaction_Method_ID = External_TM.Transaction_Method_ID
               AND Usage.Payer_External_Transaction_Method_Type = External_TM.Transaction_Method_Type
    WHERE Usage.Payer_Involved_Party_Type = 1
          AND Ref_Transaction_Categories.StatusCategoryCode = 2
          AND Usage.Usage_Initiate_DateTime <= @CutPoint;

    --SELECT * FROM DW_Main.dbo.syn_active_Dim_Transaction_Methods
    --SELECT * FROM #Usage where Routing is not null
    --SELECT * FROM DW_Main.dbo.Dim_Usage_Types
    --SELECT * FROM DW_Main.dbo.Dim_Countries 

    CREATE CLUSTERED COLUMNSTORE INDEX a ON #Usage;
    /**************************************************  Usage Aggregate ***********************************************/
    IF OBJECT_ID('tempdb..#Usage_UntilNow') IS NOT NULL
        DROP TABLE #Usage_UntilNow;

    SELECT Account_Holder_ID = BaseView.Account_Holder_ID,
           LastUsageDate = MAX(Usage.UsageDateTime),
           LastLoadDate = MAX(VolumeAgregate.LastLoadDate),
           DaysBetweenFirstUsageToCutPoint = MAX(1.0 * DATEDIFF(HOUR, Usage.UsageDateTime, @CutPoint) / 24.00),
           DaysFromLastUsageToCutPoint = MIN(1.0 * DATEDIFF(HOUR, Usage.UsageDateTime, @CutPoint) / 24.00),
           LastLoadLastUsageDiff = ABS(1.0 * DATEDIFF(HOUR, MAX(Usage.UsageDateTime), MAX(VolumeAgregate.LastLoadDate))
                                       / 24.0
                                      ),
           IsLastLoadAfterLastUsage = CASE
                                          WHEN MAX(VolumeAgregate.LastLoadDate) >= MAX(Usage.UsageDateTime) THEN
                                              1
                                          ELSE
                                              0
                                      END,
           SumUsage_UntilTotal = SUM(Usage.Usage),
           NumberOfUsage_UntilTotal = COUNT(Usage.Usage)
    INTO #Usage_UntilNow
    FROM #Usage AS Usage

        /*Base View*/
        INNER JOIN #BaseView AS BaseView
            ON Usage.Account_Holder_ID = BaseView.Account_Holder_ID

        /*Volume Aggregate*/
        LEFT JOIN #VolumeAgregate AS VolumeAgregate
            ON Usage.Account_Holder_ID = VolumeAgregate.Account_Holder_ID
    GROUP BY BaseView.Account_Holder_ID;



    IF OBJECT_ID('tempdb..#UsageSeveralMonths') IS NOT NULL
        DROP TABLE #UsageSeveralMonths;

    SELECT Usage.Account_Holder_ID,
           SumIsFX = SUM(Usage.IsFX),
           SumIsCrossCountry = SUM(Usage.IsCrossCountry),
           SumIsAutoWithdrawal = SUM(Usage.IsAutoWithdrawal),
           NumberOfMerchentCategory = COUNT(DISTINCT Usage.MerchantCategories),
           NumberOfDistinctUsageEndCountry = COUNT(DISTINCT Usage.UsageEndCountry),
           NumberOfDistinctExternalBankName = COUNT(DISTINCT Usage.ExternalBankName + ' ' + Usage.BankNumber),
           DiversityOfMerchentCategory = 100.0 * COUNT(DISTINCT Usage.MerchantCategories)
                                         / COUNT(Usage.Account_Holder_ID),
           NumberOfOfCurrencies = COUNT(DISTINCT Usage.TypeOfCurrency),
           SumCNP = SUM(Usage.CNP),
           NumberOfUsage = SUM(   CASE
                                      WHEN Usage.Usage >= 0 THEN
                                          1
                                      ELSE
                                          0
                                  END
                              ),
           SumUsage = SUM(Usage.Usage),
           GeometricMeanUsage = EXP(AVG(LOG(ABS(Usage.Usage) + 1))),
           MaxUsage = MAX(Usage.Usage),
           SdUsage = STDEV(Usage.Usage),
           MeanUsage = AVG(Usage.Usage),
           SumUsage_FX = SUM(   CASE
                                    WHEN Usage.CNP = 1 THEN
                                        Usage.Usage
                                    ELSE
                                        0
                                END
                            ),
           SumUsage_NotInUSD = SUM(   CASE
                                          WHEN Usage.TypeOfCurrency <> 'USD' THEN
                                              Usage.Usage
                                          ELSE
                                              0
                                      END
                                  ),
           SumUsage_CNP = SUM(   CASE
                                     WHEN Usage.CNP = 1 THEN
                                         Usage.Usage
                                     ELSE
                                         0
                                 END
                             ),
           SumUsage_CrossCountry = SUM(   CASE
                                              WHEN Usage.IsCrossCountry = 1 THEN
                                                  Usage.Usage
                                              ELSE
                                                  0
                                          END
                                      ),
           SumUsage_MAP = SUM(   CASE
                                     WHEN Usage.UsageTypeCategory = 'Make A Payment' THEN
                                         Usage.Usage
                                     ELSE
                                         NULL
                                 END
                             ),
           SumUsage_ATM_POS = SUM(   CASE
                                         WHEN Usage.UsageTypeCategory = 'ATM \ POS' THEN
                                             Usage.Usage
                                         ELSE
                                             NULL
                                     END
                                 ),
           SumUsage_Withdrawal = SUM(   CASE
                                            WHEN Usage.UsageTypeCategory = 'Withdrawal' THEN
                                                Usage.Usage
                                            ELSE
                                                NULL
                                        END
                                    ),
           MeanUsage_MAP = AVG(   CASE
                                      WHEN Usage.UsageTypeCategory = 'Make A Payment' THEN
                                          Usage.Usage
                                      ELSE
                                          NULL
                                  END
                              ),
           MaxUsage_MAP = MAX(   CASE
                                     WHEN Usage.UsageTypeCategory = 'Make A Payment' THEN
                                         Usage.Usage
                                     ELSE
                                         NULL
                                 END
                             ),
           MeanUsage_ATM_POS = AVG(   CASE
                                          WHEN Usage.UsageTypeCategory = 'ATM \ POS' THEN
                                              Usage.Usage
                                          ELSE
                                              NULL
                                      END
                                  ),
           MaxUsage_ATM_POS = MAX(   CASE
                                         WHEN Usage.UsageTypeCategory = 'ATM \ POS' THEN
                                             Usage.Usage
                                         ELSE
                                             NULL
                                     END
                                 ),
           MeanUsage_Withdrawal = AVG(   CASE
                                             WHEN Usage.UsageTypeCategory = 'Withdrawal' THEN
                                                 Usage.Usage
                                             ELSE
                                                 NULL
                                         END
                                     ),
           MaxUsage_Withdrawal = MAX(   CASE
                                            WHEN Usage.UsageTypeCategory = 'Withdrawal' THEN
                                                Usage.Usage
                                            ELSE
                                                NULL
                                        END
                                    )
    INTO #UsageSeveralMonths
    FROM #Usage AS Usage

        /*Usage Until Now*/
        LEFT JOIN #Usage_UntilNow AS Usage_UntilNow
            ON Usage.Account_Holder_ID = Usage_UntilNow.Account_Holder_ID
    WHERE UsageDateTime <= Usage_UntilNow.LastUsageDate
          AND DATEDIFF(DAY, UsageDateTime, Usage_UntilNow.LastUsageDate) <= 30 * 6
    GROUP BY Usage.Account_Holder_ID;

    /************************************************** Primary Usage Type ***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryUsageType') IS NOT NULL
        DROP TABLE #PrimaryUsageType;
        ;WITH PrimaryUsageType
        AS (SELECT Usage.Account_Holder_ID,
                   UsageType = Usage.UsageType,
                   Usage.Usage,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Usage.Account_Holder_ID ORDER BY Usage.Usage DESC),          --Order the UsageTypes by their USD
                   NumberOfUsageType = ROW_NUMBER() OVER (PARTITION BY Usage.Account_Holder_ID ORDER BY Usage.Usage ASC) --Count Number Of Usage Types
            FROM
            (
                SELECT Usage.Account_Holder_ID,
                       Usage.UsageType,
                       Usage = SUM(Usage.Usage)
                FROM #Usage AS Usage
                WHERE Usage.UsageDateTime <= @CutPoint
                      AND DATEDIFF(DAY, Usage.UsageDateTime, @CutPoint) <= @ActivityDays
                      AND Usage.Usage > 0
                GROUP BY Usage.Account_Holder_ID,
                         Usage.UsageType
            ) AS Usage )
    SELECT PrimaryUsageType.Account_Holder_ID,
           PrimaryUsageType = CASE
                                  WHEN 100.0 * PrimaryUsageType.Usage / TotalAmount.TotalUsage <= 50 THEN
                                      'Multi Usage Type'
                                  ELSE
                                      PrimaryUsageType.UsageType
                              END,
           PrimaryUsageType.Usage,
           PrimaryVerticalPercent = (100.0 * PrimaryUsageType.Usage / TotalAmount.TotalUsage),
           NumberOfUsageType = PrimaryUsageType.NumberOfUsageType
    INTO #PrimaryUsageType
    FROM PrimaryUsageType
        LEFT JOIN
        (
            SELECT PrimaryUsageType.Account_Holder_ID,
                   TotalUsage = SUM(PrimaryUsageType.Usage)
            FROM PrimaryUsageType
            GROUP BY PrimaryUsageType.Account_Holder_ID
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryUsageType.Account_Holder_ID
    WHERE RowNum = 1; --Take only the First Primary Usage 

    /*
--Statistics about the Primary Verticals
SELECT PrimaryUsageType,
       NumberOfAH=COUNT(*),
       [Percent]=100.0 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #PrimaryUsageType
       ),
	   MoneyPercent = 100.0*SUM(Usage)/(SELECT SUM(Usage) FROM #PrimaryUsageType)
FROM #PrimaryUsageType
GROUP BY PrimaryUsageType
ORDER BY NumberOfAH DESC;
*/


    /**************************************************Primary Usage Type Category***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryUsageTypeCategory') IS NOT NULL
        DROP TABLE #PrimaryUsageTypeCategory;

        ;WITH PrimaryUsageTypeCategory
        AS (SELECT Usage.Account_Holder_ID,
                   UsageTypeCategory = Usage.UsageTypeCategory,
                   Usage.Usage,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Usage.Account_Holder_ID ORDER BY Usage.Usage DESC),                  --Order the Usage Type Category by their USD
                   NumberOfUsageTypeCategory = ROW_NUMBER() OVER (PARTITION BY Usage.Account_Holder_ID ORDER BY Usage.Usage ASC) --Count Number Of Usage Types
            FROM
            (
                SELECT Usage.Account_Holder_ID,
                       Usage.UsageTypeCategory,
                       Usage = SUM(Usage.Usage)
                FROM #Usage AS Usage
                WHERE Usage.UsageDateTime <= @CutPoint
                      AND DATEDIFF(DAY, Usage.UsageDateTime, @CutPoint) <= @ActivityDays
                      AND Usage.Usage > 0
                GROUP BY Usage.Account_Holder_ID,
                         Usage.UsageTypeCategory
            ) AS Usage )
    SELECT PrimaryUsageTypeCategory.Account_Holder_ID,
           PrimaryUsageTypeCategory = CASE
                                          WHEN 100.0 * PrimaryUsageTypeCategory.Usage / TotalAmount.TotalUsage <= 50 THEN
                                              'Multi Usage Type Category'
                                          ELSE
                                              PrimaryUsageTypeCategory.UsageTypeCategory
                                      END,
           PrimaryUsageTypeCategory.Usage,
           PrimaryUsagePercent = (100.0 * PrimaryUsageTypeCategory.Usage / TotalAmount.TotalUsage),
           NumberOfUsageTypeCategory = PrimaryUsageTypeCategory.NumberOfUsageTypeCategory
    INTO #PrimaryUsageTypeCategory
    FROM PrimaryUsageTypeCategory
        LEFT JOIN
        (
            SELECT PrimaryUsageTypeCategory.Account_Holder_ID,
                   TotalUsage = SUM(PrimaryUsageTypeCategory.Usage)
            FROM PrimaryUsageTypeCategory
            GROUP BY PrimaryUsageTypeCategory.Account_Holder_ID
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryUsageTypeCategory.Account_Holder_ID
    WHERE RowNum = 1; --Take only the First Primary Usage Type Category

    /*
--Statistics about the Primary Verticals
SELECT PrimaryUsageTypeCategory,
       NumberOfAH=COUNT(*),
       [Percent]=100.0 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #PrimaryUsageTypeCategory
       ),
	   MoneyPercent = 100.0*SUM(Usage)/(SELECT SUM(Usage) FROM #PrimaryUsageTypeCategory)
FROM #PrimaryUsageTypeCategory
GROUP BY PrimaryUsageTypeCategory
ORDER BY NumberOfAH DESC;
*/

    /**************************************************Primary Usage Type Sub Category***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryUsageTypeSubCategory') IS NOT NULL
        DROP TABLE #PrimaryUsageTypeSubCategory;
        ;WITH PrimaryUsageTypeSubCategory
        AS (SELECT Usage.Account_Holder_ID,
                   UsageTypeSubCategory = Usage.UsageTypeSubCategory,
                   Usage.Usage,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Usage.Account_Holder_ID ORDER BY Usage.Usage DESC),                     --Order the UsageTypesSubCategory by their USD
                   NumberOfUsageTypeSubCategory = ROW_NUMBER() OVER (PARTITION BY Usage.Account_Holder_ID ORDER BY Usage.Usage ASC) --Count Number Of Usage Types Sub Category
            FROM
            (
                SELECT Usage.Account_Holder_ID,
                       Usage.UsageTypeSubCategory,
                       Usage = SUM(Usage.Usage)
                FROM #Usage AS Usage
                WHERE Usage.UsageDateTime <= @CutPoint
                      AND DATEDIFF(DAY, Usage.UsageDateTime, @CutPoint) <= @ActivityDays
                      AND Usage.Usage > 0
                GROUP BY Usage.Account_Holder_ID,
                         Usage.UsageTypeSubCategory
            ) AS Usage )
    SELECT PrimaryUsageTypeSubCategory.Account_Holder_ID,
           PrimaryUsageTypeSubCategory = CASE
                                             WHEN 100.0 * PrimaryUsageTypeSubCategory.Usage / TotalAmount.TotalUsage <= 50 THEN
                                                 'Multi Usage Type Sub Category'
                                             ELSE
                                                 PrimaryUsageTypeSubCategory.UsageTypeSubCategory
                                         END,
           PrimaryUsageTypeSubCategory.Usage,
           PrimaryUsagePercent = (100.0 * PrimaryUsageTypeSubCategory.Usage / TotalAmount.TotalUsage),
           NumberOfUsageTypeSubCategory = PrimaryUsageTypeSubCategory.NumberOfUsageTypeSubCategory
    INTO #PrimaryUsageTypeSubCategory
    FROM PrimaryUsageTypeSubCategory
        LEFT JOIN
        (
            SELECT PrimaryUsageTypeSubCategory.Account_Holder_ID,
                   TotalUsage = SUM(PrimaryUsageTypeSubCategory.Usage)
            FROM PrimaryUsageTypeSubCategory
            GROUP BY PrimaryUsageTypeSubCategory.Account_Holder_ID
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryUsageTypeSubCategory.Account_Holder_ID
    WHERE RowNum = 1; --Take only the primary Primary Usage 

    /*
--Statistics
SELECT PrimaryUsageTypeSubCategory,
       NumberOfAH=COUNT(*),
       [Percent]=100.0 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #PrimaryUsageTypeSubCategory
       ),
	   MoneyPercent = 100.0*SUM(Usage)/(SELECT SUM(Usage) FROM #PrimaryUsageTypeSubCategory)
FROM #PrimaryUsageTypeSubCategory
GROUP BY PrimaryUsageTypeSubCategory
ORDER BY NumberOfAH DESC;
*/
    /**************************************************Currency***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryUsageCurrency') IS NOT NULL
        DROP TABLE #PrimaryUsageCurrency;
        ;WITH PrimaryUsageCurrency
        AS (SELECT Usage.Account_Holder_ID,
                   UsageCurrency = Usage.TypeOfCurrency,
                   Usage.Usage,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Usage.Account_Holder_ID ORDER BY Usage.Usage DESC),
                   NumberOfUsageCurrency = ROW_NUMBER() OVER (PARTITION BY Usage.Account_Holder_ID ORDER BY Usage.Usage ASC)
            FROM
            (
                SELECT Usage.Account_Holder_ID,
                       Usage.TypeOfCurrency,
                       Usage = SUM(Usage.Usage)
                FROM #Usage AS Usage
                WHERE Usage.UsageDateTime <= @CutPoint
                      AND DATEDIFF(DAY, Usage.UsageDateTime, @CutPoint) <= @ActivityDays
                      AND Usage.Usage > 0
                GROUP BY Usage.Account_Holder_ID,
                         Usage.TypeOfCurrency
            ) AS Usage )
    SELECT PrimaryUsageCurrency.Account_Holder_ID,
           PrimaryUsageCurrency = CASE
                                      WHEN 100.0 * PrimaryUsageCurrency.Usage / TotalAmount.TotalUsage <= 50 THEN
                                          'Multi Usage Currencies'
                                      ELSE
                                          PrimaryUsageCurrency.UsageCurrency
                                  END,
           PrimaryUsageCurrency.Usage,
           PrimaryUsageCurrencyPercent = (100.0 * PrimaryUsageCurrency.Usage / TotalAmount.TotalUsage),
           NumberOfUsageCurrencies = PrimaryUsageCurrency.NumberOfUsageCurrency
    INTO #PrimaryUsageCurrency
    FROM PrimaryUsageCurrency
        LEFT JOIN
        (
            SELECT PrimaryUsageCurrency.Account_Holder_ID,
                   TotalUsage = SUM(PrimaryUsageCurrency.Usage)
            FROM PrimaryUsageCurrency
            GROUP BY PrimaryUsageCurrency.Account_Holder_ID
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryUsageCurrency.Account_Holder_ID
    WHERE RowNum = 1; --Take only the primary Primary Usage 

    /*
--Statistics
SELECT PrimaryUsageCurrency,
       NumberOfAH=COUNT(*),
       [Percent]=100.0 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #PrimaryUsageCurrency
       ),
	   MoneyPercent = 100.0*SUM(Usage)/(SELECT SUM(Usage) FROM #PrimaryUsageCurrency)
FROM #PrimaryUsageCurrency
GROUP BY PrimaryUsageCurrency
ORDER BY NumberOfAH DESC;
*/
    /**************************************************Primary Merchent***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryMerchent') IS NOT NULL
        DROP TABLE #PrimaryMerchent;
        ;WITH PrimaryMerchent
        AS (SELECT Usage.Account_Holder_ID,
                   UsageMerchent = Usage.MerchantCategories,
                   Usage.Usage,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Usage.Account_Holder_ID ORDER BY Usage.Usage DESC),
                   NumberOfMerchents = ROW_NUMBER() OVER (PARTITION BY Usage.Account_Holder_ID ORDER BY Usage.Usage ASC)
            FROM
            (
                SELECT Usage.Account_Holder_ID,
                       Usage.MerchantCategories,
                       Usage = SUM(Usage.Usage)
                FROM #Usage AS Usage
                WHERE Usage.UsageDateTime <= @CutPoint
                      AND DATEDIFF(DAY, Usage.UsageDateTime, @CutPoint) <= @ActivityDays
                      AND Usage.Usage > 0
                GROUP BY Usage.Account_Holder_ID,
                         Usage.MerchantCategories
            ) AS Usage )
    SELECT PrimaryMerchent.Account_Holder_ID,
           PrimaryMerchent = CASE
                                 WHEN 100.0 * PrimaryMerchent.Usage / TotalAmount.TotalUsage <= 50 THEN
                                     'Multi Merchent'
                                 ELSE
                                     PrimaryMerchent.UsageMerchent
                             END,
           PrimaryMerchent.Usage,
           PrimaryMerchentPercent = (100.0 * PrimaryMerchent.Usage / TotalAmount.TotalUsage),
           NumberOfMerchents = PrimaryMerchent.NumberOfMerchents
    INTO #PrimaryMerchent
    FROM PrimaryMerchent
        LEFT JOIN
        (
            SELECT PrimaryMerchent.Account_Holder_ID,
                   TotalUsage = SUM(PrimaryMerchent.Usage)
            FROM PrimaryMerchent
            GROUP BY PrimaryMerchent.Account_Holder_ID
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryMerchent.Account_Holder_ID
    WHERE RowNum = 1; --Take only the primary Usage 


    /*
--Statistics
SELECT PrimaryMerchent,
       NumberOfAH=COUNT(*),
       [Percent]=100.0 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #PrimaryMerchent
       ),
	   MoneyPercent = 100.0*SUM(Usage)/(SELECT SUM(Usage) FROM #PrimaryMerchent)
FROM #PrimaryMerchent
GROUP BY PrimaryMerchent
ORDER BY NumberOfAH DESC;
*/
    /***************************************************** LTV ******************************************************/
    --CREATE CLUSTERED INDEX a ON [Dev_Predict_DB].[dbo].[Output] (ID)

    IF OBJECT_ID('tempdb..#LTV') IS NOT NULL
        DROP TABLE #LTV;

    SELECT Account_Holder_ID = LTV.ID,
           LTV = LTV.PredictDelta,
           ChurnFromLTVModel = LTV.Churn
    INTO #LTV
    FROM
    (
        SELECT [Output].*,
               ROW_NUMBER() OVER (PARTITION BY [Output].ID ORDER BY [Output].CurrentDate DESC) AS RowNum
        FROM [Predict_DB].[dbo].[Output] AS [Output]

            /*BaseView*/
            INNER JOIN #BaseView AS BaseView
                ON BaseView.Account_Holder_ID = [Output].ID
        WHERE [Output].CurrentDate <= @CutPoint
              AND DATEDIFF(DAY, [Output].CurrentDate, @CutPoint) <= @ActivityDays
    ) AS LTV
    WHERE LTV.RowNum = 1;

    /***************************************************** Activity Segmentation ******************************************************/
    --CREATE CLUSTERED INDEX a ON [Predict_DB].Activity_Segmentation.Output_ActivitySegmentation (Account_Holder_ID)

    IF OBJECT_ID('tempdb..#ActivitySegmentation') IS NOT NULL
        DROP TABLE #ActivitySegmentation;

    SELECT Account_Holder_ID = ActivitySegmentation.Account_Holder_ID,
           ActivitySegment = ActivitySegmentation.ActivitySegment,
           FLC = ActivitySegmentation.FLC,
           FLW = ActivitySegmentation.FLW
    INTO #ActivitySegmentation
    FROM
    (
        SELECT Output_ActivitySegmentation.Account_Holder_ID,
               Output_ActivitySegmentation.ActivitySegment,
               Output_ActivitySegmentation.FLC,
               Output_ActivitySegmentation.FLW,
               ROW_NUMBER() OVER (PARTITION BY Output_ActivitySegmentation.Account_Holder_ID
                                  ORDER BY Output_ActivitySegmentation.CurrentDate DESC
                                 ) AS RowNum
        FROM [Predict_DB].Activity_Segmentation.Output_ActivitySegmentation AS Output_ActivitySegmentation
        WHERE EXISTS
        (
            SELECT BV.Account_Holder_ID
            FROM #BaseView AS BV
            WHERE Output_ActivitySegmentation.Account_Holder_ID = BV.Account_Holder_ID
        )
              --INNER JOIN #BaseView AS BaseView
              --    ON BaseView.Account_Holder_ID = Output_ActivitySegmentation.Account_Holder_ID
              AND Output_ActivitySegmentation.CurrentDate <= @CutPoint
              AND DATEDIFF(DAY, Output_ActivitySegmentation.CurrentDate, @CutPoint) <= @ActivityDays
    ) AS ActivitySegmentation
    WHERE ActivitySegmentation.RowNum = 1;


    /***************************************************** Churn *****************************************************/
    --CREATE CLUSTERED INDEX a ON [Predict_DB].[Churn].[Output_Churn_30] (Account_Holder_ID)
    --CREATE CLUSTERED INDEX a ON [Predict_DB].[Churn].[Output_Churn_90] (Account_Holder_ID)
    --CREATE CLUSTERED INDEX a ON [Predict_DB].[Churn].[Output_Churn_180] (Account_Holder_ID)


    /*Churn 30*/
    IF OBJECT_ID('tempdb..#Churn30') IS NOT NULL
        DROP TABLE #Churn30;

    SELECT Account_Holder_ID = Churn_30.Account_Holder_ID,
           Churn_30.RFProbsPredict_30,
           Churn_30.RFClassPredict_30
    INTO #Churn30
    FROM
    (
        SELECT Output_Churn_30.*,
               ROW_NUMBER() OVER (PARTITION BY Output_Churn_30.Account_Holder_ID
                                  ORDER BY Output_Churn_30.CurrentDate DESC
                                 ) AS RowNum
        FROM [Predict_DB].[Churn].[Output_Churn_30] AS Output_Churn_30
            INNER JOIN #BaseView AS BaseView
                ON BaseView.Account_Holder_ID = Output_Churn_30.Account_Holder_ID
        WHERE CurrentDate <= @CutPoint
              AND DATEDIFF(DAY, Output_Churn_30.CurrentDate, @CutPoint) <= @ActivityDays
    ) AS Churn_30
    WHERE Churn_30.RowNum = 1;

    /*Churn 90*/
    IF OBJECT_ID('tempdb..#Churn90') IS NOT NULL
        DROP TABLE #Churn90;

    SELECT Account_Holder_ID = Churn_90.Account_Holder_ID,
           Churn_90.RFProbsPredict_90,
           Churn_90.RFClassPredict_90
    INTO #Churn90
    FROM
    (
        SELECT Output_Churn_90.*,
               ROW_NUMBER() OVER (PARTITION BY Output_Churn_90.Account_Holder_ID
                                  ORDER BY Output_Churn_90.CurrentDate DESC
                                 ) AS RowNum
        FROM [Predict_DB].[Churn].[Output_Churn_90] AS Output_Churn_90
            INNER JOIN #BaseView AS BaseView
                ON BaseView.Account_Holder_ID = Output_Churn_90.Account_Holder_ID
        WHERE CurrentDate <= @CutPoint
              AND DATEDIFF(DAY, Output_Churn_90.CurrentDate, @CutPoint) <= @ActivityDays
    ) AS Churn_90
    WHERE Churn_90.RowNum = 1;

    /*Churn 180*/
    IF OBJECT_ID('tempdb..#Churn180') IS NOT NULL
        DROP TABLE #Churn180;

    SELECT Account_Holder_ID = Churn_180.Account_Holder_ID,
           Churn_180.RFProbsPredict_180,
           Churn_180.RFClassPredict_180
    INTO #Churn180
    FROM
    (
        SELECT Output_Churn_180.*,
               ROW_NUMBER() OVER (PARTITION BY Output_Churn_180.Account_Holder_ID
                                  ORDER BY Output_Churn_180.CurrentDate DESC
                                 ) AS RowNum
        FROM [Predict_DB].[Churn].[Output_Churn_180] AS Output_Churn_180
            INNER JOIN #BaseView AS BaseView
                ON BaseView.Account_Holder_ID = Output_Churn_180.Account_Holder_ID
        WHERE CurrentDate <= @CutPoint
              AND DATEDIFF(DAY, Output_Churn_180.CurrentDate, @CutPoint) <= @ActivityDays
    ) AS Churn_180
    WHERE Churn_180.RowNum = 1;

    /**************************************************Revenue***********************************************/
    IF OBJECT_ID('tempdb..#Revenue') IS NOT NULL
        DROP TABLE #Revenue;

    SELECT Account_Holder_ID = Revenue.Involved_Party_ID,
           RevenueAmountUSD = Revenue.Revenue_Amount_USD,
           TransactionDateTime = Revenue.Transaction_Datetime,
           Transaction_Code = Revenue.Transaction_Code,
           RevenueBearingEntityTypeID = Revenue.Revenue_Bearing_Entity_Type_ID,             --AH or Partner payed the revenue
           RevenueBearingCustomerCategoryID = Revenue.Revenue_Bearing_Customer_Category_ID, --If it's AH and BLS fees then distinct between receivers and payers AHs
                                                                                            --FirstVolume.FirstLoadEver,
           RevenueCategoryID = DimRevenueItems.Revenue_Category_ID,
           RevenueItem = DimRevenueItems.Revenue_Item,
           RevenueType = DimRevenueItems.Revenue_Type,
           RevenueSubCategory = DimRevenueItems.Revenue_Sub_Category,
           RevenueCategory = DimRevenueItems.Revenue_Category,
           RevenueActivity = DimRevenueItems.Revenue_Activity,
           RevenueLoaderID = Revenue.Revenue_Loader_ID,
           RevenueLoaderName = CorporateVerticals.Corporate_Name,
           RevenueVerticalName = CorporateVerticals.Vertical_Name,
           InvolvedPartyTier = Revenue.Involved_Party_Tier,
           IsFX = Revenue.Is_FX,
           IsCrossCountry = Revenue.Is_Cross_Country,
           FeeTypeCode = Revenue.Fee_Type_Code,
           IsChargedOffline = Revenue.Is_Charged_Offline,                                   --if the revenue was imidiet or in the end of the month
           ValueSegment = Revenue.Involved_Party_Value_Segment                              --Segment Value

    INTO #Revenue
    FROM [DW_Main].[dbo].[syn_active_Fact_Revenue] AS Revenue

        /* BaseView */
        INNER JOIN #BaseView AS BaseView
            ON Revenue.Involved_Party_ID = BaseView.Account_Holder_ID
               AND Revenue.Involved_Party_Type = 1

        /* Revenue_Items */
        LEFT JOIN [DW_Main].[dbo].Dim_Revenue_Items AS DimRevenueItems
            ON DimRevenueItems.Revenue_Item_ID = Revenue.Revenue_Item_ID

        /*Corporate Verticals*/
        LEFT JOIN [DW_Main].[dbo].Dim_Corporate_Verticals AS CorporateVerticals
            ON Revenue.Revenue_Loader_ID = CorporateVerticals.Corporate_ID
               AND CorporateVerticals.Corporate_Type = 2
    WHERE Revenue.Involved_Party_Type = 1;

    CREATE CLUSTERED COLUMNSTORE INDEX a ON #Revenue;

    /************************************************** Aggregate Revenue - Until Now ***********************************************/
    IF OBJECT_ID('tempdb..#RevenueUntilNow') IS NOT NULL
        DROP TABLE #RevenueUntilNow;

    SELECT Account_Holder_ID = Revenue.Account_Holder_ID,
           LastRevenueDateTime = MAX(Revenue.TransactionDateTime),
           DaysBetweenFirstRevenueToCutPoint = MAX(1.0 * DATEDIFF(HOUR, Revenue.TransactionDateTime, @CutPoint) / 24.0),
           DaysFromLastRevenueToCutPoint = MIN(1.0 * DATEDIFF(HOUR, Revenue.TransactionDateTime, @CutPoint) / 24.0),
           NumberOfDistinctTransactionCode_UntilNow = ISNULL(COUNT(DISTINCT Revenue.Transaction_Code), 1),
           NumberOfRevnueTransactions_UntilNow = COUNT(Revenue.Account_Holder_ID),
           PercentOfFX_UntilNow = 100.0 * (SUM(   CASE
                                                      WHEN Revenue.IsFX = 1 THEN
                                                          ABS(Revenue.RevenueAmountUSD)
                                                      ELSE
                                                          0
                                                  END
                                              ) + 1
                                          ) / (SUM(ABS(Revenue.RevenueAmountUSD)) + 1),
           NumberOfFX_UntilNow = SUM(Revenue.IsFX),
           PercentOfCrossCountry_UntilNow = 100.0 * (SUM(   CASE
                                                                WHEN Revenue.IsCrossCountry = 1 THEN
                                                                    ABS(Revenue.RevenueAmountUSD)
                                                                ELSE
                                                                    0
                                                            END
                                                        ) + 1
                                                    ) / (SUM(ABS(Revenue.RevenueAmountUSD)) + 1),
           NumberOfCrossCountry_UntilNow = SUM(Revenue.IsCrossCountry),
           SumRevenue_UntilNow = SUM(Revenue.RevenueAmountUSD),
           GeometricMeanRevenue_UntilNow = EXP(AVG(LOG(ABS(Revenue.RevenueAmountUSD) + 1))),
           SdRevenue_UntilNow = ISNULL(STDEV(Revenue.RevenueAmountUSD), -1),
           MeanRevenue_UntilNow = AVG(Revenue.RevenueAmountUSD),
           MaxRevenue_UntilNow = MAX(Revenue.RevenueAmountUSD),
           SumRevenueWorkingCapital_UntilNow = SUM(   CASE
                                                          WHEN Revenue.RevenueActivity = 'Working Capital' THEN
                                                              Revenue.RevenueAmountUSD
                                                          ELSE
                                                              0
                                                      END
                                                  ),
           SumRevenueUsage_UntilNow = SUM(   CASE
                                                 WHEN Revenue.RevenueActivity = 'Usage' THEN
                                                     Revenue.RevenueAmountUSD
                                                 ELSE
                                                     0
                                             END
                                         ),
           SumRevenueOther_UntilNow = SUM(   CASE
                                                 WHEN Revenue.RevenueActivity = 'Other' THEN
                                                     Revenue.RevenueAmountUSD
                                                 ELSE
                                                     0
                                             END
                                         ),
           SumRevenueVolume_UntilNow = SUM(   CASE
                                                  WHEN Revenue.RevenueActivity = 'Volume' THEN
                                                      Revenue.RevenueAmountUSD
                                                  ELSE
                                                      0
                                              END
                                          ),
           SumRevenueInternalMovements_UntilNow = SUM(   CASE
                                                             WHEN Revenue.RevenueActivity = 'Internal Movements' THEN
                                                                 Revenue.RevenueAmountUSD
                                                             ELSE
                                                                 0
                                                         END
                                                     ),
           SumRevenueFX_UntilNow = SUM(   CASE
                                              WHEN Revenue.IsFX = 1 THEN
                                                  Revenue.RevenueAmountUSD
                                              ELSE
                                                  0
                                          END
                                      ),
           SumRevenueCrossCountry_UntilNow = SUM(   CASE
                                                        WHEN Revenue.IsCrossCountry = 1 THEN
                                                            Revenue.RevenueAmountUSD
                                                        ELSE
                                                            0
                                                    END
                                                ),
           SumRevenueChargedOffline_UntilNow = SUM(   CASE
                                                          WHEN Revenue.IsChargedOffline = 1 THEN
                                                              Revenue.RevenueAmountUSD
                                                          ELSE
                                                              0
                                                      END
                                                  ),
           SumRevenueBearingEntity_UntilNow = SUM(   CASE
                                                         WHEN Revenue.RevenueBearingEntityTypeID = 2 THEN
                                                             Revenue.RevenueAmountUSD
                                                         ELSE
                                                             0
                                                     END
                                                 ),
           GeometricMeanRevenueWorkingCapital_UntilNow = ISNULL(
                                                                   EXP(AVG(LOG(   CASE
                                                                                      WHEN Revenue.RevenueActivity = 'Working Capital' THEN
                                                                                          ABS(Revenue.RevenueAmountUSD)
                                                                                          + 1
                                                                                      ELSE
                                                                                          NULL
                                                                                  END
                                                                              )
                                                                          )
                                                                      ),
                                                                   0
                                                               ),
           GeometricMeanRevenueUsage_UntilNow = ISNULL(EXP(AVG(LOG(   CASE
                                                                          WHEN Revenue.RevenueActivity = 'Usage' THEN
                                                                              ABS(Revenue.RevenueAmountUSD) + 1
                                                                          ELSE
                                                                              NULL
                                                                      END
                                                                  )
                                                              )
                                                          ),
                                                       0
                                                      ),
           GeometricMeanRevenueOther_UntilNow = ISNULL(EXP(AVG(LOG(   CASE
                                                                          WHEN Revenue.RevenueActivity = 'Other' THEN
                                                                              ABS(Revenue.RevenueAmountUSD) + 1
                                                                          ELSE
                                                                              NULL
                                                                      END
                                                                  )
                                                              )
                                                          ),
                                                       0
                                                      ),
           GeometricMeanRevenueVolume_UntilNow = ISNULL(EXP(AVG(LOG(   CASE
                                                                           WHEN Revenue.RevenueActivity = 'Volume' THEN
                                                                               ABS(Revenue.RevenueAmountUSD) + 1
                                                                           ELSE
                                                                               NULL
                                                                       END
                                                                   )
                                                               )
                                                           ),
                                                        0
                                                       ),
           GeometricMeanRevenueInternalMovements_UntilNow = ISNULL(
                                                                      EXP(AVG(LOG(   CASE
                                                                                         WHEN Revenue.RevenueActivity = 'Internal Movements' THEN
                                                                                             ABS(Revenue.RevenueAmountUSD)
                                                                                             + 1
                                                                                         ELSE
                                                                                             NULL
                                                                                     END
                                                                                 )
                                                                             )
                                                                         ),
                                                                      0
                                                                  ),
           GeometricMeanFX_UntilNow = ISNULL(EXP(AVG(LOG(   CASE
                                                                WHEN Revenue.IsFX = 1 THEN
                                                                    ABS(Revenue.RevenueAmountUSD) + 1
                                                                ELSE
                                                                    NULL
                                                            END
                                                        )
                                                    )
                                                ),
                                             0
                                            ),
           GeometricMeanCrossCountry_UntilNow = ISNULL(EXP(AVG(LOG(   CASE
                                                                          WHEN Revenue.IsCrossCountry = 1 THEN
                                                                              ABS(Revenue.RevenueAmountUSD) + 1
                                                                          ELSE
                                                                              NULL
                                                                      END
                                                                  )
                                                              )
                                                          ),
                                                       0
                                                      ),
           NumberOfBearingEntityID_UntilNow = ISNULL(COUNT(DISTINCT Revenue.RevenueBearingEntityTypeID), 0),
           NumberOfBearingCustomerCategoryID_UntilNow = ISNULL(
                                                                  COUNT(DISTINCT
                                                                           Revenue.RevenueBearingCustomerCategoryID
                                                                       ),
                                                                  0
                                                              )
    INTO #RevenueUntilNow
    FROM #Revenue AS Revenue
    WHERE TransactionDateTime <= @CutPoint
    GROUP BY Revenue.Account_Holder_ID;


    /************************************************** Aggregate Revenue - Recently ***********************************************/
    IF OBJECT_ID('tempdb..#RevenueRecently') IS NOT NULL
        DROP TABLE #RevenueRecently;

    SELECT Account_Holder_ID = Revenue.Account_Holder_ID,
           LastRevenueDateTime = MAX(Revenue.TransactionDateTime),
           DaysBetweenFirstRevenueToCutPoint = MAX(1.0 * DATEDIFF(HOUR, Revenue.TransactionDateTime, @CutPoint) / 24.0),
           DaysFromLastRevenueToCutPoint = MIN(1.0 * DATEDIFF(HOUR, Revenue.TransactionDateTime, @CutPoint) / 24.0),
           NumberOfDistinctTransactionCode_Recently = ISNULL(COUNT(DISTINCT Revenue.Transaction_Code), 1),
           NumberOfRevnueTransactions_Recently = COUNT(Revenue.Account_Holder_ID),
           PercentOfFX_Recently = 100.0 * (SUM(   CASE
                                                      WHEN Revenue.IsFX = 1 THEN
                                                          ABS(Revenue.RevenueAmountUSD)
                                                      ELSE
                                                          0
                                                  END
                                              ) + 1
                                          ) / (SUM(ABS(Revenue.RevenueAmountUSD)) + 1),
           NumberOfFX_Recently = SUM(Revenue.IsFX),
           PercentOfCrossCountry_Recently = 100.0 * (SUM(   CASE
                                                                WHEN Revenue.IsCrossCountry = 1 THEN
                                                                    ABS(Revenue.RevenueAmountUSD)
                                                                ELSE
                                                                    0
                                                            END
                                                        ) + 1
                                                    ) / (SUM(ABS(Revenue.RevenueAmountUSD)) + 1),
           NumberOfCrossCountry_Recently = SUM(Revenue.IsCrossCountry),
           SumRevenue_Recently = SUM(Revenue.RevenueAmountUSD),
           GeometricMeanRevenue_Recently = EXP(AVG(LOG(ABS(Revenue.RevenueAmountUSD) + 1))),
           SdRevenue_Recently = ISNULL(STDEV(Revenue.RevenueAmountUSD), -1),
           MeanRevenue_Recently = AVG(Revenue.RevenueAmountUSD),
           MaxRevenue_Recently = MAX(Revenue.RevenueAmountUSD),
           SumRevenueWorkingCapital_Recently = SUM(   CASE
                                                          WHEN Revenue.RevenueActivity = 'Working Capital' THEN
                                                              Revenue.RevenueAmountUSD
                                                          ELSE
                                                              0
                                                      END
                                                  ),
           SumRevenueUsage_Recently = SUM(   CASE
                                                 WHEN Revenue.RevenueActivity = 'Usage' THEN
                                                     Revenue.RevenueAmountUSD
                                                 ELSE
                                                     0
                                             END
                                         ),
           SumRevenueOther_Recently = SUM(   CASE
                                                 WHEN Revenue.RevenueActivity = 'Other' THEN
                                                     Revenue.RevenueAmountUSD
                                                 ELSE
                                                     0
                                             END
                                         ),
           SumRevenueVolume_Recently = SUM(   CASE
                                                  WHEN Revenue.RevenueActivity = 'Volume' THEN
                                                      Revenue.RevenueAmountUSD
                                                  ELSE
                                                      0
                                              END
                                          ),
           SumRevenueInternalMovements_Recently = SUM(   CASE
                                                             WHEN Revenue.RevenueActivity = 'Internal Movements' THEN
                                                                 Revenue.RevenueAmountUSD
                                                             ELSE
                                                                 0
                                                         END
                                                     ),
           SumRevenueFX_Recently = SUM(   CASE
                                              WHEN Revenue.IsFX = 1 THEN
                                                  Revenue.RevenueAmountUSD
                                              ELSE
                                                  0
                                          END
                                      ),
           SumRevenueCrossCountry_Recently = SUM(   CASE
                                                        WHEN Revenue.IsCrossCountry = 1 THEN
                                                            Revenue.RevenueAmountUSD
                                                        ELSE
                                                            0
                                                    END
                                                ),
           SumRevenueChargedOffline_Recently = SUM(   CASE
                                                          WHEN Revenue.IsChargedOffline = 1 THEN
                                                              Revenue.RevenueAmountUSD
                                                          ELSE
                                                              0
                                                      END
                                                  ),
           SumRevenueBearingEntity_Recently = SUM(   CASE
                                                         WHEN Revenue.RevenueBearingEntityTypeID = 2 THEN
                                                             Revenue.RevenueAmountUSD
                                                         ELSE
                                                             0
                                                     END
                                                 ),
           GeometricMeanRevenueWorkingCapital_Recently = ISNULL(
                                                                   EXP(AVG(LOG(   CASE
                                                                                      WHEN Revenue.RevenueActivity = 'Working Capital' THEN
                                                                                          ABS(Revenue.RevenueAmountUSD)
                                                                                          + 1
                                                                                      ELSE
                                                                                          NULL
                                                                                  END
                                                                              )
                                                                          )
                                                                      ),
                                                                   0
                                                               ),
           GeometricMeanRevenueUsage_Recently = ISNULL(EXP(AVG(LOG(   CASE
                                                                          WHEN Revenue.RevenueActivity = 'Usage' THEN
                                                                              ABS(Revenue.RevenueAmountUSD) + 1
                                                                          ELSE
                                                                              NULL
                                                                      END
                                                                  )
                                                              )
                                                          ),
                                                       0
                                                      ),
           GeometricMeanRevenueOther_Recently = ISNULL(EXP(AVG(LOG(   CASE
                                                                          WHEN Revenue.RevenueActivity = 'Other' THEN
                                                                              ABS(Revenue.RevenueAmountUSD) + 1
                                                                          ELSE
                                                                              NULL
                                                                      END
                                                                  )
                                                              )
                                                          ),
                                                       0
                                                      ),
           GeometricMeanRevenueVolume_Recently = ISNULL(EXP(AVG(LOG(   CASE
                                                                           WHEN Revenue.RevenueActivity = 'Volume' THEN
                                                                               ABS(Revenue.RevenueAmountUSD) + 1
                                                                           ELSE
                                                                               NULL
                                                                       END
                                                                   )
                                                               )
                                                           ),
                                                        0
                                                       ),
           GeometricMeanRevenueInternalMovements_Recently = ISNULL(
                                                                      EXP(AVG(LOG(   CASE
                                                                                         WHEN Revenue.RevenueActivity = 'Internal Movements' THEN
                                                                                             ABS(Revenue.RevenueAmountUSD)
                                                                                             + 1
                                                                                         ELSE
                                                                                             NULL
                                                                                     END
                                                                                 )
                                                                             )
                                                                         ),
                                                                      0
                                                                  ),
           GeometricMeanFX_Recently = ISNULL(EXP(AVG(LOG(   CASE
                                                                WHEN Revenue.IsFX = 1 THEN
                                                                    ABS(Revenue.RevenueAmountUSD) + 1
                                                                ELSE
                                                                    NULL
                                                            END
                                                        )
                                                    )
                                                ),
                                             0
                                            ),
           GeometricMeanCrossCountry_Recently = ISNULL(EXP(AVG(LOG(   CASE
                                                                          WHEN Revenue.IsCrossCountry = 1 THEN
                                                                              ABS(Revenue.RevenueAmountUSD) + 1
                                                                          ELSE
                                                                              NULL
                                                                      END
                                                                  )
                                                              )
                                                          ),
                                                       0
                                                      ),
           NumberOfBearingEntityID_Recently = ISNULL(COUNT(DISTINCT Revenue.RevenueBearingEntityTypeID), 0),
           NumberOfBearingCustomerCategoryID_Recently = ISNULL(
                                                                  COUNT(DISTINCT
                                                                           Revenue.RevenueBearingCustomerCategoryID
                                                                       ),
                                                                  0
                                                              )
    INTO #RevenueRecently
    FROM #Revenue AS Revenue
    WHERE Revenue.TransactionDateTime <= @CutPoint
          AND DATEDIFF(DAY, Revenue.TransactionDateTime, @CutPoint) <= 90
    GROUP BY Revenue.Account_Holder_ID;

    /**************************************************Primary Revenue Loader***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryRevenueLoaderName') IS NOT NULL
        DROP TABLE #PrimaryRevenueLoaderName;
        ;WITH PrimaryRevenueLoaderName
        AS (SELECT Revenue.Account_Holder_ID,
                   RevenueLoaderName = ISNULL(Revenue.RevenueLoaderName, 'Billing Services'),
                   RevenueVerticalName = ISNULL(Revenue.RevenueVerticalName, 'Billing Services'),
                   Revenue.RevenueAmount,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                               ORDER BY Revenue.RevenueAmount DESC
                                              ),
                   NumberOfRevenueLoaderName = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                                                  ORDER BY Revenue.RevenueAmount ASC
                                                                 )
            FROM
            (
                SELECT Revenue.Account_Holder_ID,
                       RevenueLoaderName = Revenue.RevenueLoaderName,
                       RevenueVerticalName = MAX(Revenue.RevenueVerticalName),
                       RevenueAmount = SUM(Revenue.RevenueAmountUSD)
                FROM #Revenue AS Revenue
                WHERE Revenue.TransactionDateTime <= @CutPoint
                      AND DATEDIFF(DAY, Revenue.TransactionDateTime, @CutPoint) <= @ActivityDays
                      AND Revenue.RevenueAmountUSD > 0
                      AND Revenue.RevenueLoaderID IS NOT NULL
                GROUP BY Revenue.Account_Holder_ID,
                         Revenue.RevenueLoaderName
            ) AS Revenue )
    SELECT PrimaryRevenueLoaderName.Account_Holder_ID,
           PrimaryRevenueLoaderName = CASE
                                          WHEN 100.0 * PrimaryRevenueLoaderName.RevenueAmount
                                               / TotalAmount.TotalRevenue <= 50 THEN
                                              'Multi Revenue Loaders'
                                          ELSE
                                              PrimaryRevenueLoaderName.RevenueLoaderName
                                      END,
           PrimaryRevenueVerticalName = CASE
                                            WHEN 100.0 * PrimaryRevenueLoaderName.RevenueAmount
                                                 / TotalAmount.TotalRevenue <= 50 THEN
                                                'Multi Revenue Verticals'
                                            ELSE
                                                PrimaryRevenueLoaderName.RevenueVerticalName
                                        END,
           RevenueLoaderName = PrimaryRevenueLoaderName.RevenueLoaderName,
           PrimaryRevenuePercent = (100.0 * PrimaryRevenueLoaderName.RevenueAmount / TotalAmount.TotalRevenue),
           NumberOfRevenueLoaderName = PrimaryRevenueLoaderName.NumberOfRevenueLoaderName,
           RevenueAmount = PrimaryRevenueLoaderName.RevenueAmount
    INTO #PrimaryRevenueLoaderName
    FROM PrimaryRevenueLoaderName
        LEFT JOIN
        (
            SELECT PrimaryRevenueLoaderName.Account_Holder_ID,
                   TotalRevenue = SUM(PrimaryRevenueLoaderName.RevenueAmount)
            FROM PrimaryRevenueLoaderName
            GROUP BY PrimaryRevenueLoaderName.Account_Holder_ID
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryRevenueLoaderName.Account_Holder_ID
    WHERE RowNum = 1; --Take only the primary Primary Usage 

    /*
--Statistics
SELECT PrimaryRevenueLoaderName,
       NumberOfAH=COUNT(*),
       [Percent]=100.0 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #PrimaryRevenueLoaderName
       ),
	   MoneyPercent = 100.0*SUM(RevenueAmount)/(SELECT SUM(RevenueAmount) FROM #PrimaryRevenueLoaderName)
FROM #PrimaryRevenueLoaderName
GROUP BY PrimaryRevenueLoaderName
ORDER BY NumberOfAH DESC;
*/
    /**************************************************Primary Revenue Activity***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryRevenueActivity') IS NOT NULL
        DROP TABLE #PrimaryRevenueActivity;
        ;WITH PrimaryRevenueActivity
        AS (SELECT Revenue.Account_Holder_ID,
                   RevenueActivity = Revenue.RevenueActivity,
                   Revenue.RevenueAmount,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                               ORDER BY Revenue.RevenueAmount DESC
                                              ),
                   NumberOfRevenueActivities = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                                                  ORDER BY Revenue.RevenueAmount ASC
                                                                 )
            FROM
            (
                SELECT Revenue.Account_Holder_ID,
                       Revenue.RevenueActivity,
                       RevenueAmount = SUM(Revenue.RevenueAmountUSD)
                FROM #Revenue AS Revenue
                WHERE Revenue.TransactionDateTime <= @CutPoint
                      AND DATEDIFF(DAY, Revenue.TransactionDateTime, @CutPoint) <= @ActivityDays
                      AND Revenue.RevenueAmountUSD > 0
                GROUP BY Revenue.Account_Holder_ID,
                         Revenue.RevenueActivity
            ) AS Revenue )
    SELECT PrimaryRevenueActivity.Account_Holder_ID,
           PrimaryRevenueActivity = CASE
                                        WHEN 100.0 * PrimaryRevenueActivity.RevenueAmount / TotalAmount.TotalRevenue <= 50 THEN
                                            'Multi Revenue Activity'
                                        ELSE
                                            PrimaryRevenueActivity.RevenueActivity
                                    END,
           RevenueActivity = PrimaryRevenueActivity.RevenueActivity,
           PrimaryRevenuePercent = (100.0 * PrimaryRevenueActivity.RevenueAmount / TotalAmount.TotalRevenue),
           NumberOfRevenueActivities = PrimaryRevenueActivity.NumberOfRevenueActivities,
           RevenueAmount = PrimaryRevenueActivity.RevenueAmount
    INTO #PrimaryRevenueActivity
    FROM PrimaryRevenueActivity
        LEFT JOIN
        (
            SELECT PrimaryRevenueActivity.Account_Holder_ID,
                   TotalRevenue = SUM(PrimaryRevenueActivity.RevenueAmount)
            FROM PrimaryRevenueActivity
            GROUP BY PrimaryRevenueActivity.Account_Holder_ID
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryRevenueActivity.Account_Holder_ID
    WHERE RowNum = 1; --Take only the primary Primary Usage 

    /*
--Statistics
SELECT PrimaryRevenueActivity,
       NumberOfAH=COUNT(*),
       [Percent]=100.0 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #PrimaryRevenueActivity
       ),
	   MoneyPercent = 100.0*SUM(RevenueAmount)/(SELECT SUM(RevenueAmount) FROM #PrimaryRevenueActivity)
FROM #PrimaryRevenueActivity
GROUP BY PrimaryRevenueActivity
ORDER BY NumberOfAH DESC;
*/
    /**************************************************Primary Revenue Category***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryRevenueCategory') IS NOT NULL
        DROP TABLE #PrimaryRevenueCategory;
        ;WITH PrimaryRevenueCategory
        AS (SELECT Revenue.Account_Holder_ID,
                   RevenueCategory = Revenue.RevenueCategory,
                   Revenue.RevenueAmount,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                               ORDER BY Revenue.RevenueAmount DESC
                                              ),
                   NumberOfRevenueCategories = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                                                  ORDER BY Revenue.RevenueAmount ASC
                                                                 )
            FROM
            (
                SELECT Revenue.Account_Holder_ID,
                       Revenue.RevenueCategory,
                       RevenueAmount = SUM(Revenue.RevenueAmountUSD)
                FROM #Revenue AS Revenue
                WHERE Revenue.TransactionDateTime <= @CutPoint
                      AND DATEDIFF(DAY, Revenue.TransactionDateTime, @CutPoint) <= @ActivityDays
                      AND Revenue.RevenueAmountUSD > 0
                GROUP BY Revenue.Account_Holder_ID,
                         Revenue.RevenueCategory
            ) AS Revenue )
    SELECT PrimaryRevenueCategory.Account_Holder_ID,
           PrimaryRevenueCategory = CASE
                                        WHEN 100.0 * PrimaryRevenueCategory.RevenueAmount / TotalAmount.TotalRevenue <= 50 THEN
                                            'Multi Revenue Category'
                                        ELSE
                                            PrimaryRevenueCategory.RevenueCategory
                                    END,
           PrimaryRevenueCategory.RevenueCategory,
           PrimaryRevenuePercent = (100.0 * PrimaryRevenueCategory.RevenueAmount / TotalAmount.TotalRevenue),
           NumberOfRevenueCategories = PrimaryRevenueCategory.NumberOfRevenueCategories,
           RevenueAmount = PrimaryRevenueCategory.RevenueAmount
    INTO #PrimaryRevenueCategory
    FROM PrimaryRevenueCategory
        LEFT JOIN
        (
            SELECT PrimaryRevenueCategory.Account_Holder_ID,
                   TotalRevenue = SUM(PrimaryRevenueCategory.RevenueAmount)
            FROM PrimaryRevenueCategory
            GROUP BY PrimaryRevenueCategory.Account_Holder_ID
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryRevenueCategory.Account_Holder_ID
    WHERE RowNum = 1;
    /*
--Statistics
SELECT PrimaryRevenueCategory,
       NumberOfAH=COUNT(*),
       [Percent]=100.0 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #PrimaryRevenueCategory
       ),
	   MoneyPercent = 100.0*SUM(RevenueAmount)/(SELECT SUM(RevenueAmount) FROM #PrimaryRevenueCategory)
FROM #PrimaryRevenueCategory
GROUP BY PrimaryRevenueCategory
ORDER BY NumberOfAH DESC;
*/
    /**************************************************Primary Revenue Sub Category***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryRevenueSubCategory') IS NOT NULL
        DROP TABLE #PrimaryRevenueSubCategory;
        ;WITH PrimaryRevenueSubCategory
        AS (SELECT Revenue.Account_Holder_ID,
                   RevenueSubCategory = Revenue.RevenueSubCategory,
                   Revenue.RevenueAmount,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                               ORDER BY Revenue.RevenueAmount DESC
                                              ),
                   NumberOfRevenueCategories = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                                                  ORDER BY Revenue.RevenueAmount ASC
                                                                 )
            FROM
            (
                SELECT Revenue.Account_Holder_ID,
                       Revenue.RevenueSubCategory,
                       RevenueAmount = SUM(Revenue.RevenueAmountUSD)
                FROM #Revenue AS Revenue
                WHERE Revenue.TransactionDateTime <= @CutPoint
                      AND DATEDIFF(DAY, Revenue.TransactionDateTime, @CutPoint) <= @ActivityDays
                      AND Revenue.RevenueAmountUSD > 0
                GROUP BY Revenue.Account_Holder_ID,
                         Revenue.RevenueSubCategory
            ) AS Revenue )
    SELECT PrimaryRevenueSubCategory.Account_Holder_ID,
           PrimaryRevenureSubCategory = CASE
                                            WHEN 100.0 * PrimaryRevenueSubCategory.RevenueAmount
                                                 / TotalAmount.TotalRevenue <= 50 THEN
                                                'Multi Revenue Sub Category'
                                            ELSE
                                                PrimaryRevenueSubCategory.RevenueSubCategory
                                        END,
           PrimaryRevenueSubCategory.RevenueSubCategory,
           PrimaryRevenuePercent = (100.0 * PrimaryRevenueSubCategory.RevenueAmount / TotalAmount.TotalRevenue),
           NumberOfRevenueSubCategories = PrimaryRevenueSubCategory.NumberOfRevenueCategories
    INTO #PrimaryRevenueSubCategory
    FROM PrimaryRevenueSubCategory
        LEFT JOIN
        (
            SELECT PrimaryRevenueSubCategory.Account_Holder_ID,
                   TotalRevenue = SUM(PrimaryRevenueSubCategory.RevenueAmount)
            FROM PrimaryRevenueSubCategory
            GROUP BY PrimaryRevenueSubCategory.Account_Holder_ID
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryRevenueSubCategory.Account_Holder_ID
    WHERE RowNum = 1;


    /**************************************************Primary Revenue Type***********************************************/
    IF OBJECT_ID('tempdb..#PrimaryRevenueType') IS NOT NULL
        DROP TABLE #PrimaryRevenueType;
        ;WITH PrimaryRevenueType
        AS (SELECT Revenue.Account_Holder_ID,
                   RevenueType = Revenue.RevenueType,
                   Revenue.RevenueAmount,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                               ORDER BY Revenue.RevenueAmount DESC
                                              ),
                   NumberOfRevenueCategories = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                                                  ORDER BY Revenue.RevenueAmount ASC
                                                                 )
            FROM
            (
                SELECT Revenue.Account_Holder_ID,
                       Revenue.RevenueType,
                       RevenueAmount = SUM(Revenue.RevenueAmountUSD)
                FROM #Revenue AS Revenue
                WHERE Revenue.TransactionDateTime <= @CutPoint
                      AND DATEDIFF(DAY, Revenue.TransactionDateTime, @CutPoint) <= @ActivityDays
                      AND Revenue.RevenueAmountUSD > 0
                GROUP BY Revenue.Account_Holder_ID,
                         Revenue.RevenueType
            ) AS Revenue )
    SELECT PrimaryRevenueType.Account_Holder_ID,
           PrimaryRevenueType = CASE
                                    WHEN 100.0 * PrimaryRevenueType.RevenueAmount / TotalAmount.TotalRevenue <= 50 THEN
                                        'Multi Revenue Type'
                                    ELSE
                                        PrimaryRevenueType.RevenueType
                                END,
           PrimaryRevenueType.RevenueType,
           PrimaryRevenuePercent = (100.0 * PrimaryRevenueType.RevenueAmount / TotalAmount.TotalRevenue),
           NumberOfRevenueTypes = PrimaryRevenueType.NumberOfRevenueCategories
    INTO #PrimaryRevenueType
    FROM PrimaryRevenueType
        LEFT JOIN
        (
            SELECT PrimaryRevenueType.Account_Holder_ID,
                   TotalRevenue = SUM(PrimaryRevenueType.RevenueAmount)
            FROM PrimaryRevenueType
            GROUP BY PrimaryRevenueType.Account_Holder_ID
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryRevenueType.Account_Holder_ID
    WHERE RowNum = 1;

    /**************************************************Primary Revenue Item***********************************************/

    IF OBJECT_ID('tempdb..#PrimaryRevenueItem') IS NOT NULL
        DROP TABLE #PrimaryRevenueItem;
        ;WITH PrimaryRevenueItem
        AS (SELECT Revenue.Account_Holder_ID,
                   RevenueItem = Revenue.RevenueItem,
                   Revenue.RevenueAmount,
                   RowNum = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                               ORDER BY Revenue.RevenueAmount DESC
                                              ),
                   NumberOfRevenueCategories = ROW_NUMBER() OVER (PARTITION BY Revenue.Account_Holder_ID
                                                                  ORDER BY Revenue.RevenueAmount ASC
                                                                 )
            FROM
            (
                SELECT Revenue.Account_Holder_ID,
                       Revenue.RevenueItem,
                       RevenueAmount = SUM(Revenue.RevenueAmountUSD)
                FROM #Revenue AS Revenue
                WHERE Revenue.TransactionDateTime <= @CutPoint
                      AND DATEDIFF(DAY, Revenue.TransactionDateTime, @CutPoint) <= @ActivityDays
                      AND Revenue.RevenueAmountUSD > 0
                GROUP BY Revenue.Account_Holder_ID,
                         Revenue.RevenueItem
            ) AS Revenue )
    SELECT PrimaryRevenueItem.Account_Holder_ID,
           PrimaryRevenueItem = CASE
                                    WHEN 100.0 * PrimaryRevenueItem.RevenueAmount / TotalAmount.TotalRevenue <= 50 THEN
                                        'Multi Revenue Item'
                                    ELSE
                                        PrimaryRevenueItem.RevenueItem
                                END,
           PrimaryRevenueItem.RevenueItem,
           PrimaryRevenuePercent = (100.0 * PrimaryRevenueItem.RevenueAmount / TotalAmount.TotalRevenue),
           NumberOfRevenueItems = PrimaryRevenueItem.NumberOfRevenueCategories
    INTO #PrimaryRevenueItem
    FROM PrimaryRevenueItem
        LEFT JOIN
        (
            SELECT PrimaryRevenueItem.Account_Holder_ID,
                   TotalRevenue = SUM(PrimaryRevenueItem.RevenueAmount)
            FROM PrimaryRevenueItem
            GROUP BY PrimaryRevenueItem.Account_Holder_ID
        ) AS TotalAmount
            ON TotalAmount.Account_Holder_ID = PrimaryRevenueItem.Account_Holder_ID
    WHERE RowNum = 1;


    /**************************************************Internal Movements***********************************************/
    /*Aggregationg Internal Movements for Payers*/
    DROP TABLE IF EXISTS #InternalMovements_Payer;
    SELECT Account_Holder_ID = FIM.Payer_Involved_Party_ID,
           Payer_InternalMovementAmount = SUM(FIM.Internal_Movement_Amount_USD),
           Payer_NumberOfInternalMovementAmount = COUNT(FIM.Internal_Movement_Amount_USD)
    INTO #InternalMovements_Payer
    FROM DW_Main.dbo.syn_active_Fact_Internal_Movements AS FIM

        /*BaseView*/
        INNER JOIN #BaseView AS BaseView
            ON FIM.Payer_Involved_Party_ID = BaseView.Account_Holder_ID
    WHERE FIM.Source_Data = 'Funds Consolidation'
          AND FIM.Payer_Involved_Party_Type = 1
          AND DATEDIFF(DAY, FIM.Internal_Movement_DateTime, @CutPoint) >= 0
          AND DATEDIFF(DAY, FIM.Internal_Movement_DateTime, @CutPoint) <= @ActivityDays
    GROUP BY FIM.Payer_Involved_Party_ID;

    /*Aggregationg Internal Movements for payee*/
    DROP TABLE IF EXISTS #InternalMovements_Payee;
    SELECT Account_Holder_ID = FIM.Payee_Involved_Party_ID,
           Payee_InternalMovementAmount = SUM(FIM.Internal_Movement_Amount_USD),
           Payee_NumberOfInternalMovementAmount = COUNT(FIM.Internal_Movement_Amount_USD)
    INTO #InternalMovements_Payee
    FROM DW_Main.dbo.syn_active_Fact_Internal_Movements AS FIM

        /*BaseView*/
        INNER JOIN #BaseView AS BaseView
            ON FIM.Payer_Involved_Party_ID = BaseView.Account_Holder_ID
    WHERE FIM.Source_Data = 'Funds Consolidation'
          AND FIM.Payer_Involved_Party_Type = 1
          AND DATEDIFF(DAY, FIM.Internal_Movement_DateTime, @CutPoint) >= 0
          AND DATEDIFF(DAY, FIM.Internal_Movement_DateTime, @CutPoint) <= @ActivityDays
    GROUP BY FIM.Payee_Involved_Party_ID;


    /**************************************************Manage Currencies***********************************************/
    DROP TABLE IF EXISTS #ManageCurrencies;
    SELECT Account_Holder_ID = FIM.Payer_Involved_Party_ID,
           SumManageCurrencies = SUM(FIM.Internal_Movement_Amount_USD),
           GeometricMeanManageCurrencies = EXP(AVG(LOG(ABS(FIM.Internal_Movement_Amount_USD) + 1))),
           NumberOfManageCurrencies = COUNT(FIM.Internal_Movement_Amount_USD)
    INTO #ManageCurrencies
    FROM DW_Main.dbo.syn_active_Fact_Internal_Movements AS FIM

        /*BaseView*/
        INNER JOIN #BaseView AS BaseView
            ON FIM.Payer_Involved_Party_ID = BaseView.Account_Holder_ID
    WHERE FIM.Source_Data = 'Manage Currencies'
          AND FIM.Payer_Involved_Party_Type = 1
          AND DATEDIFF(DAY, FIM.Internal_Movement_DateTime, @CutPoint) >= 0
          AND DATEDIFF(DAY, FIM.Internal_Movement_DateTime, @CutPoint) <= @ActivityDays
    GROUP BY FIM.Payer_Involved_Party_ID;


    /************************************************** Stores ***********************************************/
    DROP TABLE IF EXISTS #Stores;
    SELECT Account_Holder_ID = Stores.AH_ID,
           NumberOfStores = COUNT(Stores.AH_ID),
           NumberOfStoresRecently = SUM(   CASE
                                               WHEN DATEDIFF(DAY, Stores.Store_Created_Date_Time, @CutPoint) <= 30 THEN
                                                   1
                                               ELSE
                                                   0
                                           END
                                       ),
           LastStoreCreationToCutPointDaysDifference = MIN(1.0
                                                           * DATEDIFF(HOUR, Stores.Store_Created_Date_Time, @CutPoint)
                                                           / 24.0
                                                          ),
           FirstStoreCreationToCutPointDaysDifference = MAX(1.0
                                                            * DATEDIFF(HOUR, Stores.Store_Created_Date_Time, @CutPoint)
                                                            / 24.0
                                                           )
    INTO #Stores
    FROM DW_Main.dbo.syn_active_Dim_Stores AS Stores

        /*BaseView*/
        INNER JOIN #BaseView AS BaseView
            ON Stores.AH_ID = BaseView.Account_Holder_ID
    WHERE Stores.Store_Created_Date_Time <= @CutPoint
    GROUP BY Stores.AH_ID;


    /************************************************** Opportunities ***********************************************/


    DROP TABLE IF EXISTS #Opportunity;
    SELECT syn_active_Fact_SF_Opportunity.Opportunity_ID,
           syn_active_Fact_SF_Opportunity.Opportunity_SF_Account_ID,
           syn_active_Fact_SF_Opportunity.Opportunity_SF_Account_Type_ID,
           Dim_SF_Opportunity_Stages.Opportunity_Stage_Description,
           Dim_SF_Opportunity_Stage_Details.Opprtunity_Stage_Details_Description,
           Dim_Generic_Corporates.Generic_Corporate_Name,
           Dim_SF_Opportunity_Product_Types.Opportunity_Product_Type_Description,
           Dim_SF_Opportunity_Types.Opportunity_Type_Description,
           Dim_SF_Opportunity_Close_Lost_Reasons.Opportunity_Closed_Lost_Reason_Description,
           syn_active_Fact_SF_Opportunity.Opportunity_Name,
           syn_active_Fact_SF_Opportunity.Opportunity_Probability,                 --the probablility of fulfiling the opportunity. Values should appear from 0.0 to 100.0
           syn_active_Fact_SF_Opportunity.Opportunity_Monthly_Volume_Commitment_Amount_USD,
           syn_active_Fact_SF_Opportunity.Opportunity_Expected_Annual_Volume_Amount_USD,
           syn_active_Fact_SF_Opportunity.Opportunity_Expected_Annual_Revenue_Amount_USD,
           syn_active_Fact_SF_Opportunity.Opportunity_Forecast_Annual_Revenue_Amount_USD,
           syn_active_Fact_SF_Opportunity.Opportunity_Create_Datetime,
           syn_active_Fact_SF_Opportunity.Opportunity_Close_Date,
           syn_active_Fact_SF_Opportunity.Opportunity_Last_Modified_Datetime,
           syn_active_Fact_SF_Opportunity.Opportunity_Estimated_Won_Achieved_Date, --Estimation when we're about to fulfil the opportunity. For example - we've signed with the AH that he'll receive 100K$. This date estimates when he'll reach 100K$
           syn_active_Fact_SF_Opportunity.Opportunity_Offered_Fee,                 --The offered fee we're offering the AH.
           syn_active_Fact_SF_Opportunity.Opportunity_Last_Won_Achieved_Date,      --Retrieving data. Wait a few seconds and try to cut or copy again.
           syn_active_Fact_SF_Opportunity.Opportunity_Approval_Date,               --business approval date - to proceed with the opportunity,
                                                                                   --Opportunity_Estimation_Status_ID,
           Dim_SF_Opportunity_Estimation_Statuses.Opportunity_Estimation_Status_Description,
           Diligence_Statuses = Dim_SF_Opportunity_Due_Diligence_Statuses.Opportunity_Due_Diligence_Status_Description,
           Contract_Statuses = Dim_SF_Opportunity_Contract_Statuses.Opportunity_Due_Diligence_Status_Description
    INTO #Opportunity
    FROM [DW_Main].dbo.syn_active_Fact_SF_Opportunity AS syn_active_Fact_SF_Opportunity

        /*syn active Dim Involved Parties*/
        --inner JOIN (SELECT DISTINCT Master_Account_Holder_ID,Involved_Party_ID FROM  DW_Main.dbo.syn_active_Dim_Involved_Parties WHERE Involved_Party_Type=1) AS syn_active_Dim_Involved_Parties
        --    ON syn_active_Fact_SF_Opportunity.Opportunity_SF_Account_ID = syn_active_Dim_Involved_Parties.Master_Account_Holder_ID

        /*Dim SF Opportunity Stages*/
        LEFT JOIN [DW_Main].dbo.Dim_SF_Opportunity_Stages AS Dim_SF_Opportunity_Stages
            ON syn_active_Fact_SF_Opportunity.Opportunity_Stage_ID = Dim_SF_Opportunity_Stages.Opportunity_Stage_ID

        /*Dim SF Opportunity Stages Details*/
        LEFT JOIN [DW_Main].dbo.Dim_SF_Opportunity_Stage_Details AS Dim_SF_Opportunity_Stage_Details
            ON syn_active_Fact_SF_Opportunity.Opprtunity_Stage_Details_ID = Dim_SF_Opportunity_Stage_Details.Opprtunity_Stage_Details_ID

        /*Dim Generic Corporates*/
        LEFT JOIN [DW_Main].dbo.Dim_Generic_Corporates AS Dim_Generic_Corporates
            ON syn_active_Fact_SF_Opportunity.Opportunity_Source_Of_Funds_SF_Account_ID = Dim_Generic_Corporates.Generic_Corporate_SF_ID

        /*Dim SF Opportunity Product Types*/
        LEFT JOIN [DW_Main].dbo.Dim_SF_Opportunity_Product_Types AS Dim_SF_Opportunity_Product_Types
            ON syn_active_Fact_SF_Opportunity.Opportunity_Product_Type_ID = Dim_SF_Opportunity_Product_Types.Opportunity_Product_Type_ID

        /*Dim SF Opportunity Types*/
        LEFT JOIN [DW_Main].dbo.Dim_SF_Opportunity_Types AS Dim_SF_Opportunity_Types
            ON syn_active_Fact_SF_Opportunity.Opportunity_Type_ID = Dim_SF_Opportunity_Types.Opportunity_Type_ID

        /*Dim SF Opportunity Estimation Statuses*/
        LEFT JOIN [DW_Main].dbo.Dim_SF_Opportunity_Estimation_Statuses AS Dim_SF_Opportunity_Estimation_Statuses
            ON syn_active_Fact_SF_Opportunity.Opportunity_Estimation_Status_ID = Dim_SF_Opportunity_Estimation_Statuses.Opportunity_Estimation_Status_ID

        /*Dim SF Opportunity Due Diligence Statuses*/
        LEFT JOIN [DW_Main].dbo.Dim_SF_Opportunity_Due_Diligence_Statuses AS Dim_SF_Opportunity_Due_Diligence_Statuses
            ON syn_active_Fact_SF_Opportunity.Opportunity_Due_Diligence_Status_ID = Dim_SF_Opportunity_Due_Diligence_Statuses.Opportunity_Due_Diligence_Status_ID

        /*Dim SF Opportunity Contract Statuses*/
        LEFT JOIN [DW_Main].dbo.Dim_SF_Opportunity_Due_Diligence_Statuses AS Dim_SF_Opportunity_Contract_Statuses
            ON syn_active_Fact_SF_Opportunity.Opportunity_Contract_Status_ID = Dim_SF_Opportunity_Contract_Statuses.Opportunity_Due_Diligence_Status_ID

        /*Dim SF Opportunity Close Lost Reasons*/
        LEFT JOIN [DW_Main].dbo.Dim_SF_Opportunity_Close_Lost_Reasons AS Dim_SF_Opportunity_Close_Lost_Reasons
            ON syn_active_Fact_SF_Opportunity.Opportunity_Closed_Lost_Reason_ID = Dim_SF_Opportunity_Close_Lost_Reasons.Opportunity_Closed_Lost_Reason_ID
    WHERE syn_active_Fact_SF_Opportunity.Opportunity_Last_Modified_Datetime <= @CutPoint
          AND DATEDIFF(DAY, syn_active_Fact_SF_Opportunity.Opportunity_Last_Modified_Datetime, @CutPoint) >= 0
          AND DATEDIFF(DAY, syn_active_Fact_SF_Opportunity.Opportunity_Last_Modified_Datetime, @CutPoint) <= 2
                                                                                                             * @ActivityDays;

    /* Opportunity Stage Description*/
    /*
SELECT Contract_Statuses,
       COUNT(*) AS num,
       100.00 * COUNT(*) /
       (
           SELECT COUNT(*) FROM #Opportunity
       ) AS [Percent]
FROM #Opportunity
GROUP BY Contract_Statuses
ORDER BY [Percent] DESC;
*/

    DROP TABLE IF EXISTS #OpportunityAggregated;

    SELECT Opportunity_SF_Account_ID,
           Opportunity_Probability = ISNULL(AVG(Opportunity_Probability), 0),
           Opportunity_Monthly_Volume_Commitment_Amount_USD = ISNULL(
                                                                        SUM(Opportunity_Monthly_Volume_Commitment_Amount_USD),
                                                                        0
                                                                    ),
           Opportunity_Expected_Annual_Volume_Amount_USD = ISNULL(SUM(Opportunity_Expected_Annual_Volume_Amount_USD), 0),
           Opportunity_Expected_Annual_Revenue_Amount_USD = ISNULL(
                                                                      SUM(Opportunity_Expected_Annual_Revenue_Amount_USD),
                                                                      0
                                                                  ),
           Opportunity_Forecast_Annual_Revenue_Amount_USD = ISNULL(
                                                                      SUM(Opportunity_Forecast_Annual_Revenue_Amount_USD),
                                                                      0
                                                                  )
    INTO #OpportunityAggregated
    FROM #Opportunity
    GROUP BY Opportunity_SF_Account_ID;

    /************************************************** Primary Stage Description ***********************************************/
    DROP TABLE IF EXISTS #PrimaryStageDescription;
    WITH FrequencyStageDescription
    AS (SELECT Opportunity.Opportunity_SF_Account_ID,
               Opportunity.Opportunity_Stage_Description,
               FrequencyStageDescription = COUNT(Opportunity.Opportunity_Stage_Description),
               MostFrequencedStageDescription = ROW_NUMBER() OVER (PARTITION BY Opportunity.Opportunity_SF_Account_ID
                                                                   ORDER BY COUNT(Opportunity.Opportunity_Stage_Description) DESC
                                                                  ),
               NumberOfStageDescription = ROW_NUMBER() OVER (PARTITION BY Opportunity.Opportunity_SF_Account_ID
                                                             ORDER BY COUNT(Opportunity.Opportunity_Stage_Description) ASC
                                                            )
        FROM #Opportunity AS Opportunity
        GROUP BY Opportunity.Opportunity_SF_Account_ID,
                 Opportunity.Opportunity_Stage_Description)
    SELECT Opportunity_SF_Account_ID = Opportunity_SF_Account_ID,
           Opportunity_Stage_Description = Opportunity_Stage_Description,
           NumberOfStageDescription = NumberOfStageDescription
    INTO #PrimaryStageDescription
    FROM FrequencyStageDescription
    WHERE MostFrequencedStageDescription = 1;

    --SELECT * FROM #PrimaryStageDescription

    /************************************************** Primary Type Description ***********************************************/
    DROP TABLE IF EXISTS #PrimaryTypeDescription;
    WITH FrequencyTypeDescription
    AS (SELECT Opportunity.Opportunity_SF_Account_ID,
               Opportunity.Opportunity_Type_Description,
               FrequencyTypeDescription = COUNT(Opportunity.Opportunity_Type_Description),
               MostFrequencedTypeDescription = ROW_NUMBER() OVER (PARTITION BY Opportunity.Opportunity_SF_Account_ID
                                                                  ORDER BY COUNT(Opportunity.Opportunity_Type_Description) DESC
                                                                 ),
               NumberOfTypeDescription = ROW_NUMBER() OVER (PARTITION BY Opportunity.Opportunity_SF_Account_ID
                                                            ORDER BY COUNT(Opportunity.Opportunity_Type_Description) ASC
                                                           )
        FROM #Opportunity AS Opportunity
        GROUP BY Opportunity.Opportunity_SF_Account_ID,
                 Opportunity.Opportunity_Type_Description)
    SELECT Opportunity_SF_Account_ID = Opportunity_SF_Account_ID,
           Opportunity_Type_Description = Opportunity_Type_Description,
           NumberOfTypeDescription = NumberOfTypeDescription
    INTO #PrimaryTypeDescription
    FROM FrequencyTypeDescription
    WHERE MostFrequencedTypeDescription = 1;

    --SELECT * FROM #PrimaryTypeDescription

    /************************************************** Primary Diligence Statuses ***********************************************/
    DROP TABLE IF EXISTS #PrimaryDiligenceStatuses;
    WITH FrequencyDiligenceStatuses
    AS (SELECT Opportunity.Opportunity_SF_Account_ID,
               Opportunity.Diligence_Statuses,
               FrequencyTypeDescription = COUNT(Opportunity.Diligence_Statuses),
               MostFrequencedDiligenceStatuses = ROW_NUMBER() OVER (PARTITION BY Opportunity.Opportunity_SF_Account_ID
                                                                    ORDER BY COUNT(Opportunity.Diligence_Statuses) DESC
                                                                   ),
               NumberOfDiligenceStatuses = ROW_NUMBER() OVER (PARTITION BY Opportunity.Opportunity_SF_Account_ID
                                                              ORDER BY COUNT(Opportunity.Diligence_Statuses) ASC
                                                             )
        FROM #Opportunity AS Opportunity
        GROUP BY Opportunity.Opportunity_SF_Account_ID,
                 Opportunity.Diligence_Statuses)
    SELECT Opportunity_SF_Account_ID = Opportunity_SF_Account_ID,
           Opportunity_Diligence_Statuses = Diligence_Statuses,
           NumberOfDiligenceStatuses = NumberOfDiligenceStatuses
    INTO #PrimaryDiligenceStatuses
    FROM FrequencyDiligenceStatuses
    WHERE MostFrequencedDiligenceStatuses = 1;

    --SELECT * FROM #PrimaryDiligenceStatuses

    /************************************************** Create Table to Merge it to the DataPanel***********************************************/

    DROP TABLE IF EXISTS #SalesForceData_Opportunity;

    SELECT Account_Holder_ID = syn_active_Dim_Involved_Parties.Involved_Party_ID,
           syn_active_Dim_Involved_Parties.Master_Account_Holder_ID,
           PrimaryStageDescription = PrimaryStageDescription.Opportunity_Stage_Description,
           PrimaryTypeDescription = PrimaryTypeDescription.Opportunity_Type_Description,
           PrimaryDiligenceStatuses = PrimaryDiligenceStatuses.Opportunity_Diligence_Statuses,
           OpportunityAggregated.*
    INTO #SalesForceData_Opportunity
    FROM #OpportunityAggregated AS OpportunityAggregated

        /*Involved Parties*/
        INNER JOIN
        (
            SELECT DISTINCT
                   Master_Account_Holder_ID,
                   Involved_Party_ID
            FROM DW_Main.dbo.syn_active_Dim_Involved_Parties

                /*Base View*/
                INNER JOIN #BaseView AS BaseView
                    ON DW_Main.dbo.syn_active_Dim_Involved_Parties.Involved_Party_ID = BaseView.Account_Holder_ID
            WHERE Involved_Party_Type = 1
        ) AS syn_active_Dim_Involved_Parties
            ON OpportunityAggregated.Opportunity_SF_Account_ID = syn_active_Dim_Involved_Parties.Master_Account_Holder_ID

        /*Primary Stage Description*/
        LEFT JOIN #PrimaryStageDescription AS PrimaryStageDescription
            ON OpportunityAggregated.Opportunity_SF_Account_ID = PrimaryStageDescription.Opportunity_SF_Account_ID

        /*Primary Type Description*/
        LEFT JOIN #PrimaryTypeDescription AS PrimaryTypeDescription
            ON OpportunityAggregated.Opportunity_SF_Account_ID = PrimaryTypeDescription.Opportunity_SF_Account_ID

        /*Primary Diligence Statuses*/
        LEFT JOIN #PrimaryDiligenceStatuses AS PrimaryDiligenceStatuses
            ON OpportunityAggregated.Opportunity_SF_Account_ID = PrimaryDiligenceStatuses.Opportunity_SF_Account_ID;



    --SELECT * FROM #SalesForceData_Opportunity
    /*************************************************- This Year LTV -****************************************************************/
    IF OBJECT_ID('tempdb..#ThisYearLTV') IS NOT NULL
        DROP TABLE #ThisYearLTV;

    SELECT Account_Holder_ID,
           ThisYearLTV = SUM(Revenue.RevenueAmountUSD)
    INTO #ThisYearLTV
    FROM #Revenue AS Revenue
    WHERE DATEDIFF(DAY, Revenue.TransactionDateTime, @CutPoint) > 0
          AND DATEDIFF(DAY, Revenue.TransactionDateTime, @CutPoint) <= 365
    GROUP BY Account_Holder_ID
    HAVING SUM(Revenue.RevenueAmountUSD) >= 0;

    /*************************************************-Y-****************************************************************/
    IF OBJECT_ID('tempdb..#Y') IS NOT NULL
        DROP TABLE #Y;

    SELECT Account_Holder_ID,
           LTV = SUM(Revenue.RevenueAmountUSD)
    INTO #Y
    FROM #Revenue AS Revenue
    WHERE DATEDIFF(DAY, @CutPoint, Revenue.TransactionDateTime) > 0
          AND DATEDIFF(DAY, @CutPoint, Revenue.TransactionDateTime) <= 365
    GROUP BY Account_Holder_ID
    HAVING SUM(Revenue.RevenueAmountUSD) >= 0;



    /**************************************************-Y Churn90Days-****************************************************************/
    /**************************************************Chrun 90***********************************************/
    IF OBJECT_ID('tempdb..#VolumeChurn') IS NOT NULL
        DROP TABLE #VolumeChurn;

    SELECT Account_Holder_ID = Volume.Payee_Involved_Party_ID,
           Transaction_Datetime = Volume.Transaction_Datetime,
           Volume_Amount_USD = Volume.Volume_Amount_USD
    INTO #VolumeChurn
    FROM [DW_Main].[dbo].syn_active_Fact_Volume AS Volume

        /*BaseView*/
        INNER JOIN #BaseView AS BaseView
            ON Volume.Payee_Involved_Party_ID = BaseView.Account_Holder_ID

        /*Transaction Status*/
        LEFT JOIN [DW_Main].[dbo].Ref_Transaction_Categories AS Ref_Transaction_Categories
            ON Volume.Transaction_Status = Ref_Transaction_Categories.StatusCode
    WHERE Volume.Volume_Amount_USD > 1
          AND Volume.Payee_Involved_Party_Type = 1
          AND Ref_Transaction_Categories.StatusCategoryCode = 2
          AND DATEDIFF(DAY, @CutPoint, Transaction_Datetime) >= 1
          AND DATEDIFF(DAY, @CutPoint, Transaction_Datetime) <= 3 * 30;


    IF OBJECT_ID('tempdb..#Y_Churn90Days') IS NOT NULL
        DROP TABLE #Y_Churn90Days;
    SELECT Account_Holder_ID,
           Churn90Days = CASE
                             WHEN SUM(Volume_Amount_USD) > 1 THEN
                                 0
                             ELSE
                                 1
                         END
    INTO #Y_Churn90Days
    FROM #VolumeChurn AS Volume
    WHERE DATEDIFF(DAY, @CutPoint, Transaction_Datetime) >= 1
          AND DATEDIFF(DAY, @CutPoint, Transaction_Datetime) <= 3 * 30
    GROUP BY Account_Holder_ID;

    /**************************************************** DataPanel ***************************************/
    IF OBJECT_ID('[Dev_Predict_DB].[LTV].[DataPanel_FTL_Prediction]') IS NOT NULL
        DROP TABLE [Dev_Predict_DB].[LTV].[DataPanel_FTL_Prediction];


    SELECT Account_Holder_ID = BaseView.Account_Holder_ID,
           Season = @Season,
           CutPoint = @CutPoint,

           /******************************* BaseView ******************************************************/
           Account_Holder_Language = BaseView.Account_Holder_Language,
           MasterAccount = BaseView.MasterAccount,
           Hours_From_RegComp_to_FTL = BaseView.Hours_From_RegComp_to_FTL,
           Hours_From_RegStarted_to_FTL = BaseView.Hours_From_RegStarted_to_FTL,
           Minutes_RegStarted_to_Complete = BaseView.Minutes_RegStarted_to_Complete,
           MainDomain = BaseView.MainDomain,
           Country_Name = BaseView.Country_Name,
           Nationality_Country = BaseView.Nationality_Country,
           City = BaseView.City,
           Age = BaseView.Age,
           RegistrationCompletionMonth = BaseView.RegistrationCompletionMonth,
           RegistrationCompletionWeekday = BaseView.RegistrationCompletionWeekday,
           FTL_Month = BaseView.FTL_Month,
           FTL_Weekday = BaseView.FTL_Weekday,
           Seniority = BaseView.Seniority,
           TimeBetweenRegistrationCompleteToCutPoint = BaseView.TimeBetweenRegistrationCompleteToCutPoint,
           UTM_Source = BaseView.UTM_Source,
           UTM_Medium = BaseView.UTM_Medium,
           UTM_Campaign = BaseView.UTM_Campaign,
           UTM = BaseView.UTM,
           First_TM = BaseView.First_TM,
           First_TM_Flow = BaseView.First_TM_Flow,
           Account_Holder_Reg_Program = BaseView.Account_Holder_Reg_Program,
           RegistrationVertical = BaseView.RegistrationVertical,
           IsCoreVertical = BaseView.IsCoreVertical,

           /**************************************** AH Segments and Tiers ****************************************/
           ValueSegment = ISNULL(AHSegment.ValueSegment, -1),
           Tier = ISNULL(AHSegment.Tier, -1),

           /********************************************** Logs ***************************************************/
           /* First Log */
           FirstBrowser = ISNULL(LogsTable.FirstBrowser, 'None'),
           FirstDevice = ISNULL(LogsTable.FirstDevice, 'None'),
           FirstPlatform = ISNULL(LogsTable.FirstPlatform, 'None'),
           /* Statistics Log */
           NumberOfEnteringToAccount = ISNULL(LogsTable.NumberOfEnteringToAccount, -1),
           LastLogAndFirstLogDifference = ISNULL(LogsTable.LastLogAndFirstLogDifference, -1),
           LastLogCutPointDifference = ISNULL(LogsTable.LastLogCutPointDifference, -1),
           /* Primary Browser */
           PrimaryBrowser = ISNULL(PrimaryBrowser.PrimaryBrowser, 'None'),
           NumberOfBrowsers = ISNULL(PrimaryBrowser.NumberOfBrowsers, -1),
           /* Primary Device */
           PrimaryDevice = ISNULL(PrimaryDevice.PrimaryDevice, 'None'),
           NumberOfDevices = ISNULL(PrimaryDevice.NumberOfDevices, -1),
           /* Primary Platform */
           PrimaryPlatform = ISNULL(PrimaryPlatform.PrimaryPlatform, 'None'),
           NumberOfPlatforms = ISNULL(PrimaryPlatform.NumberOfPlatforms, -1),

           /************************************************** EMail **********************************************/
           SumMailSent = ISNULL(EMailTable.SumMailSent, 0),
           SumMailOpened = ISNULL(EMailTable.SumMailOpened, 0),
           SDMailOpened = ISNULL(EMailTable.SDMailOpened, 0),
           SumMailClicked = ISNULL(EMailTable.SumMailClicked, 0),
           LastMailDateCutPointDifference = ISNULL(EMailTable.LastMailDateCutPointDifference, -1),

           /************************************************** Incidents ******************************************/
           /* Agregated Incidents */
           DaysFromLastIncident = ISNULL(AgregatedIncidents.DaysFromLastIncident, -1),
           NumberOfIncidents = ISNULL(AgregatedIncidents.NumberOfIncidents, 0),
           NumberOfDistinctInquiry = ISNULL(AgregatedIncidents.NumberOfDistinctInquiry, 0),
           NumberOfDistinctFirstSubjectDescription = ISNULL(
                                                               AgregatedIncidents.NumberOfDistinctFirstSubjectDescription,
                                                               0
                                                           ),
           NumberOfDistinctFirstSubSubjectDescription = ISNULL(
                                                                  AgregatedIncidents.NumberOfDistinctFirstSubSubjectDescription,
                                                                  0
                                                              ),
           NumberOfDistinctFirstDepartment = ISNULL(AgregatedIncidents.NumberOfDistinctFirstDepartment, 0),

           /* Primary Inquiry */
           PrimaryInquiry = ISNULL(PrimaryInquiry.PrimaryInquiry, 'None'),
           NumberOfMostFrequentInquiry = ISNULL(PrimaryInquiry.NumberOfMostFrequentInquiry, -1),

           /******************************** UnSubscribe Promotions and Newsletter and ect **********************/

           /************************************************ Volume *********************************************/
           /* Volume Agregate */
           HoursBetweenLastLoadToCutPoint = ISNULL(VolumeAgregate.HoursBetweenLastLoadToCutPoint, -1),
           HoursBetweenFirstLoadToCutPoint_InVolumeDays = ISNULL(
                                                                    VolumeAgregate.HoursBetweenFirstLoadToCutPoint_InVolumeDays,
                                                                    -1
                                                                ),
           SumVolume_InVolumeDays = ISNULL(VolumeAgregate.SumVolume_InVolumeDays, 0),
           SdVolume_InVolumeDays = ISNULL(VolumeAgregate.SdVolume_InVolumeDays, -1),
           GeometricMeanVolume_InVolumeDays = ISNULL(VolumeAgregate.GeometricMeanVolume_InVolumeDays, 0),
           CV_InVolumeDays = ISNULL(VolumeAgregate.CV_InVolumeDays, -1),
           SumOfCrossCountryVolume_InVolumeDays = ISNULL(VolumeAgregate.SumOfCrossCountryVolume_InVolumeDays, 0),
           MaxVolume_InVolumeDays = ISNULL(VolumeAgregate.MaxVolume_InVolumeDays, 0),
           NumberOfLoads_InVolumeDays = ISNULL(VolumeAgregate.NumberOfLoads_InVolumeDays, 0),
           DistinctMonthsActivity_InVolumeDays = ISNULL(VolumeAgregate.DistinctMonthsActivity_InVolumeDays, 0),
           BillingServices_InVolumeDays = ISNULL(VolumeAgregate.BillingServices_InVolumeDays, 0),
           MAP_InVolumeDays = ISNULL(VolumeAgregate.MAP_InVolumeDays, 0),
           MassPayout_InVolumeDays = ISNULL(VolumeAgregate.MassPayout_InVolumeDays, 0),
           PaymentService_InVolumeDays = ISNULL(VolumeAgregate.PaymentService_InVolumeDays, 0),
           Private_InVolumeDays = ISNULL(VolumeAgregate.Private_InVolumeDays, 0),
           Unknown_InVolumeDays = ISNULL(VolumeAgregate.Unknown_InVolumeDays, 0),
           SumVolumeMap_InVolumeDays = ISNULL(VolumeAgregate.SumVolumeMap_InVolumeDays, 0),
           NumberOfVolumeMap_InVolumeDays = ISNULL(VolumeAgregate.NumberOfVolumeMap_InVolumeDays, 0),
           SumVolumePaymentRequest_InVolumeDays = ISNULL(VolumeAgregate.SumVolumePaymentRequest_InVolumeDays, 0),
           NumberOfVolumePaymentRequest_InVolumeDays = ISNULL(
                                                                 VolumeAgregate.NumberOfVolumePaymentRequest_InVolumeDays,
                                                                 0
                                                             ),

           /* Primary Vertical */
           PrimaryVertical = ISNULL(PrimaryVertical.PrimaryVertical, 'Other'),
           NumberOfVerticals = ISNULL(PrimaryVertical.NumberOfVerticals, 0),

           /* Primary Volume Type */
           PrimaryVolumeType = ISNULL(PrimaryVolumeType.PrimaryVolumeType, 'Other'),
           PrimaryVolumeTypeCategory = ISNULL(PrimaryVolumeType.PrimaryVolumeTypeCategory, 'Other'),
           PrimaryVolumeTypeSubCategory = ISNULL(PrimaryVolumeType.PrimaryVolumeTypeSubCategory, 'Other'),

           /* Primary Loader */
           PrimaryLoader = ISNULL(PrimaryLoader.PrimaryLoader, 'Other'),
           NumberOfLoadersName = ISNULL(PrimaryLoader.NumberOfLoadersName, 0),

           /* Volume Aggregation From  FTL To Now */
           DaysBetweenFirstLoadToCutPoint_UntilNow = ISNULL(VolumeUntilNow.DaysBetweenFirstLoadToCutPoint_UntilNow, -1),
           SumVolume_UntilNow = ISNULL(VolumeUntilNow.SumVolume_UntilNow, 0),
           SdVolume_UntilNow = ISNULL(VolumeUntilNow.SdVolume_UntilNow, -1),
           GeometricMeanVolume_UntilNow = ISNULL(VolumeUntilNow.GeometricMeanVolume_UntilNow, 0),
           CV_UntilNow = ISNULL(VolumeUntilNow.CV_UntilNow, -1),
           MaxVolume_UntilNow = ISNULL(VolumeUntilNow.MaxVolume_UntilNow, 0),
           SumOfCrossCountryVolume_UntilNow = ISNULL(VolumeUntilNow.SumOfCrossCountryVolume_UntilNow, 0),
           NumberOfLoads_UntilNow = ISNULL(VolumeUntilNow.NumberOfLoads_UntilNow, 0),
           DistinctMonthsActivity_UntilNow = ISNULL(VolumeUntilNow.DistinctMonthsActivity_UntilNow, 0),
           SumVolumeMap_UntilNow = ISNULL(VolumeUntilNow.SumVolumeMap_UntilNow, 0),
           NumberOfVolumeMap_UntilNow = ISNULL(VolumeUntilNow.NumberOfVolumeMap_UntilNow, 0),
           SumVolumePaymentRequest_UntilNow = ISNULL(VolumeUntilNow.SumVolumePaymentRequest_UntilNow, 0),
           NumberOfVolumePaymentRequest_UntilNow = ISNULL(VolumeUntilNow.NumberOfVolumePaymentRequest_UntilNow, 0),

           /* First 24 Hour (Volume) */
           NumberOfLoadsInFirst24H = ISNULL(First24HVolume.NumberOfLoadsInFirst24H, 0),
           VolumeAmountInFirst24H = ISNULL(First24HVolume.VolumeAmountInFirst24H, 0),

           /* Volume by each Month */
           NumberOfLoads_Volume1 = ISNULL(Volume1.NumberOfLoads_Volume1, 0),
           SumVolume_Volume1 = ISNULL(Volume1.SumVolume_Volume1, 0),
           SumVolumeMAP_Volume1 = ISNULL(Volume1.SumVolumeMAP_Volume1, 0),
           NumberOfVolumeMAP_Volume1 = ISNULL(Volume1.NumberOfVolumeMAP_Volume1, 0),
           SdVolume_Volume1 = ISNULL(Volume1.SdVolume_Volume1, -1),
           MaxVolume_Volume1 = ISNULL(Volume1.MaxVolume_Volume1, 0),
           AmountOfPaymentRequest_Volume1 = ISNULL(Volume1.AmountOfPaymentRequest_Volume1, 0),
           GeometricMeanVolume_Volume1 = ISNULL(Volume1.GeometricMeanVolume_Volume1, 0),
           SumOfCrossCountryVolume_Volume1 = ISNULL(Volume1.SumOfCrossCountryVolume_Volume1, 0),
           NumberOfLoads_Volume2 = ISNULL(Volume2.NumberOfLoads_Volume2, 0),
           SumVolume_Volume2 = ISNULL(Volume2.SumVolume_Volume2, 0),
           SumVolumeMAP_Volume2 = ISNULL(Volume2.SumVolumeMAP_Volume2, 0),
           NumberOfVolumeMAP_Volume2 = ISNULL(Volume2.NumberOfVolumeMAP_Volume2, 0),
           SdVolume_Volume2 = ISNULL(Volume2.SdVolume_Volume2, -1),
           MaxVolume_Volume2 = ISNULL(Volume2.MaxVolume_Volume2, 0),
           AmountOfPaymentRequest_Volume2 = ISNULL(Volume2.AmountOfPaymentRequest_Volume2, 0),
           GeometricMeanVolume_Volume2 = ISNULL(Volume2.GeometricMeanVolume_Volume2, 0),
           SumOfCrossCountryVolume_Volume2 = ISNULL(Volume2.SumOfCrossCountryVolume_Volume2, 0),
           NumberOfLoads_Volume3 = ISNULL(Volume3.NumberOfLoads_Volume3, 0),
           SumVolume_Volume3 = ISNULL(Volume3.SumVolume_Volume3, 0),
           SumVolumeMAP_Volume3 = ISNULL(Volume3.SumVolumeMAP_Volume3, 0),
           NumberOfVolumeMAP_Volume3 = ISNULL(Volume3.NumberOfVolumeMAP_Volume3, 0),
           SdVolume_Volume3 = ISNULL(Volume3.SdVolume_Volume3, -1),
           MaxVolume_Volume3 = ISNULL(Volume3.MaxVolume_Volume3, 0),
           AmountOfPaymentRequest_Volume3 = ISNULL(Volume3.AmountOfPaymentRequest_Volume3, 0),
           GeometricMeanVolume_Volume3 = ISNULL(Volume3.GeometricMeanVolume_Volume3, 0),
           SumOfCrossCountryVolume_Volume3 = ISNULL(Volume3.SumOfCrossCountryVolume_Volume3, 0),
           SumVolume_Volume4 = ISNULL(Volume4.SumVolume_Volume4, 0),
           SumVolume_Volume5 = ISNULL(Volume5.SumVolume_Volume5, 0),
           SumVolume_Volume6 = ISNULL(Volume6.SumVolume_Volume6, 0),

           /**************************************************Internal Movements**********************************/
           /*Payer*/
           Payer_InternalMovementAmount = ISNULL(InternalMovements_Payer.Payer_InternalMovementAmount, 0),
           Payer_NumberOfInternalMovementAmount = ISNULL(
                                                            InternalMovements_Payer.Payer_NumberOfInternalMovementAmount,
                                                            0
                                                        ),
           /*Payee*/
           Payee_InternalMovementAmount = ISNULL(InternalMovements_Payee.Payee_InternalMovementAmount, 0),
           Payee_NumberOfInternalMovementAmount = ISNULL(
                                                            InternalMovements_Payee.Payee_NumberOfInternalMovementAmount,
                                                            0
                                                        ),

           /**************************************************Manage Currencies************************************/
           SumManageCurrencies = ISNULL(ManageCurrencies.SumManageCurrencies, 0),
           GeometricMeanManageCurrencies = ISNULL(ManageCurrencies.GeometricMeanManageCurrencies, 0),
           NumberOfManageCurrencies = ISNULL(ManageCurrencies.NumberOfManageCurrencies, 0),

           /****************************************Revenue**************************************************/
           /*Until Now*/
           DaysBetweenFirstRevenueToCutPoint = ISNULL(RevenueUntilNow.DaysBetweenFirstRevenueToCutPoint, -1),
           DaysFromLastRevenueToCutPoint = ISNULL(RevenueUntilNow.DaysFromLastRevenueToCutPoint, -1),
           NumberOfDistinctTransactionCode_UntilNow = ISNULL(
                                                                RevenueUntilNow.NumberOfDistinctTransactionCode_UntilNow,
                                                                0
                                                            ),
           NumberOfRevnueTransactions_UntilNow = ISNULL(RevenueUntilNow.NumberOfRevnueTransactions_UntilNow, 0),
           PercentOfFX_UntilNow = ISNULL(RevenueUntilNow.PercentOfFX_UntilNow, 0),
           NumberOfFX_UntilNow = ISNULL(RevenueUntilNow.NumberOfFX_UntilNow, 0),
           PercentOfCrossCountry_UntilNow = ISNULL(RevenueUntilNow.PercentOfCrossCountry_UntilNow, 0),
           NumberOfCrossCountry_UntilNow = ISNULL(RevenueUntilNow.NumberOfCrossCountry_UntilNow, 0),
           SumRevenue_UntilNow = ISNULL(RevenueUntilNow.SumRevenue_UntilNow, 0),
           GeometricMeanRevenue_UntilNow = ISNULL(RevenueUntilNow.GeometricMeanRevenue_UntilNow, 0),
           SdRevenue_UntilNow = ISNULL(RevenueUntilNow.SdRevenue_UntilNow, -1),
           MeanRevenue_UntilNow = ISNULL(RevenueUntilNow.MeanRevenue_UntilNow, 0),
           MaxRevenue_UntilNow = ISNULL(RevenueUntilNow.MaxRevenue_UntilNow, 0),
           SumRevenueWorkingCapital_UntilNow = ISNULL(RevenueUntilNow.SumRevenueWorkingCapital_UntilNow, 0),
           SumRevenueUsage_UntilNow = ISNULL(RevenueUntilNow.SumRevenueUsage_UntilNow, 0),
           SumRevenueOther_UntilNow = ISNULL(RevenueUntilNow.SumRevenueOther_UntilNow, 0),
           SumRevenueVolume_UntilNow = ISNULL(RevenueUntilNow.SumRevenueVolume_UntilNow, 0),
           SumRevenueInternalMovements_UntilNow = ISNULL(RevenueUntilNow.SumRevenueInternalMovements_UntilNow, 0),
           SumRevenueFX_UntilNow = ISNULL(RevenueUntilNow.SumRevenueFX_UntilNow, 0),
           SumRevenueCrossCountry_UntilNow = ISNULL(RevenueUntilNow.SumRevenueCrossCountry_UntilNow, 0),
           SumRevenueChargedOffline_UntilNow = ISNULL(RevenueUntilNow.SumRevenueChargedOffline_UntilNow, 0),
           SumRevenueBearingEntity_UntilNow = ISNULL(RevenueUntilNow.SumRevenueBearingEntity_UntilNow, 0),
           GeometricMeanRevenueWorkingCapital_UntilNow = ISNULL(
                                                                   RevenueUntilNow.GeometricMeanRevenueWorkingCapital_UntilNow,
                                                                   0
                                                               ),
           GeometricMeanRevenueUsage_UntilNow = ISNULL(RevenueUntilNow.GeometricMeanRevenueUsage_UntilNow, 0),
           GeometricMeanRevenueOther_UntilNow = ISNULL(RevenueUntilNow.GeometricMeanRevenueOther_UntilNow, 0),
           GeometricMeanRevenueVolume_UntilNow = ISNULL(RevenueUntilNow.GeometricMeanRevenueVolume_UntilNow, 0),
           GeometricMeanRevenueInternalMovements_UntilNow = ISNULL(
                                                                      RevenueUntilNow.GeometricMeanRevenueInternalMovements_UntilNow,
                                                                      0
                                                                  ),
           GeometricMeanFX_UntilNow = ISNULL(RevenueUntilNow.GeometricMeanFX_UntilNow, 0),
           GeometricMeanCrossCountry_UntilNow = ISNULL(RevenueUntilNow.GeometricMeanCrossCountry_UntilNow, 0),
           NumberOfBearingEntityID_UntilNow = ISNULL(RevenueUntilNow.NumberOfBearingEntityID_UntilNow, 0),
           NumberOfBearingCustomerCategoryID_UntilNow = ISNULL(
                                                                  RevenueUntilNow.NumberOfBearingCustomerCategoryID_UntilNow,
                                                                  0
                                                              ),
           /* Recently */
           NumberOfDistinctTransactionCode_Recently = ISNULL(
                                                                RevenueRecently.NumberOfDistinctTransactionCode_Recently,
                                                                0
                                                            ),
           NumberOfRevnueTransactions_Recently = ISNULL(RevenueRecently.NumberOfRevnueTransactions_Recently, 0),
           PercentOfFX_Recently = ISNULL(RevenueRecently.PercentOfFX_Recently, 0),
           NumberOfFX_Recently = ISNULL(RevenueRecently.NumberOfFX_Recently, 0),
           PercentOfCrossCountry_Recently = ISNULL(RevenueRecently.PercentOfCrossCountry_Recently, 0),
           NumberOfCrossCountry_Recently = ISNULL(RevenueRecently.NumberOfCrossCountry_Recently, 0),
           SumRevenue_Recently = ISNULL(RevenueRecently.SumRevenue_Recently, 0),
           GeometricMeanRevenue_Recently = ISNULL(RevenueRecently.GeometricMeanRevenue_Recently, 0),
           SdRevenue_Recently = ISNULL(RevenueRecently.SdRevenue_Recently, -1),
           MeanRevenue_Recently = ISNULL(RevenueRecently.MeanRevenue_Recently, 0),
           MaxRevenue_Recently = ISNULL(RevenueRecently.MaxRevenue_Recently, 0),
           SumRevenueWorkingCapital_Recently = ISNULL(RevenueRecently.SumRevenueWorkingCapital_Recently, 0),
           SumRevenueUsage_Recently = ISNULL(RevenueRecently.SumRevenueUsage_Recently, 0),
           SumRevenueOther_Recently = ISNULL(RevenueRecently.SumRevenueOther_Recently, 0),
           SumRevenueVolume_Recently = ISNULL(RevenueRecently.SumRevenueVolume_Recently, 0),
           SumRevenueInternalMovements_Recently = ISNULL(RevenueRecently.SumRevenueInternalMovements_Recently, 0),
           SumRevenueFX_Recently = ISNULL(RevenueRecently.SumRevenueFX_Recently, 0),
           SumRevenueCrossCountry_Recently = ISNULL(RevenueRecently.SumRevenueCrossCountry_Recently, 0),
           SumRevenueChargedOffline_Recently = ISNULL(RevenueRecently.SumRevenueChargedOffline_Recently, 0),
           SumRevenueBearingEntity_Recently = ISNULL(RevenueRecently.SumRevenueBearingEntity_Recently, 0),
           GeometricMeanRevenueWorkingCapital_Recently = ISNULL(
                                                                   RevenueRecently.GeometricMeanRevenueWorkingCapital_Recently,
                                                                   0
                                                               ),
           GeometricMeanRevenueUsage_Recently = ISNULL(RevenueRecently.GeometricMeanRevenueUsage_Recently, 0),
           GeometricMeanRevenueOther_Recently = ISNULL(RevenueRecently.GeometricMeanRevenueOther_Recently, 0),
           GeometricMeanRevenueVolume_Recently = ISNULL(RevenueRecently.GeometricMeanRevenueVolume_Recently, 0),
           GeometricMeanRevenueInternalMovements_Recently = ISNULL(
                                                                      RevenueRecently.GeometricMeanRevenueInternalMovements_Recently,
                                                                      0
                                                                  ),
           GeometricMeanFX_Recently = ISNULL(RevenueRecently.GeometricMeanFX_Recently, 0),
           GeometricMeanCrossCountry_Recently = ISNULL(RevenueRecently.GeometricMeanCrossCountry_Recently, 0),
           NumberOfBearingEntityID_Recently = ISNULL(RevenueRecently.NumberOfBearingEntityID_Recently, 0),
           NumberOfBearingCustomerCategoryID_Recently = ISNULL(
                                                                  RevenueRecently.NumberOfBearingCustomerCategoryID_Recently,
                                                                  0
                                                              ),

           /* Primary Revenue */
           PrimaryRevenueLoaderName = ISNULL(PrimaryRevenueLoaderName.PrimaryRevenueLoaderName, 'None or Other'),
           PrimaryRevenueVerticalName = ISNULL(PrimaryRevenueLoaderName.PrimaryRevenueVerticalName, 'None or Other'),
           NumberOfRevenueLoaderName = ISNULL(PrimaryRevenueLoaderName.NumberOfRevenueLoaderName, 0),
           PrimaryRevenueItem = ISNULL(PrimaryRevenueItem.PrimaryRevenueItem, 'None or Other'),
           NumberOfRevenueItems = ISNULL(PrimaryRevenueItem.NumberOfRevenueItems, 0),
           PrimaryRevenueType = ISNULL(PrimaryRevenueType.PrimaryRevenueType, 'None or Other'),
           NumberOfRevenueTypes = ISNULL(PrimaryRevenueType.NumberOfRevenueTypes, 0),
           PrimaryRevenureSubCategory = ISNULL(PrimaryRevenueSubCategory.PrimaryRevenureSubCategory, 'None or Other'),
           NumberOfRevenueSubCategories = ISNULL(PrimaryRevenueSubCategory.NumberOfRevenueSubCategories, 0),
           PrimaryRevenueCategory = ISNULL(PrimaryRevenueCategory.PrimaryRevenueCategory, 'None or Other'),
           NumberOfRevenueCategories = ISNULL(PrimaryRevenueCategory.NumberOfRevenueCategories, 0),
           PrimaryRevenueActivity = ISNULL(PrimaryRevenueActivity.PrimaryRevenueActivity, 'None or Other'),
           NumberOfRevenueActivities = ISNULL(PrimaryRevenueActivity.NumberOfRevenueActivities, 0),

           /**************************************** Usage **************************************************/
           /* Usage Until now*/
           SumUsage_UntilTotal = ISNULL(Usage_UntilNow.SumUsage_UntilTotal, 0),
           NumberOfUsage_UntilTotal = ISNULL(Usage_UntilNow.NumberOfUsage_UntilTotal, 0),
           LastLoadLastUsageDiff = ISNULL(Usage_UntilNow.LastLoadLastUsageDiff, -1),
           IsLastLoadAfterLastUsage = ISNULL(Usage_UntilNow.IsLastLoadAfterLastUsage, -1),
           DaysBetweenFirstUsageToCutPoint = ISNULL(Usage_UntilNow.DaysBetweenFirstUsageToCutPoint, -1),
           DaysFromLastUsageToCutPoint = ISNULL(Usage_UntilNow.DaysFromLastUsageToCutPoint, -1),
           /* Usage Several Months */
           SumIsFX = ISNULL(UsageSeveralMonths.SumIsFX, 0),
           SumIsCrossCountry = ISNULL(UsageSeveralMonths.SumIsCrossCountry, 0),
           SumIsAutoWithdrawal = ISNULL(UsageSeveralMonths.SumIsAutoWithdrawal, 0),
           NumberOfMerchentCategory = ISNULL(UsageSeveralMonths.NumberOfMerchentCategory, 0),
           NumberOfDistinctUsageEndCountry = ISNULL(UsageSeveralMonths.NumberOfDistinctUsageEndCountry, 0),
           NumberOfDistinctExternalBankName = ISNULL(UsageSeveralMonths.NumberOfDistinctExternalBankName, 0),
           DiversityOfMerchentCategory = ISNULL(UsageSeveralMonths.DiversityOfMerchentCategory, -1),
           NumberOfOfCurrencies = ISNULL(UsageSeveralMonths.NumberOfOfCurrencies, 0),
           SumCNP = ISNULL(UsageSeveralMonths.SumCNP, 0),
           NumberOfUsage = ISNULL(UsageSeveralMonths.NumberOfUsage, 0),
           SumUsage = ISNULL(UsageSeveralMonths.SumUsage, 0),
           GeometricMeanUsage = ISNULL(UsageSeveralMonths.GeometricMeanUsage, 0),
           MaxUsage = ISNULL(UsageSeveralMonths.MaxUsage, 0),
           SdUsage = ISNULL(UsageSeveralMonths.SdUsage, -1),
           MeanUsage = ISNULL(UsageSeveralMonths.MeanUsage, 0),
           SumUsage_FX = ISNULL(UsageSeveralMonths.SumUsage_FX, 0),
           SumUsage_NotInUSD = ISNULL(UsageSeveralMonths.SumUsage_NotInUSD, 0),
           SumUsage_CNP = ISNULL(UsageSeveralMonths.SumUsage_CNP, 0),
           SumUsage_CrossCountry = ISNULL(UsageSeveralMonths.SumUsage_CrossCountry, 0),
           SumUsage_MAP = ISNULL(UsageSeveralMonths.SumUsage_MAP, 0),
           SumUsage_ATM_POS = ISNULL(UsageSeveralMonths.SumUsage_ATM_POS, 0),
           SumUsage_Withdrawal = ISNULL(UsageSeveralMonths.SumUsage_Withdrawal, 0),
           MeanUsage_MAP = ISNULL(UsageSeveralMonths.MeanUsage_MAP, 0),
           MaxUsage_MAP = ISNULL(UsageSeveralMonths.MaxUsage_MAP, 0),
           MeanUsage_ATM_POS = ISNULL(UsageSeveralMonths.MeanUsage_ATM_POS, 0),
           MaxUsage_ATM_POS = ISNULL(UsageSeveralMonths.MaxUsage_ATM_POS, 0),
           MeanUsage_Withdrawal = ISNULL(UsageSeveralMonths.MeanUsage_Withdrawal, 0),
           MaxUsage_Withdrawal = ISNULL(UsageSeveralMonths.MaxUsage_Withdrawal, 0),
           /* Primary Usage */
           PrimaryUsageType = ISNULL(PrimaryUsageType.PrimaryUsageType, 'None'),
           NumberOfUsageType = ISNULL(PrimaryUsageType.NumberOfUsageType, 0),
           PrimaryUsageTypeSubCategory = ISNULL(PrimaryUsageTypeSubCategory.PrimaryUsageTypeSubCategory, 'None'),
           NumberOfUsageTypeSubCategory = ISNULL(PrimaryUsageTypeSubCategory.NumberOfUsageTypeSubCategory, 0),
           PrimaryUsageTypeCategory = ISNULL(PrimaryUsageTypeCategory.PrimaryUsageTypeCategory, 'None'),
           NumberOfUsageTypeCategory = ISNULL(PrimaryUsageTypeCategory.NumberOfUsageTypeCategory, 0),
           PrimaryUsageCurrency = ISNULL(PrimaryUsageCurrency.PrimaryUsageCurrency, 'None'),
           PrimaryMerchent = ISNULL(PrimaryMerchent.PrimaryMerchent, 'None'),

           /**************************************** Stores ****************************************************/
           NumberOfStores = ISNULL(Stores.NumberOfStores, 0),
           NumberOfStoresRecently = ISNULL(Stores.NumberOfStoresRecently, 0),
           LastStoreCreationToCutPointDaysDifference = ISNULL(Stores.FirstStoreCreationToCutPointDaysDifference, -1),
           FirstStoreCreationToCutPointDaysDifference = ISNULL(Stores.FirstStoreCreationToCutPointDaysDifference, -1),

           /**************************************** Opportunities ****************************************************/
           PrimaryOpportunitiesStageDescription = ISNULL(SalesForceData_Opportunity.PrimaryStageDescription, 'None'),
           PrimaryOpportunitiesTypeDescription = ISNULL(SalesForceData_Opportunity.PrimaryTypeDescription, 'None'),
           PrimaryOpportunitiesDiligenceStatuses = ISNULL(SalesForceData_Opportunity.PrimaryDiligenceStatuses, 'None'),
           OpportunityProbability = ISNULL(SalesForceData_Opportunity.Opportunity_Probability, -1),
           OpportunityMonthlyVolumeCommitmentAmountUSD = ISNULL(
                                                                   SalesForceData_Opportunity.Opportunity_Monthly_Volume_Commitment_Amount_USD,
                                                                   -1
                                                               ),
           OpportunityExpectedAnnualVolumeAmountUSD = ISNULL(
                                                                SalesForceData_Opportunity.Opportunity_Expected_Annual_Volume_Amount_USD,
                                                                -1
                                                            ),
           OpportunityExpectedAnnualRevenueAmountUSD = ISNULL(
                                                                 SalesForceData_Opportunity.Opportunity_Expected_Annual_Revenue_Amount_USD,
                                                                 -1
                                                             ),
           OpportunityForecastAnnualRevenueAmountUSD = ISNULL(
                                                                 SalesForceData_Opportunity.Opportunity_Forecast_Annual_Revenue_Amount_USD,
                                                                 -1
                                                             ),

           /**************************************** Predictions **************************************************/
           /*LTV*/
           LTV = ISNULL(LTV.LTV, -1),
           ChurnFromLTVModel = ISNULL(LTV.ChurnFromLTVModel, -1),
           /*Activity Segmentation*/
           ActivitySegment = ISNULL(ActivitySegmentation.ActivitySegment, -1),
           FLC = ISNULL(ActivitySegmentation.FLC, -1),
           FLW = ISNULL(ActivitySegmentation.FLW, -1),
           RFProbsPredict_30 = ISNULL(Churn30.RFProbsPredict_30, -1),
           RFClassPredict_30 = ISNULL(Churn30.RFClassPredict_30, -1),
           RFProbsPredict_90 = ISNULL(Churn90.RFProbsPredict_90, -1),
           RFClassPredict_90 = ISNULL(Churn90.RFClassPredict_90, -1),
           RFProbsPredict_180 = ISNULL(Churn180.RFProbsPredict_180, -1),
           RFClassPredict_180 = ISNULL(Churn180.RFClassPredict_180, -1),

           /********************************************** Y ******************************************************/
           --TimeFromLastLoadToNow = Y.TimeFromLastLoadToNow,
           --TimeFromNowToVolumeLoad = Y.TimeFromNowToVolumeLoad,
           --CensoredOrNot = Y.CensoredOrNot,
           Churn90Days = ISNULL(Y_Churn90Days.Churn90Days, 1),
           ThisYearLTV = ISNULL(ThisYearLTV, 0),
           Y = ISNULL(Y.LTV, 0)
    INTO [Dev_Predict_DB].[LTV].[DataPanel_FTL_Prediction]
    FROM #BaseView AS BaseView
        /************************************************* Value Segment ***************************************/
        LEFT JOIN #AHSegment AS AHSegment
            ON BaseView.Account_Holder_ID = AHSegment.Account_Holder_ID
        /************************************************* PrimaryVertical *************************************/
        LEFT JOIN #PrimaryVertical AS PrimaryVertical
            ON BaseView.Account_Holder_ID = PrimaryVertical.Account_Holder_ID
        /************************************************* PrimaryVolumeType ***********************************/
        LEFT JOIN #PrimaryVolumeType AS PrimaryVolumeType
            ON BaseView.Account_Holder_ID = PrimaryVolumeType.Account_Holder_ID
        /************************************************* Primary Loader Name (Company Name) ******************/
        LEFT JOIN #PrimaryLoader AS PrimaryLoader
            ON BaseView.Account_Holder_ID = PrimaryLoader.Account_Holder_ID
        /************************************************** Volume Agregate (Several Months Aggregated) ********/
        LEFT JOIN #VolumeAgregate AS VolumeAgregate
            ON BaseView.Account_Holder_ID = VolumeAgregate.Account_Holder_ID
        /*************************************************** Volume From FTL To Now ****************************/
        LEFT JOIN #VolumeUntilNow AS VolumeUntilNow
            ON BaseView.Account_Holder_ID = VolumeUntilNow.Account_Holder_ID
        /************************************************* First 24 Hours (Volume) *****************************/
        LEFT JOIN #First24HVolume AS First24HVolume
            ON BaseView.Account_Holder_ID = First24HVolume.Account_Holder_ID
        /************************************************** Volume by each month *******************************/
        LEFT JOIN #Volume1 AS Volume1
            ON BaseView.Account_Holder_ID = Volume1.Account_Holder_ID
        LEFT JOIN #Volume2 AS Volume2
            ON BaseView.Account_Holder_ID = Volume2.Account_Holder_ID
        LEFT JOIN #Volume3 AS Volume3
            ON BaseView.Account_Holder_ID = Volume3.Account_Holder_ID
        LEFT JOIN #Volume4 AS Volume4
            ON BaseView.Account_Holder_ID = Volume4.Account_Holder_ID
        LEFT JOIN #Volume5 AS Volume5
            ON BaseView.Account_Holder_ID = Volume5.Account_Holder_ID
        LEFT JOIN #Volume6 AS Volume6
            ON BaseView.Account_Holder_ID = Volume6.Account_Holder_ID
        /************************************************* Internal Movements **********************************/
        LEFT JOIN #InternalMovements_Payer AS InternalMovements_Payer
            ON BaseView.Account_Holder_ID = InternalMovements_Payer.Account_Holder_ID
        LEFT JOIN #InternalMovements_Payee AS InternalMovements_Payee
            ON BaseView.Account_Holder_ID = InternalMovements_Payee.Account_Holder_ID
        /************************************************* Manage Currencies ***********************************/
        LEFT JOIN #ManageCurrencies AS ManageCurrencies
            ON BaseView.Account_Holder_ID = ManageCurrencies.Account_Holder_ID
        /**************************************** Logs *********************************************************/
        LEFT JOIN #LogsTable AS LogsTable
            ON BaseView.Account_Holder_ID = LogsTable.Account_Holder_ID
        LEFT JOIN #PrimaryBrowser AS PrimaryBrowser
            ON BaseView.Account_Holder_ID = PrimaryBrowser.Account_Holder_ID
        LEFT JOIN #PrimaryDevice AS PrimaryDevice
            ON BaseView.Account_Holder_ID = PrimaryDevice.Account_Holder_ID
        LEFT JOIN #PrimaryPlatform AS PrimaryPlatform
            ON BaseView.Account_Holder_ID = PrimaryPlatform.Account_Holder_ID
        /************************************************** Incidents *******************************************/
        LEFT JOIN #AgregatedIncidents AS AgregatedIncidents
            ON BaseView.Account_Holder_ID = AgregatedIncidents.Account_Holder_ID
        LEFT JOIN #PrimaryInquiry AS PrimaryInquiry
            ON BaseView.Account_Holder_ID = PrimaryInquiry.Account_Holder_ID


        /******************************UnSubscribe Promotions and Newsletter and ect****************************/

        /**************************************** EMail **********************************************************/
        LEFT JOIN #EMailTable AS EMailTable
            ON BaseView.Account_Holder_ID = EMailTable.Account_Holder_ID
        /**************************************** Revenue ********************************************************/
        LEFT JOIN #RevenueUntilNow AS RevenueUntilNow
            ON BaseView.Account_Holder_ID = RevenueUntilNow.Account_Holder_ID
        LEFT JOIN #RevenueRecently AS RevenueRecently
            ON BaseView.Account_Holder_ID = RevenueRecently.Account_Holder_ID
        /*************************************** Primary Revenue *************************************************/
        LEFT JOIN #PrimaryRevenueLoaderName AS PrimaryRevenueLoaderName
            ON BaseView.Account_Holder_ID = PrimaryRevenueLoaderName.Account_Holder_ID
        LEFT JOIN #PrimaryRevenueItem AS PrimaryRevenueItem
            ON BaseView.Account_Holder_ID = PrimaryRevenueItem.Account_Holder_ID
        LEFT JOIN #PrimaryRevenueType AS PrimaryRevenueType
            ON BaseView.Account_Holder_ID = PrimaryRevenueType.Account_Holder_ID
        LEFT JOIN #PrimaryRevenueSubCategory AS PrimaryRevenueSubCategory
            ON BaseView.Account_Holder_ID = PrimaryRevenueSubCategory.Account_Holder_ID
        LEFT JOIN #PrimaryRevenueCategory AS PrimaryRevenueCategory
            ON BaseView.Account_Holder_ID = PrimaryRevenueCategory.Account_Holder_ID
        LEFT JOIN #PrimaryRevenueActivity AS PrimaryRevenueActivity
            ON BaseView.Account_Holder_ID = PrimaryRevenueActivity.Account_Holder_ID
        /**************************************** Usage **********************************************************/
        LEFT JOIN #Usage_UntilNow AS Usage_UntilNow
            ON BaseView.Account_Holder_ID = Usage_UntilNow.Account_Holder_ID
        LEFT JOIN #UsageSeveralMonths AS UsageSeveralMonths
            ON BaseView.Account_Holder_ID = UsageSeveralMonths.Account_Holder_ID
        /**************************************** Primary Usage **************************************************/
        LEFT JOIN #PrimaryUsageType AS PrimaryUsageType
            ON BaseView.Account_Holder_ID = PrimaryUsageType.Account_Holder_ID
        LEFT JOIN #PrimaryUsageTypeCategory AS PrimaryUsageTypeCategory
            ON BaseView.Account_Holder_ID = PrimaryUsageTypeCategory.Account_Holder_ID
        LEFT JOIN #PrimaryUsageTypeSubCategory AS PrimaryUsageTypeSubCategory
            ON BaseView.Account_Holder_ID = PrimaryUsageTypeSubCategory.Account_Holder_ID
        LEFT JOIN #PrimaryUsageCurrency AS PrimaryUsageCurrency
            ON BaseView.Account_Holder_ID = PrimaryUsageCurrency.Account_Holder_ID
        LEFT JOIN #PrimaryMerchent AS PrimaryMerchent
            ON BaseView.Account_Holder_ID = PrimaryMerchent.Account_Holder_ID
        /**************************************** Predictions ****************************************************/
        LEFT JOIN #LTV AS LTV
            ON BaseView.Account_Holder_ID = LTV.Account_Holder_ID
        LEFT JOIN #ActivitySegmentation AS ActivitySegmentation
            ON BaseView.Account_Holder_ID = ActivitySegmentation.Account_Holder_ID
        LEFT JOIN #Churn30 AS Churn30
            ON BaseView.Account_Holder_ID = Churn30.Account_Holder_ID
        LEFT JOIN #Churn90 AS Churn90
            ON BaseView.Account_Holder_ID = Churn90.Account_Holder_ID
        LEFT JOIN #Churn180 AS Churn180
            ON BaseView.Account_Holder_ID = Churn180.Account_Holder_ID
        /**************************************** Stores *********************************************************/
        LEFT JOIN #Stores AS Stores
            ON BaseView.Account_Holder_ID = Stores.Account_Holder_ID
        /**************************************** Stores *********************************************************/
        LEFT JOIN #SalesForceData_Opportunity AS SalesForceData_Opportunity
            ON BaseView.Account_Holder_ID = SalesForceData_Opportunity.Account_Holder_ID
        /**************************************** Churn90Days *****************************************************/
        LEFT JOIN #Y_Churn90Days AS Y_Churn90Days
            ON BaseView.Account_Holder_ID = Y_Churn90Days.Account_Holder_ID
        /**************************************** ThisYearLTV *****************************************************/
        LEFT JOIN #ThisYearLTV AS ThisYearLTV
            ON BaseView.Account_Holder_ID = ThisYearLTV.Account_Holder_ID
        /**************************************** Y **************************************************************/
        LEFT JOIN #Y AS Y
            ON BaseView.Account_Holder_ID = Y.Account_Holder_ID;




END;

