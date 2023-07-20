from snowflake.snowpark.session import Session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *
from snowflake.snowpark.functions import col
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import from_unixtime, date_format, unix_timestamp, to_timestamp
from pathlib import Path
import configparser
import os
import time

def init_config():
    config = configparser.ConfigParser()

    parentFolderPath = Path(__file__).parent.parent.parent
    config_file_path = os.path.join(parentFolderPath, 'common/utils', 'config.ini')

    config.read(config_file_path)

    return config

timeThreshold = 3 * 60
creditThreshold = 50000

def fetchRawDf(session, customerName, batchId):
    rawDf = session.table('raw.fm_raw').filter((F.col('customer') == customerName) & (F.col('batch_Id') == batchId))

    rawDf = rawDf.withColumn('sortpriority', 
                F.when((rawDf.rectype == 't'), 1)
                .when((rawDf.rectype == 'game_changed'), 2)
                .when((rawDf.rectype == 'n'), 3)
                .when((rawDf.rectype == 'c'), 4)
                .when((rawDf.rectype == 'pss'), 5)
                .when((rawDf.rectype == 'w'), 6)
                .when((rawDf.rectype == 'x'), 7)    
                .when((rawDf.rectype == 'y'), 8)
                .when((rawDf.rectype == 'o'), 9)
                .when((rawDf.rectype == 'pse'), 10)
                .when((rawDf.rectype == 'v'),11).otherwise(100)
            )

    #region  Uncomment for unit testing or troubleshooting

    # rawDf.select(['egmId', 'rectype', 'sortpriority', 'trantimestamp', 'gameplaylogsequence', 'creditmeter', 'tranamt', 'payout', 'themeid', 'deviceid', 'denomid', 'customer']) \
    #     .orderBy(rawDf.egmId, rawDf.trantimestamp, rawDf.gameplaylogsequence, rawDf.sortpriority) \
    #     .show(500)

    #endregion  Uncomment for unit testing or troubleshooting

    return rawDf

def calcTimeAndCreditThresholds(rawDf):
    # Add next and previous timestamps as individual columns
    lagDf = rawDf.withColumn('nextTimestamp', F.lead(rawDf.trantimestamp).over(Window.partitionBy(rawDf.egmId)
                                                                                .orderBy(rawDf.trantimestamp, rawDf.gameplaylogsequence, rawDf.sortpriority))) \
            .withColumn('prevTimestamp', F.lag(rawDf.trantimestamp).over(Window.partitionBy(rawDf.egmId)
                                                                                .orderBy(rawDf.trantimestamp, rawDf.gameplaylogsequence, rawDf.sortpriority)))

    # Add the time differences between current timestamp and next and previous timestamps in seconds
    timeDeltaDf = lagDf.withColumn('nextTimestampDelta', F.datediff('second', lagDf.trantimestamp, lagDf.nexttimestamp)) \
                .withColumn('prevTimestampDelta', F.datediff('second', lagDf.prevtimestamp, lagDf.trantimestamp))

    # Check if the time differences exceed the defined time threshold gap and add the result as one more column
    timeThresholdsDf = timeDeltaDf.withColumn('isNextTimeThresholdMet', F.when((timeDeltaDf.nextTimestampDelta > timeThreshold), True).otherwise(False)) \
                .withColumn('isPrevTimeThresholdMet', F.when((timeDeltaDf.prevTimestampDelta > timeThreshold), True).otherwise(False))
    
    # Check the credit thresholds and add the result as 2 more columns so that 
    # it is in the required format to determine session start and ends
    timeAndCreditStatusDf = timeThresholdsDf.withColumn('isCashOut', F.when(((timeThresholdsDf.rectype == 'v') | (timeThresholdsDf.creditmeter < creditThreshold)), True).otherwise(False)) \
            .withColumn('isCashIn', F.when((timeThresholdsDf.rectype == 't'), True).otherwise(False))
    
    #region  Uncomment for unit testing or troubleshooting

    # timeAndCreditStatusDf.select(['egmId', 'rectype', 'sortpriority', 'gameplaylogsequence', 'trantimestamp', 
    #             'sessionstate', 'sessionstart', 'nexttimestamp','prevTimestamp', 'nextTimestampDelta', 'prevTimestampDelta', 
    #             'isNextTimeThresholdMet','isPrevTimeThresholdMet', 'isCashout','isCashIn', 
    #         'creditmeter', 'tranamt', 'payout', 'themeid', 'deviceid', 'denomid', 'customer']) \
    # .orderBy(timeAndCreditStatusDf.egmId, timeAndCreditStatusDf.trantimestamp, timeAndCreditStatusDf.gameplaylogsequence, timeAndCreditStatusDf.sortpriority) \
    # .show(2000)

    #endregion  Uncomment for unit testing or troubleshooting
    
    return timeAndCreditStatusDf

def demarcateSessionStartAndEnds(timeAndCreditStatusDf):
    sessionStatesDf = timeAndCreditStatusDf.withColumn('sessionstate', 
                                        F.when((timeAndCreditStatusDf.isCashOut & timeAndCreditStatusDf.isNextTimeThresholdMet), 'session end')
                                        .when((timeAndCreditStatusDf.isCashIn & timeAndCreditStatusDf.isPrevTimeThresholdMet), 'potential session start')
                                        .otherwise('game continued')) 
    
    sessionStatesDf = sessionStatesDf.withColumn('prevSessionState', F.lag(sessionStatesDf.sessionstate).over(Window.partitionBy(sessionStatesDf.egmId)
                                                                                                              .orderBy(
                                                                                                                        sessionStatesDf.trantimestamp, 
                                                                                                                        sessionStatesDf.gameplaylogsequence, 
                                                                                                                        sessionStatesDf.sortpriority
                                                                                                              )))
    sessionStatesDf=sessionStatesDf.withColumn('sessionstart', F.when((sessionStatesDf.prevsessionstate=='session end'),'session start').otherwise(None))

    #region  Uncomment for unit testing or troubleshooting

    # sessionStatesDf.select(['egmId', 'rectype', 'sortpriority', 'gameplaylogsequence', 'trantimestamp', 
    #              'sessionstart', 'sessionstate', 'prevsessionstate', 'nexttimestamp','prevTimestamp', 'nextTimestampDelta', 'prevTimestampDelta', 
    #             'isNextTimeThresholdMet','isPrevTimeThresholdMet', 'isCashout','isCashIn', 
    #         'creditmeter', 'tranamt', 'payout', 'themeid', 'deviceid', 'denomid', 'customer']) \
    # .orderBy(sessionStatesDf.egmId, sessionStatesDf.trantimestamp, sessionStatesDf.gameplaylogsequence, sessionStatesDf.sortpriority) \
    # .show(2000)

    #endregion  Uncomment for unit testing or troubleshooting

    return sessionStatesDf

def generateSessionNumbers(sessionStatesDf):
    sessNumDf = sessionStatesDf.withColumn('sessionNumber', 
                                                F.when((sessionStatesDf.sessionstart.isNotNull()), 
                                                            F.row_number().over(Window.partitionBy(sessionStatesDf.egmId, sessionStatesDf.sessionstart)
                                                                                    .orderBy(
                                                                                                sessionStatesDf.trantimestamp, 
                                                                                                sessionStatesDf.gameplaylogsequence, 
                                                                                                sessionStatesDf.sortpriority
                                                                                            )
                                                                                )
                                                        )
                                            )

    #region  Uncomment for unit testing or troubleshooting

    # sessNumDf.select(['egmId', 'rectype', 'sortpriority', 'gameplaylogsequence', 'trantimestamp', 
    #                 'sessionNumber','sessionstart', 'sessionstate', 
    #             'creditmeter', 'tranamt', 'payout', 'themeid', 'deviceid', 'denomid', 'customer']) \
    #     .orderBy(sessNumDf.egmId, sessNumDf.trantimestamp, sessNumDf.gameplaylogsequence, sessNumDf.sortpriority).show(500)

    #endregion  Uncomment for unit testing or troubleshooting
    
    return sessNumDf

def generateUniqueSessionIds(sessNumDf):
    segDf = sessNumDf.withColumn('timestampstr', F.when(sessNumDf.sessionNumber.isNotNull(), 
                                                        F.to_char(sessNumDf.trantimestamp, 'yyyyMMddHHmmss')))
    
    uniqueSessionIdDf = segDf.withColumn('uniqueSessionId', F.concat_ws(F.lit('_'), segDf.egmId, segDf.timestampstr, segDf.sessionNumber))

    #region  Uncomment for unit testing or troubleshooting

    # sessionIdDf.select(['egmId', 'rectype', 'sortpriority', 'gameplaylogsequence', 'trantimestamp', 
    #                 'uniqueSessionId', 'timestampstr', 'sessionNumber','sessionstart', 'sessionstate', 
    #             'creditmeter', 'tranamt', 'payout', 'themeid', 'deviceid', 'denomid', 'customer']) \
    #     .orderBy(sessionIdDf.egmId, sessionIdDf.trantimestamp, sessionIdDf.gameplaylogsequence, sessionIdDf.sortpriority).show(500)

    #endregion  Uncomment for unit testing or troubleshooting

    return uniqueSessionIdDf

def forwardFillSessionIds(uniqueSessionIdDf):
    # Unlike pandas, forward fill method is not available OOTB in snowpark
    lagsDf = uniqueSessionIdDf.withColumn('prevSessionId', F.lag(uniqueSessionIdDf.uniqueSessionId, ignore_nulls=True)
                                                        .over(Window.partitionBy(uniqueSessionIdDf.egmId)
                                                                    .orderBy(uniqueSessionIdDf.trantimestamp, 
                                                                             uniqueSessionIdDf.gameplaylogsequence, 
                                                                             uniqueSessionIdDf.sortpriority)))

    filledDf = lagsDf.withColumn('filledSessionId', F.coalesce(lagsDf.uniqueSessionId, lagsDf.prevSessionId))

    #region  Uncomment for unit testing or troubleshooting

    # filledDf.select(['egmId', 'rectype', 'sortpriority', 'gameplaylogsequence', 'trantimestamp', 
    #                 'sessionstate','sessionstart', 'filledSessionId','sessionId', 'prevSessionId', 'sessionNumber',
    #             'creditmeter', 'tranamt', 'payout', 'themeid', 'deviceid', 'denomid', 'customer']) \
    #     .orderBy(filledDf.egmId, filledDf.trantimestamp, filledDf.gameplaylogsequence, filledDf.sortpriority).show(500)

    #endregion  Uncomment for unit testing or troubleshooting
    
    sessionizedDf = filledDf

    return sessionizedDf

def saveSessionizedData(sessionizedDf, customerName, batchId):
    print(f"Initiated: Save of sessions belonging to customer {customerName} and batchId {batchId} into FM_Sessionized table")

    # sessionizedDf.show(500)

    renamedDf = sessionizedDf.withColumnRenamed('filledSessionId', 'sessionId')

    sessionsDf = renamedDf.select([
                                    'egmId', 'rectype', 'sessionId', 'trantimestamp', 'gameplaylogsequence', 
                                    'creditmeter', 'tranamt', 'payout', 'themeid', 'themeName', 'deviceid',
                                    'denomid', 'paytable', 'playerId', 'areaid', 'location','currencyid', 'customer', 'batch_id',
                                    'sortpriority', 'iscashout', 'iscashin', 'sessionstart', 'sessionstate',
                                    'sessionNumber', 'nexttimestampdelta', 'prevtimestampdelta', 'isnexttimethresholdmet', 
                                    'isprevtimethresholdmet', 'uniqueSessionId'
                                ])

    # sessionsDf.show(500)

    sessionsDf.write.mode('append').saveAsTable('RAW.FM_Sessionized')

    print(f"Completed: Save of sessions belonging to customer {customerName} and batchId {batchId} into FM_Sessionized table")

def execute(customerName, batchId):
    if((customerName == None) | (customerName == '') | (batchId == None) | (batchId == '')):
        print('Invalid customer name or batchId')
        return
    
    exec_start_time = time.time()

    config = init_config()

    #region initialize session

    CONNECTION_PARAMETERS = {
        'account': config.get('snowflake_connection', 'account'),
        'user': config.get('snowflake_connection', 'user'),
        'password': config.get('snowflake_connection', 'password'),
        'schema': config.get('snowflake_connection', 'schema'),
        'database': config.get('snowflake_connection', 'database'),
        'warehouse': config.get('snowflake_connection', 'warehouse'),
        'role': config.get('snowflake_connection', 'role'),
    }

    session = Session.builder.configs(CONNECTION_PARAMETERS).create()

    #endregion initialize session

    rawDf = fetchRawDf(session, customerName, batchId)

    timeAndCreditStatusDf = calcTimeAndCreditThresholds(rawDf)

    sessionStatesDf = demarcateSessionStartAndEnds(timeAndCreditStatusDf)

    sessNumDf = generateSessionNumbers(sessionStatesDf)

    sessionIdDf = generateUniqueSessionIds(sessNumDf)

    sessionizedDf = forwardFillSessionIds(sessionIdDf)

    saveSessionizedData(sessionizedDf, customerName, batchId)

    session.close()

    exec_end_time = time.time()

    exec_total_time = exec_end_time - exec_start_time
    
    print("Total time taken: ", exec_total_time)

if __name__ == "__main__":
    # Unit test
    testCustomerName = 'uil'
    testBatchId = 'f72fbf50-996b-45a4-b029-a477bf824c04'
    execute(testCustomerName, testBatchId)