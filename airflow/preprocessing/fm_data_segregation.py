from snowflake.snowpark.session import Session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *
from snowflake.snowpark.functions import col
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window
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

def fetchCuratedSessionsByBatchId(session, batchId,CustomerName):
    curatedDf=session.table("RAW.FM_Sessionized").filter((F.col('customer') == CustomerName) & (F.col('batch_Id') == batchId))
    print(f"Initiated: Marking payout negative records to 0 & Count step 4: { curatedDf.count() }")
    # curatedDf.show(1)
    # colmns=["EGMID","EVENTDATETIME","WAGER","TIME_DELTA","NEW_SESSIONS","GAMEPLAYLOGSEQUENCE","PAYOUT","SESSION_ID","UNIX_TIMESTAMP_UTC","BONUS_FLAG","SOFTWARE_ID"]
    df_new=curatedDf.select(when(col("payout")<0, lit(0)).otherwise(col("payout")).alias("payout_new"),'*').drop("payout").withColumnRenamed("payout_new","payout")
    df_new.createOrReplaceTempView('new_vw')
    df_final=session.sql("select RECTYPE, EGMID, SESSIONID, THEMEID,'' THEMENAME, DEVICEID, DENOMID, TRANTIMESTAMP, GAMEPLAYLOGSEQUENCE, CREDITMETER, TRANAMT, PAYOUT, case when THEMEID is null then lag(THEMEID) ignore nulls over (partition by SESSIONID order by TRANTIMESTAMP,GAMEPLAYLOGSEQUENCE) else THEMEID end as THEMEID_NEW, PLAYERID, Customer, BATCH_ID from new_vw order by EgmId,SESSIONID,TRANTIMESTAMP").drop("THEMEID").withColumnRenamed("THEMEID_NEW","THEMEID")
    df_final=df_final.filter((df_final.RECTYPE).isin ('c','t','n','w','o','x','y','v','pss','pse'))
    
    # df_final.show(1)
    # df_final_wagernull=df_final.filter(col("TRANAMT").isNull() | col("TRANAMT")<0).distinct()
    df_final_wagernull_1=df_final.filter(col("TRANAMT") <0 )
    print(df_final_wagernull_1.count())
    # df_final_wagernull_2=df_final.filter(col("TRANAMT").isNull())
    # print(df_final_wagernull_2.count())
    # df_final_wagernull = df_final_wagernull_1.unionAll(df_final_wagernull_2).distinct()
    df_final_wagernull=df_final_wagernull_1
    print(df_final_wagernull.count())
    invalid_sessions_tranamt=df_final_wagernull_1.select(col("SESSIONID")) \
                        .distinct().withColumn("error_type", lit("SessionLength")) \
                        .withColumn("err_rec_load_ts", current_timestamp() ) \
                        .withColumn("CustomerName", lit(CustomerName)  ) \
                        .withColumn("BATCHID", lit(batchId) ) 

    invalid_sessions_tranamt.write.mode("append").save_as_table("RAW.invalid_sessions_fm")

    df_final_wagernull_f=df_final_wagernull.select("RECTYPE", "EGMID", "SESSIONID", "THEMEID", "THEMENAME", "DEVICEID", "DENOMID", "TRANTIMESTAMP", "GAMEPLAYLOGSEQUENCE", "CREDITMETER", "TRANAMT", "PAYOUT", "PLAYERID", "Customer", "BATCH_ID")
    # df_final_wagernull_f.show(1)
    df_final_wagernull_f.write.mode('append').saveAsTable('RAW.FM_RAW_DUP')
    print("Completed: Marking payout negative records to 0")
    print("Initiated: Removing negative TRANAMT")
    print(df_final.count())
    # curatedDf=df_final.filter(col("TRANAMT").isNotNull() ).filter(col("TRANAMT")>=0)
    curatedDf=df_final
    print(curatedDf.count())
    print("Completed: Removing negative TRANAMT")
    # print("Initiated: Save of curated data")
    # # curatedDf.show(1)
    # curatedDf.write.mode('append').saveAsTable('RAW.FM_CURATED')
    # print("Completed: Save of curated data")
    return curatedDf

def prioritized_sort_fm(curatedDf):
    # print("Initiated: Preprocessing - sorting raw data by session, record types")
    # add filter for batchid = {batch_id} & pass as parameter to the function in above line
    print(f"Info: No. of records in fm RAW table : { curatedDf.count() }")

    # Sorting the fm RAW Table
    
    curatedDf = curatedDf.withColumn('sortpriority', 
                F.when((col("rectype") == 't'), 1)
                .when((col("rectype") == 'game_changed'), 2)
                .when((col("rectype") == 'n'), 3)
                .when((col("rectype") == 'c'), 4)
                .when((col("rectype") == 'w'), 5)
                .when((col("rectype") == 'x'), 6)    
                .when((col("rectype") == 'y'), 7)
                .when((col("rectype") == 'o'), 8)
                .when((col("rectype") == 'v'), 9).otherwise(10)
            )
    # curatedDf.show(1)
    fm_sorted = curatedDf.sort("SESSIONID","sortpriority",).drop("sortpriority")

    print("Completed: Preprocessing - sorting raw data by session, record types")

    return fm_sorted

def fetch_invalid_sessions(fm_sorted, batchId, CustomerName):
    print("Initiated: Preprocessing - filtering invalid sessions")

    #1. check for minimum session length ==> "SessionLength"
    invalidSessdf = fm_sorted \
        .filter((col('RECTYPE') == 'w') & (col('TRANAMT') >= '0')) \
        .groupBy('SESSIONID') \
        .agg(count('SESSIONID').alias('no_of_primary_wager_commands')) \
        .filter(col('NO_OF_PRIMARY_WAGER_COMMANDS') < 1) \
        .select('SESSIONID')

    invalidSessList = invalidSessdf.collect()
    print("printing invalidlist")
    print(invalidSessList)
    if invalidSessList==[]:
        invalidSessList=[None]
    print(fm_sorted.filter(col('SESSIONID').isin(invalidSessList)).select(fm_sorted.sessionid).distinct().count())
    final_errors = fm_sorted.filter(col('SESSIONID').isin(invalidSessList)) \
        .select("SESSIONID").distinct().withColumn("error_type", lit("SessionLength")) \
        .withColumn("err_rec_load_ts", current_timestamp()) \
        .withColumn("CustomerName", lit(CustomerName) ) \
        .withColumn("BATCHID", lit(batchId) ) 

    # final_errors.show()
    
    final_errors.write.mode("append").save_as_table("RAW.invalid_sessions_fm")
    return fm_sorted
    # def cashout_invalid_sessions(fm_sorted, batchId, CustomerName):
    #     # 5. check if residual credits after cashout ==> "ResidualCredits"
    #     # fm_sorted.show(1)
    #     ResidualCredits = fm_sorted \
    #         .filter((col('RECTYPE').isin ('v')) & (col('CREDITMETER') != 0)) \
    #                 .select("SESSIONID").distinct().withColumn("error_type", lit("ResidualCredits")) \
    #                 .withColumn("err_rec_load_ts", current_timestamp()) \
    #                             .withColumn("CustomerName", lit(CustomerName) ) \
    #                             .withColumn("BATCHID", lit(batchId)  )
    #     ResidualCredits.write.mode("append").save_as_table("RAW.invalid_sessions_fm")

    #     # 6.a check if only one cashOut and if this is the last command
    #     #
    #     one_cashout = fm_sorted.filter(col('RECTYPE') == 'v').groupBy("SESSIONID", "RECTYPE").count()
    #     one_cashout = one_cashout.filter(col('COUNT') > 1 ).drop("COUNT","RECTYPE") \
    #                             .distinct() \
    #                             .withColumn("error_type", lit("more_cashouts")) \
    #                             .withColumn("err_rec_load_ts", current_timestamp() ) \
    #                             .withColumn("CustomerName", lit(CustomerName) ) \
    #                             .withColumn("BATCHID", lit(batchId)  )
    #     one_cashout.write.mode("append").save_as_table("RAW.invalid_sessions_fm")
    #     return fm_sorted

def cashin_position_invalid_sessions(fm_sorted, batchId, CustomerName):

    windowSpec = Window.partitionBy("SESSIONID").orderBy("EGMID","SESSIONID","TRANTIMESTAMP","GAMEPLAYLOGSEQUENCE")
    cashin_pos=fm_sorted.withColumn('row_number_col', F.row_number().over(windowSpec)) \
                        .select(col("RECTYPE"),col("EGMID"),col("SESSIONID"),col("TRANTIMESTAMP"),col("GAMEPLAYLOGSEQUENCE"),col("ROW_NUMBER_COL")) \
                        .filter(col('row_number_col')=='1')
    
    # print(cashin_pos.select(fm_sorted.sessionid).distinct().count())
    # cashin_pos.show(1)
    cashin_pos=cashin_pos.filter((col("rectype") != 't' ) & (col("rectype")  != 'n') & (col("rectype")  != 'c'))
    print(cashin_pos.select(fm_sorted.sessionid).distinct().count())
    cashin_pos = cashin_pos.select("SESSIONID") \
                            .distinct() \
                            .withColumn("error_type", lit("cashin_position")) \
                            .withColumn("err_rec_load_ts", current_timestamp() ) \
                            .withColumn("CustomerName", lit(CustomerName) ) \
                            .withColumn("BATCHID", lit(batchId)  )
    # cashin_pos.show(1)
    # print(cashin_pos.count())

    cashin_pos.write.mode("append").save_as_table("RAW.invalid_sessions_fm")
    return fm_sorted

def stm_w_check(fm_sorted, batchId, CustomerName):
    # fm_sorted.show(1)
    sessionsDf_new=fm_sorted.filter(F.col('rectype').isin ('w','x','pse','pss'))
    sessionsDf_new_1=sessionsDf_new.withColumn('rectype_nextrec', F.lead(sessionsDf_new.rectype).over(Window.partitionBy(sessionsDf_new.egmId).orderBy(sessionsDf_new.trantimestamp, sessionsDf_new.gameplaylogsequence)) )
    sessionsDf_new_2=sessionsDf_new_1.withColumn('invalid_sess_check', \
                                F.when(((sessionsDf_new_1.rectype == 'pss') & ((sessionsDf_new_1.rectype_nextrec == 'w') | (sessionsDf_new_1.rectype_nextrec == 'x') ) ), lit('valid_sess')) \
                                .when(((sessionsDf_new_1.rectype == 'pss') & (sessionsDf_new_1.rectype_nextrec == 'pse') ), lit('invalid_sess')).otherwise(None))
    sessionsDf_new_3=sessionsDf_new_2.filter(F.col('invalid_sess_check').isin ('invalid_sess'))

    pss_sess_df=fm_sorted.filter((col("rectype") == 'pss' )).select(fm_sorted.sessionid).distinct()
    print(pss_sess_df.count())

    pse_sess_df=fm_sorted.filter((col("rectype") == 'pse' )).select(fm_sorted.sessionid).distinct()
    print(pse_sess_df.count())

    pss_pse_sess_df=pss_sess_df.join(pse_sess_df,pss_sess_df.sessionid==pse_sess_df.sessionid).select(pss_sess_df.sessionid.alias("pss_sessionid"), pse_sess_df.sessionid.alias("pse_sessionid"))
    print(pss_pse_sess_df.count())

    invalid_sessions_df =fm_sorted.select('sessionid').distinct().join(pss_pse_sess_df, pss_pse_sess_df.pss_sessionid == fm_sorted.SESSIONID, 'leftanti')
    print(invalid_sessions_df.count())

    union_df=invalid_sessions_df.select("SESSIONID").union(sessionsDf_new_3.select("SESSIONID"))
    
    print(union_df.select(fm_sorted.sessionid).distinct().count())
    # cashin_pos.show(1)
    cashin_pos = union_df.select("SESSIONID") \
                            .distinct() \
                            .withColumn("error_type", lit("stm_pss_pse_no_w_records")) \
                            .withColumn("err_rec_load_ts", current_timestamp() ) \
                            .withColumn("CustomerName", lit(CustomerName) ) \
                            .withColumn("BATCHID", lit(batchId)  )
    # cashin_pos.show(1)
    # print(cashin_pos.count())

    cashin_pos.write.mode("append").save_as_table("RAW.invalid_sessions_fm")
    return fm_sorted

def check_valid_wager_pair_for_intensity(fm_sorted, batchId, CustomerName):
    wagerDf = fm_sorted.filter(F.col('rectype').isin('w'))

    wagerTimesDf = wagerDf.withColumn('prev_wager_amt', 
                        F.lag(wagerDf.tranamt).over(Window.partitionBy(wagerDf.sessionid).orderBy(wagerDf.trantimestamp)))

    wagerTimesDf = wagerTimesDf.withColumn('prev_wager_timestamp', 
                        F.lag(wagerDf.trantimestamp).over(Window.partitionBy(wagerDf.sessionid).orderBy(wagerDf.trantimestamp))) \
        
    filteredDf = wagerTimesDf.filter(wagerTimesDf.prev_wager_timestamp.isNotNull())

    timeDiffDf = filteredDf.withColumn('seconds_between_next_wager', F.when(((wagerTimesDf.tranamt > 0) & (wagerTimesDf.prev_wager_amt > 0)), 
                                            F.datediff('second', filteredDf.prev_wager_timestamp, filteredDf.trantimestamp)).otherwise(F.lit(0)))

    timeDiffDf = timeDiffDf.filter((timeDiffDf.tranamt > 0) & (timeDiffDf.prev_wager_amt > 0))

    wagerPairStatusDf = timeDiffDf.groupBy(timeDiffDf.sessionid) \
                            .agg(
                                ((F.sum(timeDiffDf.seconds_between_next_wager)) > 0)
                                .alias('has_valid_wager_pair')
                                )
    
    # wagerPairStatusDf.show()

    invalidSessionsDf = wagerPairStatusDf.filter(wagerPairStatusDf.has_valid_wager_pair == False)
  
    print(f'Invalid wager pair sessions count: {invalidSessionsDf.select(invalidSessionsDf.sessionid).distinct().count()}')

    wagerPairErrosDf = invalidSessionsDf.select("SESSIONID") \
                            .distinct() \
                            .withColumn("error_type", lit("no_valid_wager_pairs_found_in_session_to_calculate_intensity")) \
                            .withColumn("err_rec_load_ts", current_timestamp()) \
                            .withColumn("CustomerName", lit(CustomerName)) \
                            .withColumn("BATCHID", lit(batchId))

    wagerPairErrosDf.write.mode("append").save_as_table("RAW.invalid_sessions_fm")

#region Future error handling cases
# def creditmeter_invalid_sessions(fm_sorted, batchId, CustomerName):
#     # 6.c --credit meter
#     windowSpec = Window.partitionBy("SESSIONID").orderBy("EGMID","SESSIONID","TRANTIMESTAMP","GAMEPLAYLOGSEQUENCE")
#     cashin_pos=fm_sorted.withColumn('row_number_col', F.count(col("RECTYPE")).over(windowSpec)) \
#                         .select(col("RECTYPE"),col("EGMID"),col("SESSIONID"),col("TRANTIMESTAMP"),col("GAMEPLAYLOGSEQUENCE"),col("ROW_NUMBER_COL"))
    
    


#     creditmeter = fm_sorted.withColumn('row_number', F.row_number().over(windowSpec)) \
#     .withColumn('last', F.max('row_number').over(window2)) \
#     .filter('row_number = last') \
#     .select("SESSIONID","CREDITMETER","row_number","last")

#     creditmeter_df = creditmeter.filter(col("CREDITMETER") != 0).select("SESSIONID") \
#                                 .distinct() \
#                                 .withColumn("error_type", lit("creditmeter")) \
#                                 .withColumn("err_rec_load_ts", current_timestamp() ) \
#                                 .withColumn("CustomerName", lit(CustomerName) ) \
#                                 .withColumn("BATCHID", lit(batchId)  )
#     creditmeter_df.write.mode("append").save_as_table("RAW.invalid_sessions_fm")
#     # print("Initiated: Save of curated data")
#     # # curatedDf.show(1)
#     # creditmeter_df.write.mode('append').saveAsTable('RAW.FM_CURATED')
#     # print("Completed: Save of curated data")

#     print("Completed: Preprocessing - filtering invalid sessions")
#     return fm_sorted

#endregion Keeping for future error handling cases

def curated_data_load(session,batchId,CustomerName):
    print("Initiated: Save of curated data")
    # curatedDf.show(1)
    curatedDf=session.table("RAW.FM_Sessionized").filter(col("batch_id") == batchId).filter(col("Customer") == CustomerName)
    invalid_sess=session.table("RAW.INVALID_SESSIONS_FM").filter(col("batchid") == batchId).filter(col("Customer") == CustomerName)
    valid_sess_df = curatedDf.join(invalid_sess, curatedDf.SESSIONID == invalid_sess.SESSIONID, 'leftanti')
    print(f"Info: No. of InValid Sessions : { invalid_sess.count()}")
    print(f"Info: No. of Valid Sessions : { curatedDf.count()}")
    valid_sess_df=valid_sess_df.select(when(col("payout")<0, lit(0)).otherwise(col("payout")).alias("payout_new"),'*').drop("payout").withColumnRenamed("payout_new","payout")
    #******
    valid_sess_df = valid_sess_df.withColumn('THEMEID_NEW', F.lag(valid_sess_df.THEMEID, ignore_nulls=True).over(Window.partitionBy(valid_sess_df.sessionId).orderBy(valid_sess_df.TRANTIMESTAMP))) \
                .withColumn('THEMENAME_NEW', F.lag(valid_sess_df.THEMENAME, ignore_nulls=True).over(Window.partitionBy(valid_sess_df.sessionId).orderBy(valid_sess_df.TRANTIMESTAMP))) \
                .drop("THEMEID").withColumnRenamed("THEMEID_NEW","THEMEID") \
                .drop("THEMENAME").withColumnRenamed("THEMENAME_NEW","THEMENAME")
    #******
    valid_sess_df=valid_sess_df.drop("creditmeter")
    valid_sess_df_1=valid_sess_df.withColumn('intermim_creditmeter', F.when((valid_sess_df.rectype == 't'), valid_sess_df.tranamt) \
                              .when((valid_sess_df.rectype == 'w'), valid_sess_df.payout-valid_sess_df.tranamt) \
                              .when((valid_sess_df.rectype == 'x'), valid_sess_df.payout-valid_sess_df.tranamt) \
                              .when((valid_sess_df.rectype == 'v'), -valid_sess_df.tranamt) \
                              .otherwise(F.lit(None))) 

    valid_sess_df_2=valid_sess_df_1.withColumn('CREDITMETER', F.sum(valid_sess_df_1.intermim_creditmeter).over(Window.partitionBy(valid_sess_df_1.SESSIONID).orderBy(valid_sess_df_1.trantimestamp, valid_sess_df_1.GAMEPLAYLOGSEQUENCE))) 
    areaidFill = valid_sess_df_2.withColumn('AREAID',when(valid_sess_df_2.areaid.isNull(),substring(valid_sess_df_2.LOCATION,1,4))
                                         .otherwise(valid_sess_df_2.AREAID))
    validFinalDf=areaidFill.select("EGMID","RECTYPE","SESSIONID","TRANTIMESTAMP","GAMEPLAYLOGSEQUENCE","CREDITMETER","TRANAMT","PAYOUT","THEMEID","THEMENAME","DEVICEID","DENOMID","PAYTABLE","PLAYERID","AREAID","LOCATION","CURRENCYID","CUSTOMER","BATCH_ID","SORTPRIORITY","ISCASHOUT","ISCASHIN","SESSIONSTART","SESSIONSTATE","SESSIONNUMBER","NEXTTIMESTAMPDELTA","PREVTIMESTAMPDELTA","ISNEXTTIMETHRESHOLDMET","ISPREVTIMETHRESHOLDMET","UNIQUESESSIONID")
    print("Initiated: writing data into fm_curated table")
    print(validFinalDf.select(validFinalDf.sessionid).distinct().count())
    validFinalDf.write.mode('append').saveAsTable('RAW.FM_CURATED')
    print("Completed: Save of curated data")
    
def execute(CustomerName,batchId):
    if((CustomerName == None) | (CustomerName == '') | (batchId == None) | (batchId == '')):
        print('Invalid CustomerName name or batchId')
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
    
    # check if records already exist in PD_CURATED table before processing
    curatedDf = fetchCuratedSessionsByBatchId(session, batchId, CustomerName)
    print(" fetchCuratedSessionsByBatchId function completed")
    
    fm_sorted=prioritized_sort_fm(curatedDf)
    print(" prioritized_sort_fm function completed")

    fm_sorted=fetch_invalid_sessions(fm_sorted, batchId, CustomerName)
    print(" fetch_invalid_sessions function completed")

    # fm_sorted=cashout_invalid_sessions(fm_sorted, batchId, CustomerName)
    # print(" cashout_invalid_sessions function completed")

    fm_sorted=cashin_position_invalid_sessions(fm_sorted, batchId, CustomerName)
    print(" cashin_position_invalid_sessions function completed")

    stm_w_check(fm_sorted, batchId, CustomerName)
    print(" stm_w_check function completed")

    check_valid_wager_pair_for_intensity(fm_sorted, batchId, CustomerName)
    print('Completed invalidating sessions with less than 1 valid pairs of wager in a session')

    # fm_sorted=creditmeter_invalid_sessions(fm_sorted, batchId, CustomerName)
    # print(" creditmeter_invalid_sessions function completed")

    curated_data_load(session,batchId,CustomerName)
    print(" curated_data_load function completed")

    session.close()
    exec_end_time = time.time()
    exec_total_time = exec_end_time - exec_start_time
    print("Total time taken: ", exec_total_time)

if __name__ == "__main__":
    # Unit test batch ids
    testBatchId = 'f72fbf50-996b-45a4-b029-a477bf824c04'
    # testBatchId = None
    # testBatchId = ''
    CustomerName = 'uil'
    execute(CustomerName,testBatchId)
