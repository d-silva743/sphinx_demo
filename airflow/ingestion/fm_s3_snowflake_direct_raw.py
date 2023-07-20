import configparser
from snowflake.snowpark import functions 
from snowflake.snowpark import Window
from snowflake.snowpark.functions import col, lead
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import *
from pathlib import Path
import os
import uuid
from ast import literal_eval
from datetime import datetime
from airflow import *
from airflow.exceptions import AirflowFailException
from itertools import *
import sys
root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, root_dir.__str__())

# Internal imports
# import common.utils.definitions as defi


def init_config():
    config = configparser.ConfigParser()
    parentFolderPath = Path(__file__).parent.parent.parent
    config_file_path = os.path.join(parentFolderPath, 'common/utils', 'config.ini')
    config.read(config_file_path)
    return config

def list_query(session,table_name):
    try:
        db_res=session.sql("select CURRENT_DATABASE(),CURRENT_SCHEMA()  ")
        print(db_res)
        db_res.show()
        list_query=f"list @FM_S3_STAGE/{table_name}"
        vlt_files=session.sql(list_query)
        vlt_files.show()
        #finding the list of files available in S3 Location
        lst=[]
        s3_location=''
        vlt_files_list=vlt_files.collect()
        for i in vlt_files_list:
            flie_name=i.name
            print(flie_name)
            position_1=[i for i, n in enumerate(flie_name) if n == '/'][2]
            s3_location=flie_name[:position_1]+'/'
            new_flie_name=flie_name[position_1+1:]
            print(new_flie_name)
            if new_flie_name not in lst:
                lst.append(new_flie_name)
        print("printing lst")
        print(lst)
        print("printing s3_location")
        print(s3_location)
    except Exception as error:
        # session.close()
        print("An expection Occured while doing the fetching the file list details 2")
        raise error
        
    return lst,s3_location

# def admin_log_check(session,path_lst,s3_location):
#     # STEP 1 if there are files in-progress, Code exists right here .....
#     try:
#         inprogress_files_details= session.table("raw.admin_log")
#         inprogress_files_details=inprogress_files_details.filter((col("SOURCE_NAME")=='FM') & (col("LOAD_STATUS")=='Started')) \
#                                                     .select(col("file_name")) \
#                                                     .distinct()
#         inprogress_files=inprogress_files_details.collect()
#         dag_status='proceed'
#         # if len(inprogress_files)!=0:
#         #     print("process will be skipped as there are few files in-progress")
#         #     session.close()
#         #     dag_status='skip'
#         #     print(dag_status)
#         #     # raise AirflowFailException('Bad run: Please wait till the previous runs succeeds or close the previous run')
#         # else:
#         #     dag_status='proceed'
#     except Exception as error:
#         session.close()
#         print("An expection Occured while doing the fetching the file list processed details")
#         raise error    
#     return s3_location,dag_status,path_lst

def processed_files(session,path_lst,s3_location):

    try:
        #  STEP 2 finding the list files that are already proceed and exluding them from current files to be processed list
        executed_files_details= session.table("raw.admin_log")
        executed_files_details=executed_files_details.filter((col("SOURCE_NAME")=='FM') & (col("LOAD_STATUS")=='Completed')) \
                                                    .select(col("file_name")) \
                                                    .distinct()
        executed_files_col_list=executed_files_details.collect()
        executed_files_list=[]

        for abc in executed_files_col_list:
            defg=abc.FILE_NAME
            executed_files_list.append(defg)
            print(defg)
            if defg in path_lst:
                path_lst.remove(defg)
        dag_status='proceed'

        # STEP 3  if no files left to be proessed, Process will exit right here
        if len(path_lst)==0:
            print("exiting as there are no pending latest file to be processed")
            dag_status='skip'
            print(dag_status)
            # raise AirflowFailException('Bad run: No files available to ingest')

    except Exception as error:
        session.close()
        print("An expection Occured while doing the fetching the file list processed details")
        raise error

    return s3_location,dag_status,path_lst

def fetchBatchIdsToPreprocess(session,path_lst,s3_location):
    print("printing fetchBatchIdsToPreprocess function started")
    print(path_lst)
    for fl in path_lst:
        try:
            print("loading into admin log for the below file: ")
            print(fl)
            # s3_location_new = [name.replace("'",'') for name in s3_location if(fl in name)]
            # s3_location_new=['-'.join(s3_location_new)]
            # s3_location_val=s3_location_new[0]
            # print(s3_location_val)
            admin_table_df = session.create_dataframe(['FM']).to_df("SOURCE_NAME")
            admin_table_df=admin_table_df.withColumn('FILE_NAME',lit(fl)) \
                                        .withColumn('S3_Location',lit(s3_location)) \
                                        .withColumn('LOAD_START_TIME',current_timestamp()) \
                                        .withColumn('LOAD_STATUS',lit('Started')) \
                                        .withColumn('LOAD_END_TIME',lit(None)) \
                                        .withColumn('EXECUTION_DURATION',lit(None)) \
                                        .withColumn('FILE_FMT_ERROR_CNT',lit(None)) \
                                        .withColumn('BATCHID',lit(None)) \
                                        .withColumn('DATA_MIN_TIMESTAMP',lit(None)) \
                                        .withColumn('DATA_MAX_TIMESTAMP',lit(None)) 
            # admin_table_df.show()
            admin_table_df.write.mode('append').saveAsTable("raw.ADMIN_LOG")
        except Exception as error:
            session.close()
            print("An expection Occured while doing the ADMIN_LOG entry")
            raise error
    return s3_location,path_lst  
   

def timestamp(path_lst,session,batchId):
    print(path_lst)
    for fl in path_lst:
        try:
            print(fl)
            # unique_key=uuid.uuid4()
            unique_key=batchId
            print(unique_key)
            tble_name=fl.split("/",1)[0]
            raw_table=f"fm_landing.{tble_name}_raw"
            interim_table=f"fm_landing.{tble_name}_interim"
            # truncate_query=f"truncate table {interim_table}"
            print(tble_name)
            column_nm=f"defi.def_{tble_name}_col"
            colmns_name=eval(column_nm)
            columns = ','.join(colmns_name)
            print(columns)
            # truncate=session.sql(truncate_query).show()
            read_query=f"COPY INTO {raw_table} FROM (select {columns}, '{unique_key}'  FROM @FM_S3_STAGE/{fl}) FILE_FORMAT=(FORMAT_NAME=PARQUET_FILE_FMT) ON_ERROR='CONTINUE' "
            # print(read_query)
            read_df=session.sql(read_query).show()

            print("Details for records with format errors entered into table FILE_FMT_ERR")
            # insert_query_df = session.table(interim_table)
            # insert_query_df=insert_query_df.withColumn('BATCH_ID',lit(str(unique_key)))
            # insert_query_df.write.mode('append').saveAsTable(raw_table)
            update_batchid_q=f'''UPDATE RAW.ADMIN_LOG set batchid = '{unique_key}', LOAD_END_TIME=current_timestamp,LOAD_STATUS = 'Completed'  WHERE LOAD_STATUS = 'Started' and source_name='FM' and file_name='{fl}' '''
            update_batchid_detailss = session.sql(update_batchid_q).collect()
            load_end_query_2=f'''UPDATE RAW.ADMIN_LOG set EXECUTION_DURATION = datediff(minute, LOAD_START_TIME, LOAD_END_TIME) WHERE LOAD_STATUS = 'Completed' and source_name='FM' and file_name='{fl}' '''
            load_end_details = session.sql(load_end_query_2).collect()

        except Exception as error:
            delete_query=f"delete from {raw_table} where batch_id = '{unique_key}' "
            print(delete_query)
            read_df=session.sql(delete_query).show()
            print("An expection Occured while doing the FM data ingestion")
            load_end_query_1=f'''UPDATE RAW.ADMIN_LOG set LOAD_STATUS = 'Failed' WHERE LOAD_STATUS = 'Started' and source_name='FM' and file_name='{fl}' '''
            load_end_details = session.sql(load_end_query_1).collect()
            session.close()
            raise error
        
def execute(table_name, batchId):
    config = init_config()

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

    print(f'Current batchId from fetch_fm_landing_subfolders task: {batchId}')
    print(f'Current table_name from fetch_fm_landing_subfolders task: {table_name}')

    batchIds=[]
    new_lis=list_query(session,table_name)
    path_lst = new_lis[0]
    s3_location = new_lis[1]

    # lst_1=admin_log_check(session,path_lst,s3_location)
    # s3_location=lst_1[0]
    # dag_status=lst_1[1]
    # path_lst=lst_1[2]

    lst_2=processed_files(session,path_lst,s3_location)
    s3_location=lst_2[0]
    dag_status=lst_2[1]
    path_lst=lst_2[2]
    if dag_status=='proceed':
        file_lis=fetchBatchIdsToPreprocess(session,path_lst,s3_location)
        s3_location=file_lis[0]
        path_lst=file_lis[1]
        result1=timestamp(path_lst,session,batchId)
    session.close()
    print(dag_status)
    return batchIds,dag_status

if __name__ == "__main__":
    # subfolder_to_ingest = ['BonusAwardPaid', 'CabinetProfileReceived', 'GameAvailability', 'GameComboActivated', 'GameEnded', 'GamePlayProfileReceived', 'GamePlayStatusReceived', 'NoteStacked', 'OptionListReceived', 'PrimaryGameStarted', 'SecondaryGameEnded', 'SecondaryGameStarted', 'VoucherIssued', 'VoucherRedeemed']
    subfolder_to_ingest = 'notestacked'
    # subfolder_to_ingest = None
    # subfolder_to_ingest = ''
    table_name=subfolder_to_ingest
    testBatchId = str(uuid.uuid4())

    print(table_name)
    execute(table_name, testBatchId)
