from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark import functions as F

from snowflake.snowpark.window import Window
import time
import uuid
from pathlib import Path
import configparser
import os

# To import modules from another directory into the current directory
import sys

root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, root_dir.__str__())

# Internal imports
# import common.utils.definitions as df


def init_config():
    """
    This function initializes and returns a ConfigParser object

    Returns:
        ConfigParser: A ConfigParser object.
    """

    config = configparser.ConfigParser()

    parentFolderPath = Path(__file__).parent.parent.parent
    config_file_path = os.path.join(parentFolderPath, "common/utils", "config.ini")

    config.read(config_file_path)

    return config


def fetchWagerPayouts(session, customerName, batchId):
    """
    Wager payout

    Args:
        None

    Return:
        None
    """
    pgsDf = (
        session.table("primarygamestarted_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )
    pgeDf = (
        session.table("gameended_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )

    pgsDf_cols = [
        "egmId",
        "deviceId",
        "denomId",
        "eventDatetime",
        "gameplaylogsequence",
        "g2s_playercashableamt",
        "customer",
        "g2s_wageredcashableamt",
    ]
    pgeDf_cols = [
        "egmId",
        "deviceId",
        "denomId",
        "eventDatetime",
        "gameplaylogsequence",
        "g2s_playercashableamt",
        "customer",
    ]

    pgsDf = pgsDf.select(pgsDf_cols)
    pgstimeDf = pgsDf.withColumn("rectype", lit("w"))

    pgeDf = pgeDf.select(pgeDf_cols)
    pgetimeDf = pgeDf.withColumn("g2s_wageredcashableamt", lit(None)).withColumn(
        "rectype", lit("o")
    )

    wagerDf = pgstimeDf.unionAll(pgetimeDf)
    sortwagerDf = wagerDf.withColumn(
        "sortpriority",
        F.when((wagerDf.rectype == "w"), 1).when((wagerDf.rectype == "o"), 2),
    )

    cashAmtDf = sortwagerDf.withColumn(
        "next_G2S_PLAYERCASHABLEAMT",
        F.lead(sortwagerDf.G2S_PLAYERCASHABLEAMT).over(
            Window.partitionBy(
                sortwagerDf.EGMID,
                sortwagerDf.GAMEPLAYLOGSEQUENCE,
                sortwagerDf.DEVICEID,
            ).orderBy(sortwagerDf.EVENTDATETIME, sortwagerDf.sortpriority)
        ),
    )

    filterDf = cashAmtDf.withColumn(
        "payout", cashAmtDf.next_G2S_PLAYERCASHABLEAMT - cashAmtDf.G2S_PLAYERCASHABLEAMT
    )

    filteredDf = filterDf.select(
        filterDf.rectype,
        filterDf.egmId,
        F.lit(None).alias("themeId"),
        F.lit(None).alias("themeName"),
        filterDf.deviceId,
        filterDf.denomId.alias("denomId"),
        filterDf.eventDatetime.alias("tranTimestamp"),
        filterDf.gameplaylogsequence,
        filterDf.G2S_wageredcashableamt.alias("creditMeter"),
        filterDf.payout,
        F.lit(None).alias("playerId"),
        filterDf.customer.alias("customer"),
        F.lit(None).alias("paytable"),
        F.lit(None).alias("areaid"),
        F.lit(None).alias("location"),
        F.lit(None).alias("currencyid"),
    )

    # creditMeter is calculated only for rectype 'w' records, offset of 2 in lag indicator allows us to take only rectype 'w' records
    wagerPayoutsDf = filteredDf.withColumn(
        "tranAmt",
        filteredDf.creditMeter
        - F.lag(filteredDf.creditMeter, 2).over(
            Window.partitionBy(filteredDf.egmId).orderBy(filteredDf.tranTimestamp)
        ),
    )
    primaryGamesDf = wagerPayoutsDf.select(
        wagerPayoutsDf.rectype,
        wagerPayoutsDf.egmId,
        wagerPayoutsDf.themeId,
        F.lit(None).alias("themeName"),
        wagerPayoutsDf.deviceId,
        wagerPayoutsDf.denomId,
        wagerPayoutsDf.tranTimestamp,
        wagerPayoutsDf.gameplaylogsequence,
        wagerPayoutsDf.creditMeter,
        wagerPayoutsDf.tranAmt,
        wagerPayoutsDf.payout,
        wagerPayoutsDf.playerId,
        wagerPayoutsDf.customer,
        F.lit(None).alias("paytable"),
        wagerPayoutsDf.areaid,
        wagerPayoutsDf.location,
        wagerPayoutsDf.currencyid,
    )

    # primaryGamesDf.show(500)
    return primaryGamesDf


def fetchSecondaryGames(session, customerName, batchId):
    sgsDf = (
        session.table("secondarygamestarted_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )
    sgeDf = (
        session.table("secondarygameended_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )

    # Remove duplicate records of events generated possibly as part of erroneous incremental data load
    sgsDistinctDf = sgsDf.select(
        [
            "id",
            "egmId",
            "eventDatetime",
            "deviceId",
            "denomId",
            "gamePlaylogsequence",
            "gcmdatetime",
            "commserverid",
            "customer",
            "g2s_secwageredamt",
        ]
    ).distinct()

    print(sgsDf.count())
    print(sgsDistinctDf.count())

    sgeDistinctDf = sgeDf.select(
        [
            "id",
            "egmId",
            "eventDatetime",
            "deviceId",
            "denomId",
            "gameplaylogsequence",
            "gcmdatetime",
            "commserverid",
            "customer",
            "g2s_secwonamt",
            "g2s_secwoncnt",
            "g2s_seclostcnt",
            "g2s_sectiedcnt",
        ]
    ).distinct()

    print(sgeDf.count())
    print(sgeDistinctDf.count())

    sgsDistinctDf = (
        sgsDistinctDf.withColumn("rectype", lit("x"))
        .withColumn("g2s_secwonamt", lit(None))
        .withColumn("g2s_secwoncnt", lit(None))
        .withColumn("g2s_seclostcnt", lit(None))
        .withColumn("g2s_sectiedcnt", lit(None))
    )

    sgeDistinctDf = sgeDistinctDf.withColumn("rectype", lit("y")).withColumn(
        "g2s_secwageredamt", lit(None)
    )

    # select to columns in the same order to perform union on sgsDistinctDf and sgeDistinctDf dataframes
    ordered_cols = [
        "id",
        "egmId",
        "eventDatetime",
        "deviceId",
        "denomId",
        "gameplaylogsequence",
        "gcmdatetime",
        "commserverid",
        "customer",
        "g2s_secwonamt",
        "g2s_secwoncnt",
        "g2s_seclostcnt",
        "g2s_sectiedcnt",
        "g2s_secwageredamt",
        "rectype",
    ]

    sgsDistinctDf = sgsDistinctDf.select(ordered_cols)
    sgeDistinctDf = sgeDistinctDf.select(ordered_cols)

    bonusDf = sgsDistinctDf.unionAll(sgeDistinctDf)
    sortwagerDf = bonusDf.withColumn(
        "sortpriority",
        F.when((bonusDf.rectype == "x"), 1).when((bonusDf.rectype == "y"), 2),
    )

    # bonusDf = sgsDistinctDf.join(sgeDistinctDf, ['egmId','gameplaylogsequence', 'deviceId'], 'inner')

    print(sortwagerDf.count())

    filteredDf = sortwagerDf.select(
        sortwagerDf.egmId,
        F.lit(None).alias("themeId"),
        F.lit(None).alias("themeName"),
        sortwagerDf.deviceId,
        sortwagerDf.denomId,
        sortwagerDf.eventDatetime.alias("tranTimestamp"),
        sortwagerDf.gameplaylogsequence,
        sortwagerDf.G2S_secwageredAmt.alias("creditMeter"),
        (
            sortwagerDf.G2S_secwageredAmt
            - F.lag(sortwagerDf.G2S_secwageredAmt, 2).over(
                Window.partitionBy(sortwagerDf.egmId).orderBy(
                    sortwagerDf.eventDatetime, sortwagerDf.sortpriority
                )
            )
        ).alias("tranAmt"),
        (
            sortwagerDf.G2S_secwonamt
            - F.lag(sortwagerDf.G2S_secwonamt, 2).over(
                Window.partitionBy(sortwagerDf.egmId).orderBy(
                    sortwagerDf.eventDatetime, sortwagerDf.sortpriority
                )
            )
        ).alias("payout"),
        F.lit(None).alias("playerId"),
        sortwagerDf.customer,
        F.lit(None).alias("paytable"),
        sortwagerDf.rectype,
        F.lit(None).alias("areaid"),
        F.lit(None).alias("location"),
        F.lit(None).alias("currencyid"),
    )

    bonusPayoutsDf = filteredDf

    # Extract only the required columns for RAW data model in the same order followed for union
    F.lit("x").alias("rectype"),
    secondaryGamesDf = bonusPayoutsDf.select(
        bonusPayoutsDf.rectype,
        bonusPayoutsDf.egmId,
        bonusPayoutsDf.themeId,
        F.lit(None).alias("themeName"),
        bonusPayoutsDf.deviceId,
        bonusPayoutsDf.denomId,
        bonusPayoutsDf.tranTimestamp,
        bonusPayoutsDf.gameplaylogsequence,
        bonusPayoutsDf.creditMeter,
        bonusPayoutsDf.tranAmt,
        bonusPayoutsDf.payout,
        bonusPayoutsDf.playerId,
        bonusPayoutsDf.customer,
        F.lit(None).alias("paytable"),
        bonusPayoutsDf.areaid,
        bonusPayoutsDf.location,
        bonusPayoutsDf.currencyid,
    )

    # secondaryGamesDf.show(500)

    return secondaryGamesDf


def fetchBonusPayouts(session, customerName, batchId):
    bapdf = (
        session.table("bonusawardpaid_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )

    BonusPayouts = bapdf.select(
        F.lit("bonus_award_paid").alias("rectype"),
        bapdf.egmId,
        F.lit(None).alias("themeId"),
        F.lit(None).alias("themeName"),
        F.lit(None).alias("deviceId"),
        F.lit(None).alias("denomId"),
        bapdf.eventDatetime.alias("tranTimestamp"),
        bapdf.bonuslogsequence.alias("GamePlayLogSequence"),
        bapdf.g2s_cashableinamt.alias("creditMeter"),
        bapdf.g2s_playercashableamt.alias("tranAmt"),
        F.lit(None).alias("payout"),
        F.lit(None).alias("playerId"),
        bapdf.customer.alias("customer"),
        F.lit(None).alias("paytable"),
        F.lit(None).alias("areaid"),
        F.lit(None).alias("location"),
        F.lit(None).alias("currencyid"),
    )
    return BonusPayouts


def fetchCashEvents(session, customerName, batchId):
    nsdf = (
        session.table("notestacked_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )
    vidf = (
        session.table("voucherissued_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )
    vrdf = (
        session.table("voucherredeemed_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )

    notestackeddf = nsdf.select(
        F.lit("n").alias("rectype"),
        nsdf.egmId,
        F.lit(None).alias("themeId"),
        F.lit(None).alias("themeName"),
        F.lit(None).alias("deviceId"),
        F.lit(None).alias("denomId"),
        nsdf.eventDatetime.alias("tranTimestamp"),
        nsdf.noteacceptorlogsequence.alias("GamePlayLogSequence"),
        nsdf.g2s_currencyinamt.alias("creditMeter"),
        nsdf.g2s_playercashableamt.alias("tranAmt"),
        F.lit(None).alias("payout"),
        F.lit(None).alias("playerId"),
        nsdf.customer.alias("customer"),
        F.lit(None).alias("paytable"),
        F.lit(None).alias("areaid"),
        F.lit(None).alias("location"),
        F.lit(None).alias("currencyid"),
    )

    voucherissueddf = vidf.select(
        F.lit("v").alias("rectype"),
        vidf.egmid,
        F.lit(None).alias("themeId"),
        F.lit(None).alias("themeName"),
        F.lit(None).alias("deviceId"),
        F.lit(None).alias("denomId"),
        vidf.eventDatetime.alias("tranTimestamp"),
        vidf.transactionid.alias("GamePlayLogSequence"),
        vidf.transferamt.alias("creditMeter"),
        vidf.voucheramt.alias("tranAmt"),
        F.lit(None).alias("payout"),
        vidf.playerId.alias("playerId"),
        vidf.customer.alias("customer"),
        F.lit(None).alias("paytable"),
        F.lit(None).alias("areaid"),
        F.lit(None).alias("location"),
        F.lit(None).alias("currencyid"),
    )

    voucherredeemeddf = vrdf.select(
        F.lit("t").alias("rectype"),
        vrdf.egmid,
        F.lit(None).alias("themeId"),
        F.lit(None).alias("themeName"),
        F.lit(None).alias("deviceId"),
        F.lit(None).alias("denomId"),
        vrdf.eventDatetime.alias("tranTimestamp"),
        vrdf.transactionid.alias("GamePlayLogSequence"),
        vrdf.g2s_cashableinamt.alias("creditMeter"),
        vrdf.g2s_playercashableamt.alias("tranAmt"),
        F.lit(None).alias("payout"),
        vrdf.playerId.alias("playerId"),
        vrdf.customer.alias("customer"),
        F.lit(None).alias("paytable"),
        F.lit(None).alias("areaid"),
        F.lit(None).alias("location"),
        F.lit(None).alias("currencyid"),
    )
    cashDf = notestackeddf.unionAll(voucherissueddf).unionAll(voucherredeemeddf)
    return cashDf


def fetchMiscEvents(session, customerName, batchId):
    gcadf = (
        session.table("gamecomboactivated_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )
    # pssdf = session.table("playersessionstarted_raw").filter(F.col('customer') == customerName).filter(F.col('batch_id') == batchId)
    # psedf = session.table("playersessionended_raw").filter(F.col('customer') == customerName).filter(F.col('batch_id') == batchId)
    pssdf = (
        session.table("stm_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )
    psedf = (
        session.table("stm_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )
    gpprdf = (
        session.table("gameplayprofilereceived_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )
    gpsrdf = (
        session.table("gameplaystatusreceived_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )
    crdf = (
        session.table("cabinetprofilereceived_raw")
        .filter(F.col("customer") == customerName)
        .filter(F.col("batch_id") == batchId)
    )

    gamecomboactivateddf = gcadf.select(
        F.lit("game_changed").alias("rectype"),
        gcadf.egmid,
        gcadf.themeId.alias("themeId"),
        F.lit(None).alias("themeName"),
        gcadf.deviceId.alias("deviceId"),
        gcadf.denomId.alias("denomId"),
        gcadf.eventDatetime.alias("tranTimestamp"),
        F.lit(None).alias("GamePlayLogSequence"),
        F.lit(None).alias("creditMeter"),
        gcadf.maxwagercredits.alias("tranAmt"),
        F.lit(None).alias("payout"),
        F.lit(None).alias("playerId"),
        gcadf.customer.alias("customer"),
        gcadf.paytableid.alias("paytable"),
        F.lit(None).alias("areaid"),
        F.lit(None).alias("location"),
        F.lit(None).alias("currencyid"),
    )

    playersessionstarteddf = pssdf.select(
        F.lit("pss").alias("rectype"),
        pssdf.egmid,
        pssdf.slotthemeid.alias("themeId"),
        pssdf.slotthemename.alias("themeName"),
        F.lit(None).alias("deviceId"),
        F.lit(None).alias("denomId"),
        pssdf.sessionstartdate.alias("tranTimestamp"),
        F.lit(None).alias("GamePlayLogSequence"),
        F.lit(None).alias("creditMeter"),
        F.lit(None).alias("tranAmt"),
        F.lit(None).alias("payout"),
        F.lit(None).alias("playerId"),
        pssdf.customer.alias("customer"),
        F.lit(None).alias("paytable"),
        F.lit(None).alias("areaid"),
        F.lit(None).alias("location"),
        F.lit(None).alias("currencyid"),
    )

    playersessionendeddf = psedf.select(
        F.lit("pse").alias("rectype"),
        psedf.egmid,
        psedf.slotthemeid.alias("themeId"),
        psedf.slotthemename.alias("themeName"),
        F.lit(None).alias("deviceId"),
        F.lit(None).alias("denomId"),
        psedf.sessionenddate.alias("tranTimestamp"),
        F.lit(None).alias("GamePlayLogSequence"),
        F.lit(None).alias("creditMeter"),
        F.lit(None).alias("tranAmt"),
        F.lit(None).alias("payout"),
        F.lit(None).alias("playerId"),
        psedf.customer.alias("customer"),
        F.lit(None).alias("paytable"),
        F.lit(None).alias("areaid"),
        F.lit(None).alias("location"),
        F.lit(None).alias("currencyid"),
    )

    gameplayprofilereceiveddf = gpprdf.select(
        F.lit("gppr").alias("rectype"),
        gpprdf.egmid,
        F.lit(None).alias("themeId"),
        F.lit(None).alias("themeName"),
        gpprdf.deviceId.alias("deviceId"),
        F.lit(None).alias("denomId"),
        gpprdf.eventDatetime.alias("tranTimestamp"),
        F.lit(None).alias("GamePlayLogSequence"),
        F.lit(None).alias("creditMeter"),
        gpprdf.maxwagercredits.alias("tranAmt"),
        F.lit(None).alias("payout"),
        F.lit(None).alias("playerId"),
        gpprdf.customer.alias("customer"),
        F.lit(None).alias("paytable"),
        F.lit(None).alias("areaid"),
        F.lit(None).alias("location"),
        F.lit(None).alias("currencyid"),
    )

    gameplaystatusreceiveddf = gpsrdf.select(
        F.lit("gpsr").alias("rectype"),
        gpsrdf.egmid,
        gpsrdf.themeId.alias("themeId"),
        F.lit(None).alias("themeName"),
        gpsrdf.deviceId.alias("deviceId"),
        F.lit(None).alias("denomId"),
        gpsrdf.eventDatetime.alias("tranTimestamp"),
        F.lit(None).alias("GamePlayLogSequence"),
        F.lit(None).alias("creditMeter"),
        F.lit(None).alias("tranAmt"),
        F.lit(None).alias("payout"),
        F.lit(None).alias("playerId"),
        gpsrdf.customer.alias("customer"),
        gpsrdf.paytableid.alias("paytable"),
        F.lit(None).alias("areaid"),
        F.lit(None).alias("location"),
        F.lit(None).alias("currencyid"),
    )
    cabinetprofilereceiveddf = crdf.select(
        F.lit("cbpr").alias("rectype"),
        crdf.egmid,
        F.lit(None).alias("themeId"),
        F.lit(None).alias("themeName"),
        F.lit(None).alias("deviceId"),
        F.lit(None).alias("denomId"),
        crdf.eventDatetime.alias("tranTimestamp"),
        F.lit(None).alias("GamePlayLogSequence"),
        F.lit(None).alias("creditMeter"),
        F.lit(None).alias("tranAmt"),
        F.lit(None).alias("payout"),
        F.lit(None).alias("playerId"),
        crdf.customer.alias("customer"),
        F.lit(None).alias("paytable"),
        crdf.areaid,
        crdf.location,
        crdf.currencyid,
    )

    miscDf = (
        gamecomboactivateddf.unionAll(playersessionstarteddf)
        .unionAll(playersessionendeddf)
        .unionAll(gameplayprofilereceiveddf)
        .unionAll(gameplaystatusreceiveddf)
        .unionAll(cabinetprofilereceiveddf)
    )
    return miscDf


def unionAllRectypes(primaryGamesDf, secondaryGamesDf, bonusPayoutsDf, cashDf, miscDf):
    print("Initiated: Union of individual rectypes of raw data model")

    rawDf = (
        primaryGamesDf.unionAll(secondaryGamesDf)
        .unionAll(bonusPayoutsDf)
        .unionAll(cashDf)
        .unionAll(miscDf)
    )

    print("Completed: Union of individual rectypes of raw data model")

    return rawDf


def saveRawDataModel(session, rawDf, customerName, batchId):
    print("Initiated: Construction and save of FM raw data model")

    print(f"Current customer: { customerName }")

    rawModelDf = rawDf.withColumn("batch_Id", F.lit(batchId))

    rawModelDf.write.mode("append").saveAsTable("RAW.FM_RAW")

    print("Completed: Construction and save of FM raw data model")


def execute(customerName, batchId):
    if (
        (customerName == None)
        | (customerName == "")
        | (batchId == None)
        | (batchId == "")
    ):
        print("Invalid customer name or batchId")
        return

    print(f"Current batchId from fetch_fm_landing_subfolders task: {batchId}")
    print(f"Current customerName from await_ingestion_completion task: {customerName}")

    exec_start_time = time.time()

    config = init_config()

    # region initialize session

    schema = "FM_Landing"

    CONNECTION_PARAMETERS = {
        "account": config.get("snowflake_connection", "account"),
        "user": config.get("snowflake_connection", "user"),
        "password": config.get("snowflake_connection", "password"),
        "schema": schema,
        "database": config.get("snowflake_connection", "database"),
        "warehouse": config.get("snowflake_connection", "warehouse"),
        "role": config.get("snowflake_connection", "role"),
    }

    session = Session.builder.configs(CONNECTION_PARAMETERS).create()

    print(
        session.sql(
            "select current_warehouse(), current_database(), current_schema()"
        ).collect()
    )

    # endregion initialize session

    primaryGamesDf = fetchWagerPayouts(session, customerName, batchId)

    secondaryGamesDf = fetchSecondaryGames(session, customerName, batchId)

    bonuspayouts = fetchBonusPayouts(session, customerName, batchId)

    cashEvents = fetchCashEvents(session, customerName, batchId)

    miscEvents = fetchMiscEvents(session, customerName, batchId)

    rawDf = unionAllRectypes(
        primaryGamesDf, secondaryGamesDf, bonuspayouts, cashEvents, miscEvents
    )

    saveRawDataModel(session, rawDf, customerName, batchId)

    # create_fm_raw(session)

    exec_end_time = time.time()

    exec_total_time = exec_end_time - exec_start_time

    print("Total time taken: ", exec_total_time)

    session.close()


if __name__ == "__main__":
    # Unit test batch ids
    # testCustomerName = 'casino2k'
    # testCustomerName = 'meridian'
    # testCustomerName = 'rank'
    # testCustomerName = 'colusa'
    # testCustomerName = 'eldorado'
    # testCustomerName = 'venicetb'
    # testCustomerName = 'victgate'
    # testCustomerName = 'codera'
    # testCustomerName = 'aisa'
    # testCustomerName = 'tecvivo'
    # testCustomerName = 'veneziag'
    # testCustomerName = 'favbet'
    # testCustomerName = 'genting'
    # testCustomerName = 'cdlv'
    testCustomerName = "uil"
    randomId = str(uuid.uuid4())

    # For unit testing, before executing, ensure the FM_Landing tables
    #  have records with the batchId mentioned below
    testBatchId = "f72fbf50-996b-45a4-b029-a477bf824c04"

    execute(testCustomerName, testBatchId)
