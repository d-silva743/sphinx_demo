from snowflake.snowpark.functions import *
dff = session.sql("""select  RECTYPE,EGMID, TRANTIMESTAMP,GAMEPLAYLOGSEQUENCE,CREDITMETER,TRANAMT,customer,
lead(TRANTIMESTAMP) over (partition by EGMID order by EGMID, TRANTIMESTAMP,GAMEPLAYLOGSEQUENCE) PREV_TRANTIMESTAMP
,case when CAST(DATEDIFF(seconds,TRANTIMESTAMP, PREV_TRANTIMESTAMP) as INT) > 120 then 'session_end' else 'continue' end AS TIME_GAP
,case when CREDITMETER <= TRANAMT then 'session_end' else 'countinue' end as credit_status
, case when rectype='game_changed' then 0 when rectype='n' then 1 when rectype='c' then 2 when rectype='t' then 3 when rectype='w' then 4 when rectype='o' then 5 when rectype='x' then 6 when rectype='y' then 7 else 8 end as rectype_order
, case when RECTYPE='v' then 'cashout' else 'other rectype' end as calc_col
,(row_number() over (order by EGMID, TRANTIMESTAMP,GAMEPLAYLOGSEQUENCE,rectype_order)) - (row_number() over (partition by calc_col order by EGMID, TRANTIMESTAMP,GAMEPLAYLOGSEQUENCE,rectype_order) ) as grp
,case when RECTYPE='v' then row_number() over (partition by calc_col order by EGMID, TRANTIMESTAMP,GAMEPLAYLOGSEQUENCE,rectype_order) else grp+1 end as final_num
from "IGT_BIT_10205"."RAW"."FM_RAW"
where EGMID in ('IGT_0001295F7FB6') order by EGMID, TRANTIMESTAMP,GAMEPLAYLOGSEQUENCE,rectype_order;""")