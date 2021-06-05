--1
insert into itde1.SVET_REP_FRAUD (fio, PASSPORT, PHONE,  event_dt, REPORT_DT, event_type)
select
    LAST_NAME || ' ' || FIRST_NAME || ' '|| PATRONYMIC as fio,
        PASSPORT_NUM,
        PHONE
        , TRANSACTION_DATE
        , (select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_FACT_TRANSACTIONS') as report_dt
        , '1'
from bank.CLIENTS cl
left join bank.ACCOUNTS ak
    on cl.CLIENT_ID = ak.CLIENT
left join bank.cards c
    on ak.account = c.ACCOUNT
left join SVET_DWH_FACT_TRANSACTIONS tr
    on trim(c.CARD_NUM) = tr.CARD_NUM
where
    trunc(TRANSACTION_DATE, 'DD') = trunc((select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_FACT_TRANSACTIONS'), 'DD')
    and (PASSPORT_NUM in (select PASSPORT_NUM from SVET_DWH_FACT_PSSPRT_BLCKLST)
   OR coalesce(cl.PASSPORT_VALID_TO, to_date( '2999-12-31 00:00:00', 'YYYY-MM-DD HH24:MI:SS' )) <
   (select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_FACT_TRANSACTIONS'));

--2

insert into itde1.SVET_REP_FRAUD (fio, PASSPORT, PHONE,  event_dt, REPORT_DT, event_type)
select
    LAST_NAME || ' ' || FIRST_NAME || ' '|| PATRONYMIC as fio,
        PASSPORT_NUM,
        PHONE
        , TRANSACTION_DATE
        , (select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_FACT_TRANSACTIONS') as report_dt
        , '2'
from bank.CLIENTS cl
left join bank.ACCOUNTS ak
    on cl.CLIENT_ID = ak.CLIENT
left join bank.cards c
    on ak.account = c.ACCOUNT
left join SVET_DWH_FACT_TRANSACTIONS tr
    on trim(c.CARD_NUM) = tr.CARD_NUM
where
    trunc(TRANSACTION_DATE, 'DD') = trunc((select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_FACT_TRANSACTIONS'), 'DD')
    and ak.VALID_TO <
   (select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_FACT_TRANSACTIONS');

--3
insert into itde1.SVET_REP_FRAUD (fio, PASSPORT, PHONE,  event_dt, REPORT_DT, event_type)
select
         fio
        , PASSPORT_NUM
        , phone
        , TRANSACTION_DATE
        , (select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_FACT_TRANSACTIONS') as report_dt
        , '3' as type
from (
         select
                fio
                , PASSPORT_NUM
                , phone
                , TRANSACTION_DATE
                , OPER_RESULT
                , rank() over (partition by PASSPORT_NUM order by TRANSACTION_DATE) as num
         from (
                  select LAST_NAME || ' ' || FIRST_NAME || ' ' || PATRONYMIC                            as fio
                       , PASSPORT_NUM
                       , PHONE
                       , TRANSACTION_DATE
                       , TERMINAL_CITY
                       , OPER_RESULT
                       , lead(TERMINAL_CITY) over (partition by PASSPORT_NUM order by TRANSACTION_DATE) as last_city
                       , extract(minute from
                                 (lead(TRANSACTION_DATE) over (partition by PASSPORT_NUM order by TRANSACTION_DATE) -
                                  TRANSACTION_DATE) day to second)                                        as min
                  from bank.CLIENTS cl
                           left join bank.ACCOUNTS ak
                                     on cl.CLIENT_ID = ak.CLIENT
                           left join bank.cards c
                                     on ak.account = c.ACCOUNT
                           left join itde1.SVET_DWH_FACT_TRANSACTIONS tr
                                     on trim(c.CARD_NUM) = tr.CARD_NUM
                           left join itde1.SVET_DWH_DIM_TERMINALS_HIST t
                                     on t.TERMINAL_ID = tr.TERMINAL
              )
         where last_city <> TERMINAL_CITY
                AND min < 60
     )
where num = 2 and
      trunc(TRANSACTION_DATE, 'DD') = trunc((select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_FACT_TRANSACTIONS'), 'DD');
