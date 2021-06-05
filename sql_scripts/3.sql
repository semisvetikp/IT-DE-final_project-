select
         fio
        , PASSPORT_NUM
        , phone
        , TRANSACTION_DATE
        , sysdate
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
                           left join SVET_DWH_FACT_TRANSACTIONS tr
                                     on trim(c.CARD_NUM) = tr.CARD_NUM
                           left join SVET_DWH_DIM_TERMINALS_HIST t
                                     on t.TERMINAL_ID = tr.TERMINAL
              )
         where last_city <> TERMINAL_CITY
                AND min < 60 
                and OPER_RESULT = 'SUCCESS'
     )
where num = 2 and 
      trunc(TRANSACTION_DATE, 'DD') = trunc((select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_FACT_TRANSACTIONS'), 'DD');