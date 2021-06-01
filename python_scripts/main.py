import pandas as pd 
import numpy as np
import re
import os
import sys
import shutil
import jaydebeapi

conn = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:itde1/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
['itde1', 'bilbobaggins'],
'/Users/jradioac/Desktop/data/python_scripts/ojdbc8.jar')
curs = conn.cursor()

#############################################
# Создание dataframes из файлов csv и excel #
#############################################
listOfFiles = os.listdir('.')

r = re.compile("passport_blacklist_*")
file = list(filter(r.match, listOfFiles))
if (len(file) == 1):
	blacklist = pd.read_excel( file[0], dtype={'date':str, 'passport' : str}, sheet_name='blacklist', header=0, index_col=None )
else:
	print("Some wrong with file passport_blacklist")
	sys.exit(1)
source_path = file[0] + ".backup"
os.rename(file[0], source_path)
if (os.path.exists(source_path) and os.path.exists("./archive")):
	shutil.move(source_path, "./archive")

r = re.compile("transaction_*")
file = list(filter(r.match, listOfFiles))
if (len(file) == 1):
	transaction = pd.read_csv( file[0], delimiter=';')
else:
	print("Some wrong with file transaction")
	sys.exit(1)
source_path = file[0] + ".backup"
os.rename(file[0], source_path)
if (os.path.exists(source_path) and os.path.exists("./archive")):
	shutil.move(source_path, "./archive")

r = re.compile("terminals_*")
file = list(filter(r.match, listOfFiles))
if (len(file) == 1):
	terminals = pd.read_excel( file[0], sheet_name='terminals', header=0, index_col=None )
else:
	print("Some wrong with file 'terminals'")
	sys.exit(1)

#Получаем дату загрузки данных о терминалах
terminals_date = (file[0].replace('.xlsx', '').replace('terminals_', ''),)
source_path = file[0] + ".backup"
os.rename(file[0], source_path)
if (os.path.exists(source_path) and os.path.exists("./archive")):
	shutil.move(source_path, "./archive")

#Загружаем дату терминалов в таблицу  META
curs.execute("""UPDATE ITDE1.SVET_META_LOADING
set LAST_UPDATE =  to_date( ?, 'DDMMYYYY')
where DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_DIM_TERMINALS_HIST'""", terminals_date)

#Убираем дубликаты из терминалов 

curs.execute("""DELETE FROM ITDE1.SVET_STG_TERMINALS
WHERE EXISTS
    (
    SELECT *
    FROM ITDE1.SVET_DWH_DIM_TERMINALS_HIST tgt
    WHERE tgt.TERMINAL_ID = ITDE1.SVET_STG_TERMINALS.TERMINAL_ID
        AND tgt.TERMINAL_TYPE = ITDE1.SVET_STG_TERMINALS.TERMINAL_TYPE
        AND tgt.TERMINAL_CITY = ITDE1.SVET_STG_TERMINALS.TERMINAL_CITY
        AND tgt.TERMINAL_ADDRESS = ITDE1.SVET_STG_TERMINALS.terminal_address
    ) """)

######################################
# Выполняем инкрементальную загрузку #
######################################

#-- начало транзакции

#-- 1. Очистка данных из STG
curs.execute( "TRUNCATE TABLE ITDE1.SVET_STG_ACCOUNTS")
curs.execute( "TRUNCATE TABLE ITDE1.SVET_STG_CARDS")
curs.execute( "TRUNCATE TABLE ITDE1.SVET_STG_CLIENTS")
curs.execute( "TRUNCATE TABLE ITDE1.SVET_STG_PASSPORT_BLACKLIST")
curs.execute( "TRUNCATE TABLE ITDE1.SVET_STG_TERMINALS")
curs.execute( "TRUNCATE TABLE ITDE1.SVET_STG_TRANSACTIONS")

#Выполнение SQL запросов в Oracle и выгрузка dataframes
curs.executemany( "insert into ITDE1.SVET_STG_PASSPORT_BLACKLIST (entry_dt, passport_num) values (?, ?)",
blacklist.values.tolist() )

curs.executemany( "insert into ITDE1.SVET_STG_TERMINALS values (?, ?, ?, ?)",
terminals.values.tolist() )

curs.executemany( "insert into ITDE1.SVET_STG_TRANSACTIONS values (?,?,?,?,?,?,?)",
transaction.values.tolist() )
#
curs.execute( """INSERT INTO  ITDE1.SVET_STG_ACCOUNTS(ACCOUNT, VALID_TO, CLIENT, CREATE_DT, UPDATE_DT )
SELECT
    ACCOUNT,
    VALID_TO,
    CLIENT,
    CREATE_DT,
    UPDATE_DT
FROM BANK.ACCOUNTS
WHERE COALESCE( UPDATE_DT, CREATE_DT ) > (
	SELECT LAST_UPDATE FROM ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_DIM_ACCOUNTS_HIST'
)""")

curs.execute( """INSERT INTO  ITDE1.SVET_STG_CARDS(CARD_NUM, ACCOUNT, CREATE_DT, UPDATE_DT)
SELECT
    CARD_NUM,
    ACCOUNT,
    CREATE_DT,
    UPDATE_DT
FROM BANK.CARDS
WHERE COALESCE( UPDATE_DT, CREATE_DT ) > (
	SELECT LAST_UPDATE FROM ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_DIM_CARDS_HIST'
)""")

curs.execute( """INSERT INTO  ITDE1.SVET_STG_CLIENTS(CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, CREATE_DT, UPDATE_DT)
SELECT
    CLIENT_ID,
    LAST_NAME,
    FIRST_NAME,
    PATRONYMIC,
    DATE_OF_BIRTH,
    PASSPORT_NUM,
    PASSPORT_VALID_TO,
    PHONE,
    CREATE_DT,
    UPDATE_DT
FROM BANK.CLIENTS
WHERE COALESCE( UPDATE_DT, CREATE_DT ) > (
	SELECT LAST_UPDATE FROM ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_DIM_CLIENTS_HIST'
)""")

#-- 3. Обновляем обновленные строки в хранилище

#-- Вставка фактов

curs.execute( """INSERT INTO ITDE1.SVET_DWH_FACT_TRANSACTIONS(transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal )
SELECT
    transaction_id,
    to_date(transaction_date, 'YYYY-MM-DD HH24:MI:SS'),
    TO_NUMBER (REPLACE(amount, ',', '.')),
    card_num,
    oper_type,
    oper_result,
    terminal
FROM ITDE1.SVET_STG_TRANSACTIONS""")

curs.execute( """INSERT INTO itde1.SVET_DWH_FACT_PSSPRT_BLCKLST ( entry_dt, passport_num)
SELECT
       to_date(entry_dt, 'YYYY-MM-DD HH24:MI:SS'),
       passport_num
from ITDE1.SVET_STG_PASSPORT_BLACKLIST""")

#-- Загрузка измерений

#ACCOUNTS
curs.execute( """INSERT INTO ITDE1.SVET_DWH_DIM_ACCOUNTS_HIST (ACCOUNT, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
select
    ACCOUNT,
    VALID_TO,
    CLIENT,
    UPDATE_DT,
    to_date( '2999-12-31', 'YYYY-MM-DD' ),
    'N'
from ITDE1.SVET_STG_ACCOUNTS""")

curs.execute( """MERGE INTO ITDE1.SVET_DWH_DIM_ACCOUNTS_HIST tgt
USING ITDE1.SVET_STG_ACCOUNTS stg
ON ( tgt.ACCOUNT = stg.ACCOUNT  and tgt.EFFECTIVE_FROM < stg.UPDATE_DT )
WHEN MATCHED THEN UPDATE SET
    tgt.EFFECTIVE_TO = to_date(stg.UPDATE_DT) - interval '1' second
        WHERE tgt.EFFECTIVE_TO =  to_date( '2999-12-31', 'YYYY-MM-DD' )""")

#CARDS
curs.execute( """INSERT INTO ITDE1.SVET_DWH_DIM_CARDS_HIST (CARD_NUM, ACCOUNT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
select
    CARD_NUM,
    ACCOUNT,
    UPDATE_DT,
    to_date( '2999-12-31', 'YYYY-MM-DD' ),
    'N'
from ITDE1.SVET_STG_CARDS""")

curs.execute( """MERGE INTO ITDE1.SVET_DWH_DIM_CARDS_HIST tgt
USING ITDE1.SVET_STG_CARDS stg
ON (tgt.CARD_NUM = stg.CARD_NUM and tgt.EFFECTIVE_FROM < stg.UPDATE_DT )
WHEN MATCHED THEN UPDATE SET
    tgt.EFFECTIVE_TO = to_date(stg.UPDATE_DT) - interval '1' second
        WHERE tgt.EFFECTIVE_TO =  to_date( '2999-12-31', 'YYYY-MM-DD' )""")

#CLIENTS
curs.execute( """INSERT INTO ITDE1.SVET_DWH_DIM_CLIENTS_HIST (CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
select
    CLIENT_ID,
    LAST_NAME,
    FIRST_NAME,
    PATRONYMIC,
    DATE_OF_BIRTH,
    PASSPORT_NUM,
    PASSPORT_VALID_TO,
    PHONE,
    UPDATE_DT,
    to_date( '2999-12-31', 'YYYY-MM-DD' ),
    'N'
from ITDE1.SVET_STG_CLIENTS""")

curs.execute( """MERGE INTO ITDE1.SVET_DWH_DIM_CLIENTS_HIST tgt
USING ITDE1.SVET_STG_CLIENTS stg
ON (tgt.CLIENT_ID = stg.CLIENT_ID and tgt.EFFECTIVE_FROM < stg.UPDATE_DT )
WHEN MATCHED THEN UPDATE SET
    tgt.EFFECTIVE_TO = to_date(stg.UPDATE_DT) - interval '1' second
        WHERE tgt.EFFECTIVE_TO =  to_date( '2999-12-31', 'YYYY-MM-DD' )""")

#TERMINALS
curs.execute( """INSERT INTO ITDE1.SVET_DWH_DIM_TERMINALS_HIST (TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
select
    TERMINAL_ID,
    TERMINAL_TYPE,
    TERMINAL_CITY,
    TERMINAL_ADDRESS,
    (select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_DIM_TERMINALS_HIST'),
    to_date( '2999-12-31', 'YYYY-MM-DD' ),
    'N'
from ITDE1.SVET_STG_TERMINALS""")

curs.execute( """MERGE INTO ITDE1.SVET_DWH_DIM_TERMINALS_HIST tgt
USING ITDE1.SVET_STG_TERMINALS stg
ON (tgt.TERMINAL_ID = stg.TERMINAL_ID and tgt.EFFECTIVE_FROM < 
                                          (select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_DIM_TERMINALS_HIST') )
WHEN MATCHED THEN UPDATE SET
    tgt.EFFECTIVE_TO = (select LAST_UPDATE from ITDE1.SVET_META_LOADING WHERE DBNAME = 'ITDE1' AND TABLENAME = 'SVET_DWH_DIM_TERMINALS_HIST') 
                           - interval '1' second
        WHERE tgt.EFFECTIVE_TO =  to_date( '2999-12-31', 'YYYY-MM-DD' )""")

#-- 4. Захватываем ключи для проверки удалений (опционально)

curs.execute( """insert into ITDE1.SVET_STG_DEL_ACCOUNTS( ACCOUNT )
select ACCOUNT from bank.account""")

curs.execute( """insert into ITDE1.SVET_STG_DEL_CARDS( CARD_NUM )
select CARD_NUM from bank.cards""")

curs.execute( """insert into ITDE1.SVET_STG_DEL_CLIENTS( CLIENT_ID )
select CLIENT_ID from bank.CLIENTS""")

curs.executemany( "insert into ITDE1.SVET_STG_DEL_TERMINALS values (?)",
terminals[['terminal_id']].values.tolist())


#-- 5. Удаляем удаленные записи в целевой таблице (опционально)

#-- открываем новую версию (insert) и закрываем предыдущую версию (update)
#ACCOUNTS
curs.execute( """insert into ITDE1.SVET_DWH_DIM_ACCOUNTS_HIST (ACCOUNT, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
select
    tbl.ACCOUNT,
    tbl.VALID_TO,
    tbl.CLIENT,
	to_date(tbl.EFFECTIVE_TO) + interval '1' second,
	to_date( '2999-12-31', 'YYYY-MM-DD' ),
	'Y'
from
    (select t.*
     from ITDE1.SVET_DWH_DIM_ACCOUNTS_HIST t
    left join ITDE1.SVET_STG_DEL_ACCOUNTS s
    on t.ACCOUNT = s.ACCOUNT
        and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
        and DELETED_FLG = 'N'
    where s.ACCOUNT is null ) tbl""")

curs.execute( """update ITDE1.SVET_DWH_DIM_ACCOUNTS_HIST
set effective_to = sysdate - interval '1' second
where ACCOUNT in (
	select t.ACCOUNT
	from ITDE1.SVET_DWH_DIM_ACCOUNTS_HIST t
	left join ITDE1.SVET_STG_DEL_ACCOUNTS s
	on t.ACCOUNT = s.ACCOUNT
		and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and DELETED_FLG = 'N'
	where s.ACCOUNT is null )
and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
and EFFECTIVE_FROM < sysdate""")

#CARDS
curs.execute( """insert into ITDE1.SVET_DWH_DIM_CARDS_HIST (CARD_NUM, ACCOUNT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
select
    tbl.CARD_NUM,
    tbl.ACCOUNT,
	to_date(tbl.EFFECTIVE_TO) + interval '1' second,
	to_date( '2999-12-31', 'YYYY-MM-DD' ),
	'Y'
from
    (select t.*
     from ITDE1.SVET_DWH_DIM_CARDS_HIST t
    left join ITDE1.SVET_STG_DEL_CARDS s
    on t.CARD_NUM = s.CARD_NUM
        and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
        and DELETED_FLG = 'N'
    where s.CARD_NUM is null ) tbl""")

curs.execute( """update ITDE1.SVET_DWH_DIM_CARDS_HIST
set effective_to = sysdate - interval '1' second
where CARD_NUM in (
	select t.CARD_NUM
	from ITDE1.SVET_DWH_DIM_CARDS_HIST t
	left join ITDE1.SVET_STG_DEL_CARDS s
	on t.CARD_NUM = s.CARD_NUM
		and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and DELETED_FLG = 'N'
	where s.CARD_NUM is null )
and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
and EFFECTIVE_FROM < sysdate""")

#CLIENTS
curs.execute( """insert into ITDE1.SVET_DWH_DIM_CLIENTS_HIST (CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
select
    tbl.CLIENT_ID,
    tbl.LAST_NAME,
    tbl.FIRST_NAME,
    tbl.PATRONYMIC,
    tbl.DATE_OF_BIRTH,
    tbl.PASSPORT_NUM,
    tbl.PASSPORT_VALID_TO,
    tbl.PHONE,
	to_date(tbl.EFFECTIVE_TO) + interval '1' second,
	to_date( '2999-12-31', 'YYYY-MM-DD' ),
	'Y'
from
    (select t.*
     from ITDE1.SVET_DWH_DIM_CLIENTS_HIST t
    left join ITDE1.SVET_STG_DEL_CLIENTS s
    on t.CLIENT_ID = s.CLIENT_ID
        and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
        and DELETED_FLG = 'N'
    where s.CLIENT_ID is null ) tbl""")

curs.execute( """update ITDE1.SVET_DWH_DIM_CLIENTS_HIST
set effective_to = sysdate - interval '1' second
where CLIENT_ID in (
	select t.CLIENT_ID
	from ITDE1.SVET_DWH_DIM_CLIENTS_HIST t
	left join ITDE1.SVET_STG_DEL_CLIENTS s
	on t.CLIENT_ID = s.CLIENT_ID
		and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and DELETED_FLG = 'N'
	where s.CLIENT_ID is null )
and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
and EFFECTIVE_FROM < sysdate""")

#TERMINALS
curs.execute( """insert into ITDE1.SVET_DWH_DIM_TERMINALS_HIST (TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
select
    tbl.TERMINAL_ID,
    tbl.TERMINAL_TYPE,
    tbl.TERMINAL_CITY,
    tbl.TERMINAL_ADDRESS,
	to_date(tbl.EFFECTIVE_TO) + interval '1' second,
	to_date( '2999-12-31', 'YYYY-MM-DD' ),
	'Y'
from
    (select t.*
     from ITDE1.SVET_DWH_DIM_TERMINALS_HIST t
    left join ITDE1.SVET_STG_DEL_TERMINALS s
    on t.TERMINAL_ID = s.TERMINAL_ID
        and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
        and DELETED_FLG = 'N'
    where s.TERMINAL_ID is null ) tbl""")

curs.execute( """update ITDE1.SVET_DWH_DIM_TERMINALS_HIST
set effective_to = sysdate - interval '1' second
where TERMINAL_ID in (
	select t.TERMINAL_ID
	from ITDE1.SVET_DWH_DIM_TERMINALS_HIST t
	left join ITDE1.SVET_STG_DEL_TERMINALS s
	on t.TERMINAL_ID = s.TERMINAL_ID
		and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
		and DELETED_FLG = 'N'
	where s.TERMINAL_ID is null )
and EFFECTIVE_TO = to_date( '2999-12-31', 'YYYY-MM-DD' )
and EFFECTIVE_FROM < sysdate""")

#-- 6. Обновляем метаданные - дату максимальной загрузуки

curs.execute( """UPDATE ITDE1.SVET_META_LOADING
SET LAST_UPDATE = ( SELECT MAX( UPDATE_DT, CREATE_DT ) FROM ITDE1.SVET_STG_ACCOUNTS )
WHERE 1=1
	AND DBNAME = 'ITDE1'
	AND TABLENAME = 'SVET_DWH_DIM_ACCOUNTS_HIST'
	AND ( SELECT MAX( UPDATE_DT, CREATE_DT ) FROM ITDE1.SVET_STG_ACCOUNTS ) IS NOT NULL""")

curs.execute( """
UPDATE ITDE1.SVET_META_LOADING
SET LAST_UPDATE = ( SELECT MAX( COALESCE( UPDATE_DT, CREATE_DT ) ) FROM ITDE1.SVET_STG_CARDS )
WHERE 1=1
	AND DBNAME = 'ITDE1'
	AND TABLENAME = 'SVET_DWH_DIM_CARDS_HIST'
	AND ( SELECT MAX( COALESCE( UPDATE_DT, CREATE_DT ) ) FROM ITDE1.SVET_STG_CARDS ) IS NOT NULL""")

curs.execute( """UPDATE ITDE1.SVET_META_LOADING
SET LAST_UPDATE = ( SELECT MAX( COALESCE( UPDATE_DT, CREATE_DT ) ) FROM ITDE1.SVET_STG_CLIENTS )
WHERE 1=1
	AND DBNAME = 'ITDE1'
	AND TABLENAME = 'SVET_DWH_DIM_CLIENTS_HIST'
	AND ( SELECT MAX( COALESCE( UPDATE_DT, CREATE_DT ) ) FROM ITDE1.SVET_STG_CLIENTS ) IS NOT NULL""")

curs.execute( """UPDATE ITDE1.SVET_META_LOADING
SET LAST_UPDATE = ( SELECT MAX( ENTRY_DT ) FROM ITDE1.SVET_STG_PASSPORT_BLACKLIST )
WHERE 1=1
	AND DBNAME = 'ITDE1'
	AND TABLENAME = 'SVET_DWH_FACT_PSSPRT_BLCKLST'
	AND ( SELECT MAX(  ENTRY_DT ) FROM ITDE1.SVET_STG_PASSPORT_BLACKLIST ) IS NOT NULL""")

curs.execute( """UPDATE ITDE1.SVET_META_LOADING
SET LAST_UPDATE = ( SELECT MAX( TRANSACTION_DATE ) FROM ITDE1.SVET_STG_TRANSACTIONS )
WHERE 1=1
	AND DBNAME = 'ITDE1'
	AND TABLENAME = 'SVET_DWH_FACT_TRANSACTIONS'
	AND ( SELECT MAX(  TRANSACTION_DATE ) FROM ITDE1.SVET_STG_TRANSACTIONS ) IS NOT NULL""")

#-- 7. Фиксируется транзакция
curs.execute( "COMMIT")

curs.close()
conn.close()