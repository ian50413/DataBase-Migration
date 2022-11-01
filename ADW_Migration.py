# -*- coding: utf-8 -*-
"""
Created on Tue Mar 15 09:20:42 2022

@author: Ian.Chen
"""

import pandas as pd
import cx_Oracle
import time
from datetime import timedelta
from datetime import datetime as dt
import datetime

#connect to sqlserver
import pyodbc as odbc
import sys


#create empty dict for daily dataframe
sic_dict={}

#DEV
#get oracle
#conn_oracle = cx_Oracle.connect("sic/aa1234@twtpedb01u:1522/sicdev")

#sqlserver info
#DRIVER ='SQL Server'
#SERVER_NAME = 'TWTPEWDPDB01D'
#DATABASE_NAME = 'master'

#PRD
#get oracle
conn_oracle = cx_Oracle.connect("sic/dd0987^%$#@10.36.28.18:1521/SIC")

#sqlserver info (PROD)
DRIVER ='SQL Server'
SERVER_NAME = 'TWTPEWADWCI01P.hq.intra.acer.com,50962'
DATABASE_NAME = 'DMSCM'
UID = 'SCM_OP'
PWD = '454zdq$X'

#get sqlserver
#conn_string = f"""
#Driver={{{DRIVER}}};
#Server={SERVER_NAME};
#Database={DATABASE_NAME};
#Trust_Connection=yes;
#"""

conn_string = f"""
Driver={{{DRIVER}}};
Server={SERVER_NAME};
Database={DATABASE_NAME};
UID={UID};
PWD={PWD};
"""

conn_sqlserver = odbc.connect(conn_string)
print(conn_sqlserver)
cursor_sqlserver = conn_sqlserver.cursor()

#Bamboo改寫Start from here
partition = pd.read_csv('partition_desc.txt', header = None)

now = dt.now()
print(now)
try:
    for i in partition[0]:
        #以上需改寫透過迴圈讀取partition抓出某n年單月的資料，以下可單獨執行測試讀取&塞入
        #(目前讀取位置為oracle-SIC.SERIAL_NUMBER_LEVEL2)
            #抓取每年某一月partiont，並取得起始和截止日
        print (f"partition:{i}")
        max_min=f"SELECT MAX (REGISTER_DATE) AS max_d, MIN (REGISTER_DATE) AS min_d FROM SIC.SERIAL_NUMBER_LEVEL2 PARTITION ( {i} )" #add partiont(A) here
        max_min_date=pd.read_sql(max_min, conn_oracle)
        if len(max_min_date) > 0 :
            #set to 0 a.m
            max_min_date['max'] = pd.to_datetime(max_min_date['MAX_D']).dt.floor('D')
            max_min_date['min'] = pd.to_datetime(max_min_date['MIN_D']).dt.floor('D')
            partiont_start_date = max_min_date['min'].dt.strftime('%Y-%m-%d')
            partiont_start_date = partiont_start_date.iloc[0]
            partiont_start_date_ts = datetime.datetime.strptime(f"{partiont_start_date}", "%Y-%m-%d")
            #ttl 31 day - start day for iteration
            partiont_start_day = max_min_date['min'].dt.strftime('%d')
            partiont_start_day = int(partiont_start_day.iloc[0])
            for j in range(1,32-partiont_start_day+1,1):
                start_partiont = time.time()
                #partiont_start_date = '2021/12/02'
                print(f"Begin process {partiont_start_date}")
                for k in range(0,23,1):
                    print(f"get data...{partiont_start_date}-{k}")
                    partiont_d=(f"""
                            SELECT * 
                            FROM SIC.SERIAL_NUMBER_LEVEL2 PARTITION ( {i} )
                            WHERE TRUNC(REGISTER_DATE, 'HH24') = TO_DATE('{partiont_start_date} {k}', 'YYYY/MM/DD HH24')
                            ORDER BY REGISTER_DATE ASC
                            """)
                    sic_dict[f"{partiont_start_date}"]=pd.read_sql(partiont_d, conn_oracle)
                    data_count = len(sic_dict[f"{partiont_start_date}"])
                    now = dt.now()
                    print(now)
                    print(f"{partiont_start_date}-{k} data count:{data_count}")
                    if data_count > 0 :
                        print("parepare insert data...")

                        parameters = []
                        for index, row in sic_dict[f"{partiont_start_date}"].iterrows():
                            # get current time stamp
                            lsmd = now.strftime("%Y-%m-%d %H:%M:%S")
                            tmp_array = [row.PROCESS_ID, row.SERIAL_NUMBER, row.KC_SERIAL_NUMBER, row.KC_SEQUENCE_NUMBER, 
                                         row.KC_PRODUCT_NUMBER, row.KC_AI_PRODUCT_NUMBER, row.KC_PRODUCT_DESCRIPTION, row.KC_PRODUCT_TYPE, 
                                         row.ORG_KC_SERIAL_NUMBER, row.ORG_KC_PRODUCT_NUMBER, row.WARRANTY_TYPE, row.WARRANTY_DATE, row.REGISTER_DATE, 
                                         row.FOB_FLAG, row.PROCESS_STATUS, row.PROCESS_DATE, row.REGION_CODE, row.BRAND, row.KC_SN_HASH, 
                                         row.ODM_KC_PRODUCT_NUMBER, row.ODM_KC_SERIAL_NUMBER, row.DATA_SOURCE, row.ODM_FG_PN, lsmd, lsmd]
                            parameters.append(tmp_array);

                        now = dt.now()
                        print(now)
                        print("insert data to temp...")
                        cursor_sqlserver.fast_executemany = True
                        cursor_sqlserver.executemany(f"""
                                                     INSERT INTO DW.SIC.FB_SERIAL_NUMBER_LEVEL2_T 
                                                     (
                                                      PROCESS_ID, SERIAL_NUMBER, KC_SERIAL_NUMBER, KC_SEQUENCE_NUMBER, KC_PRODUCT_NUMBER, KC_AI_PRODUCT_NUMBER, 
                                                      KC_PRODUCT_DESCRIPTION ,KC_PRODUCT_TYPE, ORG_KC_SERIAL_NUMBER, ORG_KC_PRODUCT_NUMBER, WARRANTY_TYPE, WARRANTY_DATE, 
                                                      REGISTER_DATE, FOB_FLAG, PROCESS_STATUS, PROCESS_DATE, REGION_CODE, BRAND, KC_SN_HASH, ODM_KC_PRODUCT_NUMBER, 
                                                      ODM_KC_SERIAL_NUMBER, DATA_SOURCE, ODM_FG_PN, LAST_MAINTAIN_DATE, LAST_SYSTEM_MAINTAIN_DATE
                                                      ) 
                                                     VALUES 
                                                     (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                     """, 
                                                     parameters
                                                     )

                        #Merge DW.SIC.FB_SERIAL_NUMBER_LEVEL2_T with DW.SIC.FB_SERIAL_NUMBER_LEVEL2
                        now = dt.now()
                        print(now)
                        print("Merge data from temp...")
                        sqlserver_merge_query=(f"""
                                               MERGE DW.SIC.FB_SERIAL_NUMBER_LEVEL2 t
                                               USING DW.SIC.FB_SERIAL_NUMBER_LEVEL2_T s
                                               ON (s.SERIAL_NUMBER = t.SERIAL_NUMBER
                                               AND s.KC_SERIAL_NUMBER = t.KC_SERIAL_NUMBER
                                               AND s.KC_SEQUENCE_NUMBER = t.KC_SEQUENCE_NUMBER)
                                               WHEN MATCHED
                                               THEN UPDATE SET
                                                   t.KC_PRODUCT_NUMBER = s.KC_PRODUCT_NUMBER,
                                                   t.KC_AI_PRODUCT_NUMBER = s.KC_AI_PRODUCT_NUMBER,
                                                   t.KC_PRODUCT_DESCRIPTION = s.KC_PRODUCT_DESCRIPTION,
                                                   t.KC_PRODUCT_TYPE = s.KC_PRODUCT_TYPE,
                                                   t.ORG_KC_SERIAL_NUMBER = s.ORG_KC_SERIAL_NUMBER,
                                                   t.ORG_KC_PRODUCT_NUMBER = s.ORG_KC_PRODUCT_NUMBER,
                                                   t.WARRANTY_TYPE = s.WARRANTY_TYPE,
                                                   t.WARRANTY_DATE = s.WARRANTY_DATE,
                                                   t.REGISTER_DATE = s.REGISTER_DATE,
                                                   t.FOB_FLAG = s.FOB_FLAG,
                                                   t.PROCESS_STATUS = s.PROCESS_STATUS,
                                                   t.PROCESS_DATE = s.PROCESS_DATE,
                                                   t.REGION_CODE = s.REGION_CODE,
                                                   t.BRAND = s.BRAND,
                                                   t.KC_SN_HASH = s.KC_SN_HASH,
                                                   t.ODM_KC_PRODUCT_NUMBER = s.ODM_KC_PRODUCT_NUMBER,
                                                   t.ODM_KC_SERIAL_NUMBER = s.ODM_KC_SERIAL_NUMBER,
                                                   t.DATA_SOURCE = s.DATA_SOURCE,
                                                   t.ODM_FG_PN = s.ODM_FG_PN,
                                                   t.LAST_MAINTAIN_DATE = s.LAST_MAINTAIN_DATE,
                                                   t.LAST_SYSTEM_MAINTAIN_DATE = s.LAST_SYSTEM_MAINTAIN_DATE
                                               WHEN NOT MATCHED
                                               THEN INSERT (PROCESS_ID, SERIAL_NUMBER, KC_SERIAL_NUMBER, KC_SEQUENCE_NUMBER, KC_PRODUCT_NUMBER, 
                                               KC_AI_PRODUCT_NUMBER, KC_PRODUCT_DESCRIPTION, KC_PRODUCT_TYPE, ORG_KC_SERIAL_NUMBER, ORG_KC_PRODUCT_NUMBER, 
                                               WARRANTY_TYPE, WARRANTY_DATE, REGISTER_DATE, FOB_FLAG, PROCESS_STATUS, PROCESS_DATE, REGION_CODE, BRAND, KC_SN_HASH, 
                                               ODM_KC_PRODUCT_NUMBER, ODM_KC_SERIAL_NUMBER, DATA_SOURCE, ODM_FG_PN, LAST_MAINTAIN_DATE, LAST_SYSTEM_MAINTAIN_DATE) 
                                               VALUES (s.PROCESS_ID, s.SERIAL_NUMBER, s.KC_SERIAL_NUMBER, s.KC_SEQUENCE_NUMBER, s.KC_PRODUCT_NUMBER, s.KC_AI_PRODUCT_NUMBER, 
                                               s.KC_PRODUCT_DESCRIPTION, s.KC_PRODUCT_TYPE, s.ORG_KC_SERIAL_NUMBER, s.ORG_KC_PRODUCT_NUMBER, s.WARRANTY_TYPE, s.WARRANTY_DATE,
                                               s.REGISTER_DATE, s.FOB_FLAG, s.PROCESS_STATUS, s.PROCESS_DATE, s.REGION_CODE, s.BRAND, s.KC_SN_HASH, s.ODM_KC_PRODUCT_NUMBER,
                                               s.ODM_KC_SERIAL_NUMBER, s.DATA_SOURCE, s.ODM_FG_PN, s.LAST_MAINTAIN_DATE, s.LAST_SYSTEM_MAINTAIN_DATE)
                                               ;
                                               """)
                        cursor_sqlserver.execute(sqlserver_merge_query)
                        cursor_sqlserver.commit()
                        # #When Merge or Insert is Done delete DW.SIC.FB_SERIAL_NUMBER_LEVEL2_T
                        print("clean temp...")
                        sqlserver_delete_query=(f"""
                                               DELETE FROM DW.SIC.FB_SERIAL_NUMBER_LEVEL2_T
                                               """)
                        cursor_sqlserver.execute(sqlserver_delete_query)
                        cursor_sqlserver.commit()

                end_partiont = time.time()
                elapsed = end_partiont - start_partiont
                print(f"Done {partiont_start_date} select & merge & delete in "+str(timedelta(seconds=elapsed))+'\n')

                partiont_next_date_ts = partiont_start_date_ts + datetime.timedelta(days=j)
                partiont_start_date = partiont_next_date_ts.strftime('%Y-%m-%d')
except Exception as inst:
    print("An exception occurred")
    print(type(inst))    # the exception instance
    print(inst.args)     # arguments stored in .args
    print(inst) 
finally:
    conn_oracle.close()
    conn_sqlserver.close()
    now = dt.now()
    print(now)