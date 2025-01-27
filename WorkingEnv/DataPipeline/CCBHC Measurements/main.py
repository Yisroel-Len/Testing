import logging
from typing import List
from sqlalchemy import create_engine
from DEP_REM import *
from ASC import *
from CDF_AD import *
from CDF_CH import *
from I_SERV import *
from SDOH import *

# set up logger for all measures
log_path = r"../Logs/clinical_log.log"
logging.basicConfig(filename=log_path,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d,%(name)s,%(levelname)s,%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)


# set up enginge for all measures
conn:Engine
conn = create_engine(r'mssql+pyodbc://@PYTHONSERVER\SQLEXPRESS/InSync?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes', fast_executemany=True)


# set up measurment list
measure_list:List[Measurement]
measure_list = [ASC(conn,logging.getLogger("ASC")),
                CDF_AD(conn,logging.getLogger("CDF AD")),
                CDF_CH(conn,logging.getLogger("CDF CH")),
                DEP_REM(conn,logging.getLogger("DEP REM")),
                I_Serv(conn,logging.getLogger("I Serv")),
                SDOH(conn,logging.getLogger("SDOH"))
                ]

# calculate all measures
for measurement in measure_list:
    try:
        sub_data = measurement.get_submeasure_data()
        for key, value in sub_data.items():
            # Iserv sub 2 is a subset of sub 1, so the DB only needs the sub1 stratify and sub2 can referance sub1's stratify
            if key == "ISERV_sub_1_stratify": 
                value.to_sql('pt'+key+'_and_2_strtatify',conn,if_exists='replace',index=False)
                continue
            elif key == "ISERV_sub_2_stratify":
                continue
            value.to_sql('pt'+key,conn,if_exists='replace',index=False)
            logging.getLogger(measurement.get_name()).info(f"Successfully pushed {key} to SQL")
    except Exception as e:
        print("OH NO! WE'VE GOT ISSUES")
        print(e)
        pass
