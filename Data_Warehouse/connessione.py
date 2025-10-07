import oracledb

def get_connections():
    conn_stage = oracledb.connect(user="STAGE_SPARK", password="STAGE_SPARK", dsn="localhost:1521/orcl")
    conn_dwh = oracledb.connect(user="DWH_SPARK", password="DWH_SPARK", dsn="localhost:1521/orcl")
    return conn_stage, conn_dwh, conn_stage.cursor(), conn_dwh.cursor()
