import oracledb
from datetime import datetime

# --------------------------------------------
# Parametri connessione
# --------------------------------------------
user_stage = "STAGE_SPARK"
pwd_stage = "STAGE_SPARK"
dsn_stage = "localhost:1521/orcl"

user_dwh = "DWH_SPARK"
pwd_dwh = "DWH_SPARK"
dsn_dwh = "localhost:1521/orcl"

# --------------------------------------------
# Connessioni
# --------------------------------------------
conn_stage = oracledb.connect(user=user_stage, password=pwd_stage, dsn=dsn_stage)
conn_dwh = oracledb.connect(user=user_dwh, password=pwd_dwh, dsn=dsn_dwh)
cur_stage = conn_stage.cursor()
cur_dwh = conn_dwh.cursor()

# --------------------------------------------
# Recupero ID_PERIOD più recente dal stage
# --------------------------------------------
cur_stage.execute("SELECT MAX(ID_PER) FROM STAGE_SPARK.GRP02_PRS")
id_period = cur_stage.fetchone()[0]
print(f"Ultimo ID_PERIOD: {id_period}")

# --------------------------------------------
# Recupero ruoli dal stage
# --------------------------------------------
cur_stage.execute(f"""
    SELECT DISTINCT COD_RUOLO, DESC_RUOLO
    FROM STAGE_SPARK.GRP02_RUO
    WHERE ID_PER = {id_period}
""")
ruoli = cur_stage.fetchall()  # lista di tuple (COD_RUOLO, DESC_RUOLO)

# --------------------------------------------
# Inserimento / aggiornamento ruoli nel DWH
# --------------------------------------------
for cod_ruolo, desc_ruolo in ruoli:
    cur_dwh.execute(f"""
        MERGE INTO DWH_SPARK.DIM_RUOLO d
        USING (SELECT '{cod_ruolo}' AS COD_RUOLO FROM dual) src
        ON (d.COD_RUOLO = src.COD_RUOLO)
        WHEN MATCHED THEN UPDATE SET
            d.DESCR_RUOLO = '{desc_ruolo}',
            d.D_UPD = SYSDATE,
            d.LAST_ID_PER = {id_period}
        WHEN NOT MATCHED THEN INSERT (ID_RUOLO, COD_RUOLO, DESCR_RUOLO, D_INS, D_UPD, LAST_ID_PER)
        VALUES (DWH_SPARK.SEQ_DIM_RUOLO.NEXTVAL, '{cod_ruolo}', '{desc_ruolo}', SYSDATE, SYSDATE, {id_period})
    """)

# Commit finale
conn_dwh.commit()
print("DIM_RUOLO aggiornata correttamente.")

# --------------------------------------------
# Recupero unità dal stage
# --------------------------------------------
cur_stage.execute(f"""
    SELECT DISTINCT COD_UNT, DESC_UNT
    FROM STAGE_SPARK.GRP02_UNT
    WHERE ID_PER = {id_period}
""")
unita = cur_stage.fetchall()  # lista di tuple (COD_UNITA, DESC_UNITA)

# --------------------------------------------
# Inserimento / aggiornamento unità nel DWH
# --------------------------------------------
for cod_unita, desc_unita in unita:
    cur_dwh.execute(f"""
        MERGE INTO DWH_SPARK.DIM_UNITA d
        USING (SELECT '{cod_unita}' AS COD_UNITA FROM dual) src
        ON (d.COD_UNITA = src.COD_UNITA)
        WHEN MATCHED THEN UPDATE SET
            d.DESC_UNITA = '{desc_unita}',
            d.D_UPD = SYSDATE,
            d.LAST_ID_PER = {id_period}
        WHEN NOT MATCHED THEN INSERT (ID_UNT, COD_UNITA, DESC_UNITA, D_INS, D_UPD, LAST_ID_PER)
        VALUES (DWH_SPARK.SEQ_DIM_UNITA.NEXTVAL, '{cod_unita}', '{desc_unita}', SYSDATE, SYSDATE, {id_period})
    """)

conn_dwh.commit()
print("DIM_UNITA aggiornata correttamente.")

# --------------------------------------------
# DIM_ANAGRAFICA (SCD su COD_UNT e COD_RUOLO)
# --------------------------------------------
cur_stage = conn_stage.cursor()
cur_dwh = conn_dwh.cursor()

# Recupero ultimo ID_PERIOD disponibile nello stage
cur_stage.execute("SELECT MAX(ID_PER) FROM STAGE_SPARK.GRP02_ANA")
id_period = cur_stage.fetchone()[0]
print(f"Ultimo ID_PER: {id_period}")

# Recupero tutti i record dello stage relativi all'ultimo periodo
cur_stage.execute("""
                  SELECT *
                  FROM STAGE_SPARK.GRP02_ANA
                  WHERE ID_PER = :id_period
                  """, id_period=id_period)

rows = cur_stage.fetchall()
col_names = [desc[0] for desc in cur_stage.description]

oggi = datetime.now()

for r in rows:
    row = dict(zip(col_names, r))

    # Aggiungo le colonne mancanti
    row['D_INS'] = oggi
    row['D_UPD'] = oggi
    row['D_INIVAL'] = row['ID_PER']
    row['D_ENDVAL'] = 999999
    row['LAST_ID_PER'] = row['ID_PER']

    # Verifica se esiste record corrente (D_ENDVAL = 999999)
    cur_dwh.execute("""
                    SELECT COD_UNT, COD_RUOLO
                    FROM DWH_SPARK.D_ANAGRAFICA_SCD
                    WHERE ID_DIP = :COD_DIP
                      AND D_ENDVAL = 999999
                    """, COD_DIP=row['COD_DIP'])

    res = cur_dwh.fetchone()

    if res:
        cod_unt_old, cod_ruolo_old = res
        if cod_unt_old != row['COD_UNT'] or cod_ruolo_old != row['COD_RUOLO']:
            # Chiudo il vecchio record
            cur_dwh.execute("""
                            UPDATE DWH_SPARK.D_ANAGRAFICA_SCD
                            SET D_ENDVAL = :D_INIVAL - 1,
                                D_UPD    = :D_UPD
                            WHERE ID_DIP = :COD_DIP
                              AND D_ENDVAL = 999999
                            """, D_INIVAL=row['ID_PER'], D_UPD=oggi, COD_DIP=row['COD_DIP'])

            # Inserimento nuovo record
            cur_dwh.execute("""
                            INSERT INTO DWH_SPARK.D_ANAGRAFICA_SCD
                            (ID_DIP, COD_DIP, COD_UNT, COD_RUOLO, COGNOME, NOME, DT_INI_UNT,
                             TIPO_CTR, COD_CTR, COD_LIV, DT_INI_LIV, DT_INI_MNS, MATRICOLA,
                             CF, SESSO, DT_NASCITA, TIPO_RAPP, DT_INI_TIPO_CTR, DT_FINE_TIPO_CTR,
                             DT_INI_RAPP, DT_FINE_RAPP, AMBITO, TIPO_ASS, DT_PROROGA_TERMINE,
                             DT_CTR_SOST_MAT, QTA_PART_TIME, TITOLARE1, TITOLARE2, MOTIVO_PROMOZIONE,
                             FASCICOLO_AGGIORNATO, TITOLO_STUDIO, DESC_CTR, DESC_LIV,
                             D_INS, D_UPD, D_INIVAL, D_ENDVAL, LAST_ID_PER)
                            VALUES (:COD_DIP, :COD_DIP, :COD_UNT, :COD_RUOLO, :COGNOME, :NOME, :DT_INI_UNT,
                                    :TIPO_CTR, :COD_CTR, :COD_LIV, :DT_INI_LIV, :DT_INI_MNS, :MATRICOLA,
                                    :CF, :SESSO, :DT_NASCITA, :TIPO_RAPP, :DT_INI_TIPO_CTR, :DT_FINE_TIPO_CTR,
                                    :DT_INI_RAPP, :DT_FINE_RAPP, :AMBITO, :TIPO_ASS, :DT_PROROGA_TERMINE,
                                    :DT_CTR_SOST_MAT, :QTA_PART_TIME, :TITOLARE1, :TITOLARE2, :MOTIVO_PROMOZIONE,
                                    :FASCICOLO_AGGIORNATO, :TITOLO_STUDIO, :DESC_CTR, :DESC_LIV,
                                    :D_INS, :D_UPD, :D_INIVAL, :D_ENDVAL, :LAST_ID_PER)
                            """, row)
        else:
            # Aggiorno solo le altre informazioni
            cur_dwh.execute("""
                            UPDATE DWH_SPARK.D_ANAGRAFICA_SCD
                            SET COGNOME=:COGNOME,
                                NOME=:NOME,
                                DT_INI_UNT=:DT_INI_UNT,
                                TIPO_CTR=:TIPO_CTR,
                                COD_CTR=:COD_CTR,
                                COD_LIV=:COD_LIV,
                                DT_INI_LIV=:DT_INI_LIV,
                                DT_INI_MNS=:DT_INI_MNS,
                                MATRICOLA=:MATRICOLA,
                                CF=:CF,
                                SESSO=:SESSO,
                                DT_NASCITA=:DT_NASCITA,
                                TIPO_RAPP=:TIPO_RAPP,
                                DT_INI_TIPO_CTR=:DT_INI_TIPO_CTR,
                                DT_FINE_TIPO_CTR=:DT_FINE_TIPO_CTR,
                                DT_INI_RAPP=:DT_INI_RAPP,
                                DT_FINE_RAPP=:DT_FINE_RAPP,
                                AMBITO=:AMBITO,
                                TIPO_ASS=:TIPO_ASS,
                                DT_PROROGA_TERMINE=:DT_PROROGA_TERMINE,
                                DT_CTR_SOST_MAT=:DT_CTR_SOST_MAT,
                                QTA_PART_TIME=:QTA_PART_TIME,
                                TITOLARE1=:TITOLARE1,
                                TITOLARE2=:TITOLARE2,
                                MOTIVO_PROMOZIONE=:MOTIVO_PROMOZIONE,
                                FASCICOLO_AGGIORNATO=:FASCICOLO_AGGIORNATO,
                                TITOLO_STUDIO=:TITOLO_STUDIO,
                                DESC_CTR=:DESC_CTR,
                                DESC_LIV=:DESC_LIV,
                                D_UPD=:D_UPD,
                                LAST_ID_PER=:LAST_ID_PER
                            WHERE ID_DIP = :COD_DIP
                              AND D_ENDVAL = 999999
                            """, row)
    else:
        # Inserimento nuovo record se non esiste
        cur_dwh.execute("""
                        INSERT INTO DWH_SPARK.D_ANAGRAFICA_SCD
                        (ID_DIP, COD_DIP, COD_UNT, COD_RUOLO, COGNOME, NOME, DT_INI_UNT,
                         TIPO_CTR, COD_CTR, COD_LIV, DT_INI_LIV, DT_INI_MNS, MATRICOLA,
                         CF, SESSO, DT_NASCITA, TIPO_RAPP, DT_INI_TIPO_CTR, DT_FINE_TIPO_CTR,
                         DT_INI_RAPP, DT_FINE_RAPP, AMBITO, TIPO_ASS, DT_PROROGA_TERMINE,
                         DT_CTR_SOST_MAT, QTA_PART_TIME, TITOLARE1, TITOLARE2, MOTIVO_PROMOZIONE,
                         FASCICOLO_AGGIORNATO, TITOLO_STUDIO, DESC_CTR, DESC_LIV,
                         D_INS, D_UPD, D_INIVAL, D_ENDVAL, LAST_ID_PER)
                        VALUES (:COD_DIP, :COD_DIP, :COD_UNT, :COD_RUOLO, :COGNOME, :NOME, :DT_INI_UNT,
                                :TIPO_CTR, :COD_CTR, :COD_LIV, :DT_INI_LIV, :DT_INI_MNS, :MATRICOLA,
                                :CF, :SESSO, :DT_NASCITA, :TIPO_RAPP, :DT_INI_TIPO_CTR, :DT_FINE_TIPO_CTR,
                                :DT_INI_RAPP, :DT_FINE_RAPP, :AMBITO, :TIPO_ASS, :DT_PROROGA_TERMINE,
                                :DT_CTR_SOST_MAT, :QTA_PART_TIME, :TITOLARE1, :TITOLARE2, :MOTIVO_PROMOZIONE,
                                :FASCICOLO_AGGIORNATO, :TITOLO_STUDIO, :DESC_CTR, :DESC_LIV,
                                :D_INS, :D_UPD, :D_INIVAL, :D_ENDVAL, :LAST_ID_PER)
                        """, row)

conn_dwh.commit()
print("DIM_ANAGRAFICA aggiornata correttamente.")

