import oracledb
from datetime import datetime, timedelta
import locale
import unidecode

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
cur_stage.execute("SELECT MAX(ID_PER) FROM STAGE_SPARK.GRP02_RUO")
id_period = cur_stage.fetchone()[0]
print(f"Ultimo ID_PERIOD: {id_period}")

# --------------------------------------------
# Recupero RUOLI dal stage
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
# Recupero UNITA dal stage
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

# ANAGRAFICA

# --------------------------------------------
# Recupero ID_PERIOD più recente dal stage
# --------------------------------------------
cur_stage.execute("SELECT MAX(ID_PER) FROM STAGE_SPARK.GRP02_ANA")
id_period = cur_stage.fetchone()[0]
print(f"Ultimo ID_PER: {id_period}")

# --------------------------------------------
# Estrazione record corrispondenti dallo staging
# --------------------------------------------
sql_stage = """
SELECT COD_DIP, COD_UNT, COD_RUOLO, COGNOME, NOME, DT_INI_UNT,
       TIPO_CTR, COD_CTR, COD_LIV, DT_INI_LIV, DT_INI_MNS,
       MATRICOLA, CF, SESSO, DT_NASCITA, TIPO_RAPP, DT_INI_TIPO_CTR,
       DT_FINE_TIPO_CTR, DT_INI_RAPP, DT_FINE_RAPP, AMBITO,
       TIPO_ASS, DT_PROROGA_TERMINE, DT_CTR_SOST_MAT, QTA_PART_TIME,
       TITOLARE1, TITOLARE2, MOTIVO_PROMOZIONE, FASCICOLO_AGGIORNATO,
       TITOLO_STUDIO, DESC_CTR, DESC_LIV
FROM STAGE_SPARK.GRP02_ANA
WHERE ID_PER = :id_per
"""
cur_stage.execute(sql_stage, id_per=id_period)
rows = cur_stage.fetchall()

print(f"Record trovati nello staging: {len(rows)}")

# --------------------------------------------
# Generatore chiave surrogata (fallback se non esiste una sequenza)
# --------------------------------------------
def get_next_id_dip():
    cur_dwh.execute("SELECT NVL(MAX(ID_DIP),0) + 1 FROM DWH_SPARK.D_ANAGRAFICA_SCD")
    return cur_dwh.fetchone()[0]

# --------------------------------------------
# Loop sui record e gestione logica SCD
# --------------------------------------------
for r in rows:
    cod_dip, cod_unt, cod_ruolo = r[0], r[1], r[2]

    # Verifica se il dipendente esiste già con D_ENDVAL NULL
    cur_dwh.execute("""
        SELECT ID_DIP, COD_UNT, COD_RUOLO
        FROM DWH_SPARK.D_ANAGRAFICA_SCD
        WHERE COD_DIP = :cod_dip AND D_ENDVAL IS NULL
    """, cod_dip=cod_dip)
    existing = cur_dwh.fetchone()

    if existing:
        v_id_dip, v_cod_unt, v_cod_ruolo = existing

        # Se cambia unità o ruolo → storicizza
        if v_cod_unt != cod_unt or v_cod_ruolo != cod_ruolo:
            # Chiude il vecchio record
            cur_dwh.execute("""
                UPDATE DWH_SPARK.D_ANAGRAFICA_SCD
                SET D_ENDVAL = TO_NUMBER(TO_CHAR(ADD_MONTHS(TO_DATE(:id_per,'YYYYMM'),-1),'YYYYMM')),
                    D_UPD = SYSDATE
                WHERE ID_DIP = :id_dip
            """, id_per=id_period, id_dip=v_id_dip)

            # Inserisce un nuovo record
            new_id = get_next_id_dip()
            cur_dwh.execute("""
                INSERT INTO DWH_SPARK.D_ANAGRAFICA_SCD (
                    ID_DIP, COD_DIP, COD_UNT, COD_RUOLO, COGNOME, NOME, DT_INI_UNT,
                    TIPO_CTR, COD_CTR, COD_LIV, DT_INI_LIV, DT_INI_MNS,
                    MATRICOLA, CF, SESSO, DT_NASCITA, TIPO_RAPP, DT_INI_TIPO_CTR,
                    DT_FINE_TIPO_CTR, DT_INI_RAPP, DT_FINE_RAPP, AMBITO,
                    TIPO_ASS, DT_PROROGA_TERMINE, DT_CTR_SOST_MAT, QTA_PART_TIME,
                    TITOLARE1, TITOLARE2, MOTIVO_PROMOZIONE, FASCICOLO_AGGIORNATO,
                    TITOLO_STUDIO, DESC_LIV,
                    D_INIVAL, D_ENDVAL, D_INS, D_UPD, LAST_ID_PER
                ) VALUES (
                    :id_dip, :cod_dip, :cod_unt, :cod_ruolo, :cognome, :nome, :dt_ini_unt,
                    :tipo_ctr, :cod_ctr, :cod_liv, :dt_ini_liv, :dt_ini_mns,
                    :matricola, :cf, :sesso, :dt_nascita, :tipo_rapp, :dt_ini_tipo_ctr,
                    :dt_fine_tipo_ctr, :dt_ini_rapp, :dt_fine_rapp, :ambito,
                    :tipo_ass, :dt_proroga_termine, :dt_ctr_sost_mat, :qta_part_time,
                    :titolare1, :titolare2, :motivo_promozione, :fascicolo_aggiornato,
                    :titolo_studio, :desc_liv,
                    :d_inival, NULL, SYSDATE, NULL, :last_id_per
                )
            """, {
                "id_dip": new_id,
                "cod_dip": r[0],
                "cod_unt": r[1],
                "cod_ruolo": r[2],
                "cognome": r[3],
                "nome": r[4],
                "dt_ini_unt": r[5],
                "tipo_ctr": r[6],
                "cod_ctr": r[7],
                "cod_liv": r[8],
                "dt_ini_liv": r[9],
                "dt_ini_mns": r[10],
                "matricola": r[11],
                "cf": r[12],
                "sesso": r[13],
                "dt_nascita": r[14],
                "tipo_rapp": r[15],
                "dt_ini_tipo_ctr": r[16],
                "dt_fine_tipo_ctr": r[17],
                "dt_ini_rapp": r[18],
                "dt_fine_rapp": r[19],
                "ambito": r[20],
                "tipo_ass": r[21],
                "dt_proroga_termine": r[22],
                "dt_ctr_sost_mat": r[23],
                "qta_part_time": r[24],
                "titolare1": r[25],
                "titolare2": r[26],
                "motivo_promozione": r[27],
                "fascicolo_aggiornato": r[28],
                "titolo_studio": r[29],
                "desc_liv": r[31],  # attenzione: salto DESC_CTR
                "d_inival": id_period,
                "last_id_per": id_period
            })

        else:
            # Nessun cambio → aggiorna campi base e last_id_per
            cur_dwh.execute("""
                UPDATE DWH_SPARK.D_ANAGRAFICA_SCD
                SET TIPO_CTR = :tipo_ctr,
                    COD_CTR = :cod_ctr,
                    COD_LIV = :cod_liv,
                    DT_INI_LIV = :dt_ini_liv,
                    DT_INI_MNS = :dt_ini_mns,
                    TIPO_RAPP = :tipo_rapp,
                    DT_INI_TIPO_CTR = :dt_ini_tipo_ctr,
                    DT_FINE_TIPO_CTR = :dt_fine_tipo_ctr,
                    DT_INI_RAPP = :dt_ini_rapp,
                    DT_FINE_RAPP = :dt_fine_rapp,
                    AMBITO = :ambito,
                    TIPO_ASS = :tipo_ass,
                    DT_PROROGA_TERMINE = :dt_proroga_termine,
                    DT_CTR_SOST_MAT = :dt_ctr_sost_mat,
                    QTA_PART_TIME = :qta_part_time,
                    TITOLARE1 = :titolare1,
                    TITOLARE2 = :titolare2,
                    MOTIVO_PROMOZIONE = :motivo_promozione,
                    FASCICOLO_AGGIORNATO = :fascicolo_aggiornato,
                    TITOLO_STUDIO = :titolo_studio,
                    DESC_LIV = :desc_liv,
                    D_UPD = SYSDATE,
                    LAST_ID_PER = :last_id_per
                WHERE ID_DIP = :id_dip
            """, {
                "tipo_ctr": r[6],
                "cod_ctr": r[7],
                "cod_liv": r[8],
                "dt_ini_liv": r[9],
                "dt_ini_mns": r[10],
                "tipo_rapp": r[15],
                "dt_ini_tipo_ctr": r[16],
                "dt_fine_tipo_ctr": r[17],
                "dt_ini_rapp": r[18],
                "dt_fine_rapp": r[19],
                "ambito": r[20],
                "tipo_ass": r[21],
                "dt_proroga_termine": r[22],
                "dt_ctr_sost_mat": r[23],
                "qta_part_time": r[24],
                "titolare1": r[25],
                "titolare2": r[26],
                "motivo_promozione": r[27],
                "fascicolo_aggiornato": r[28],
                "titolo_studio": r[29],
                "desc_liv": r[31],
                "last_id_per": id_period,
                "id_dip": v_id_dip
            })

    else:
        # Nuovo dipendente → inserimento
        print(f"Inserisco nuovo dipendente {cod_dip}")
        new_id = get_next_id_dip()
        cur_dwh.execute("""
            INSERT INTO DWH_SPARK.D_ANAGRAFICA_SCD (
                ID_DIP, COD_DIP, COD_UNT, COD_RUOLO, COGNOME, NOME, DT_INI_UNT,
                TIPO_CTR, COD_CTR, COD_LIV, DT_INI_LIV, DT_INI_MNS,
                MATRICOLA, CF, SESSO, DT_NASCITA, TIPO_RAPP, DT_INI_TIPO_CTR,
                DT_FINE_TIPO_CTR, DT_INI_RAPP, DT_FINE_RAPP, AMBITO,
                TIPO_ASS, DT_PROROGA_TERMINE, DT_CTR_SOST_MAT, QTA_PART_TIME,
                TITOLARE1, TITOLARE2, MOTIVO_PROMOZIONE, FASCICOLO_AGGIORNATO,
                TITOLO_STUDIO, DESC_LIV,
                D_INIVAL, D_ENDVAL, D_INS, D_UPD, LAST_ID_PER
            )
            VALUES (
                :id_dip, :cod_dip, :cod_unt, :cod_ruolo, :cognome, :nome, :dt_ini_unt,
                :tipo_ctr, :cod_ctr, :cod_liv, :dt_ini_liv, :dt_ini_mns,
                :matricola, :cf, :sesso, :dt_nascita, :tipo_rapp, :dt_ini_tipo_ctr,
                :dt_fine_tipo_ctr, :dt_ini_rapp, :dt_fine_rapp, :ambito,
                :tipo_ass, :dt_proroga_termine, :dt_ctr_sost_mat, :qta_part_time,
                :titolare1, :titolare2, :motivo_promozione, :fascicolo_aggiornato,
                :titolo_studio, :desc_liv,
                :d_inival, NULL, SYSDATE, NULL, :last_id_per
            )
        """, {
            "id_dip": new_id,
            "cod_dip": r[0],
            "cod_unt": r[1],
            "cod_ruolo": r[2],
            "cognome": r[3],
            "nome": r[4],
            "dt_ini_unt": r[5],
            "tipo_ctr": r[6],
            "cod_ctr": r[7],
            "cod_liv": r[8],
            "dt_ini_liv": r[9],
            "dt_ini_mns": r[10],
            "matricola": r[11],
            "cf": r[12],
            "sesso": r[13],
            "dt_nascita": r[14],
            "tipo_rapp": r[15],
            "dt_ini_tipo_ctr": r[16],
            "dt_fine_tipo_ctr": r[17],
            "dt_ini_rapp": r[18],
            "dt_fine_rapp": r[19],
            "ambito": r[20],
            "tipo_ass": r[21],
            "dt_proroga_termine": r[22],
            "dt_ctr_sost_mat": r[23],
            "qta_part_time": r[24],
            "titolare1": r[25],
            "titolare2": r[26],
            "motivo_promozione": r[27],
            "fascicolo_aggiornato": r[28],
            "titolo_studio": r[29],
            "desc_liv": r[31],
            "d_inival": id_period,
            "last_id_per": id_period
        })

# --------------------------------------------
# Commit finale
# --------------------------------------------
conn_dwh.commit()
print("Caricamento SCD completato con successo.")

# --------------------------------------------
# Impostazione lingua per i nomi dei giorni/mesi
# --------------------------------------------

# --------------------------------------------
# Range di date da generare
# (esempio: tutto il 2025)
# --------------------------------------------
p_from = 20250101
p_to = 20251231

start_date = datetime.strptime(str(p_from), "%Y%m%d")
end_date = datetime.strptime(str(p_to), "%Y%m%d")

# --------------------------------------------
# Pulizia tabella (facoltativa)
# --------------------------------------------
cur_dwh.execute("TRUNCATE TABLE DWH_SPARK.D_PERIOD")
print("Tabella D_PERIOD svuotata.")

# --------------------------------------------
# Popolamento tabella giorno per giorno
# --------------------------------------------
current_date = start_date
count = 0

while current_date <= end_date:
    id_period = int(current_date.strftime("%Y%m%d"))
    id_year_month = int(current_date.strftime("%Y%m"))

    # Calcolo campi derivati
    day = current_date
    day_of_week = int(current_date.strftime("%u"))  # 1=lunedì ... 7=domenica
    day_of_month = int(current_date.strftime("%d"))
    day_of_year = int(current_date.strftime("%j"))
    day_name = unidecode.unidecode(current_date.strftime("%A")).upper()
    day_names = current_date.strftime("%a").upper()

    # Sabato o domenica = giorno festivo
    flag_h = 1 if day_names in ("SAB", "DOM") else 0

    week_of_month = int((day_of_month - 1) / 7) + 1
    week_of_year = int(current_date.strftime("%V"))
    month_num = int(current_date.strftime("%m"))
    month_numc = f"{month_num:02d}"
    month_name = current_date.strftime("%B").upper()
    month_names = current_date.strftime("%b").upper()
    quarter = (month_num - 1) // 3 + 1
    semester = 1 if month_num < 7 else 2
    year_num = current_date.strftime("%Y")

    # Inserimento riga
    cur_dwh.execute("""
                INSERT INTO DWH_SPARK.D_PERIOD (ID_PERIOD, ID_YEAR_MONTH, DAY, DAY_OF_WEEK, DAY_OF_MONTH, DAY_OF_YEAR,
                                                DAY_NAME, DAY_NAMES, FLAG_H, WEEK_OF_MONTH, WEEK_OF_YEAR,
                                                MONTH_NUM, MONTH_NUMC, MONTH_NAME, MONTH_NAMES, QUARTER, SEMESTER,
                                                YEAR_NUM)
                VALUES (:1, :2, :3, :4, :5, :6,
                        :7, :8, :9, :10, :11,
                        :12, :13, :14, :15, :16, :17, :18)
                """, (
                    id_period, id_year_month, day, day_of_week, day_of_month, day_of_year,
                    day_name, day_names, flag_h, week_of_month, week_of_year,
                    month_num, month_numc, month_name, month_names, quarter, semester, year_num
                ))

    count += 1
    current_date += timedelta(days=1)

print(f"Caricamento completato con successo! {count} record inseriti in D_PERIOD.")

# --------------------------------------------
# Recupero ultimo ID_PER disponibile
# --------------------------------------------
cur_stage.execute("""
                  SELECT MAX(ID_PER)
                  FROM GRP02_PRS
                  """)
max_id_per = cur_stage.fetchone()[0]
print(f"Ultimo ID_PER in GRP02_PRS: {max_id_per}")

# --------------------------------------------
# Recupero dati da GRP02_PRS per ultimo ID_PER
# --------------------------------------------
cur_stage.execute("""
                  SELECT CF, DATA_PRES, OREPRES, ORESTRAOR, OREPERMES
                  FROM GRP02_PRS
                  WHERE ID_PER = :1
                  """, (max_id_per,))
rows_presenze = cur_stage.fetchall()

# --------------------------------------------
# Popolamento FCT_PRESENZE
# --------------------------------------------

# --------------------------------------------
# Connessione Oracle
# --------------------------------------------
conn_stage = oracledb.connect(user=user_stage, password=pwd_stage, dsn=dsn_stage)
conn_dwh = oracledb.connect(user=user_dwh, password=pwd_dwh, dsn=dsn_dwh)

cur_stage = conn_stage.cursor()
cur_dwh = conn_dwh.cursor()


count = 0
for cf, data_pres, orepres, orestraor, orepermes in rows_presenze:
    # Converto data_pres in numero YYYYMM per confronto con D_ENDVAL
    data_num = int(data_pres.strftime("%Y%m"))

    # Recupero ID_DIP e chiavi surrogate dalle dimensioni tramite CF
    cur_dwh.execute("""
        SELECT a.id_dip, u.id_unt, r.id_ruolo
        FROM D_ANAGRAFICA_SCD a
                 JOIN DIM_UNITA u ON u.cod_unita = a.cod_unt
                 JOIN DIM_RUOLO r ON r.cod_ruolo = a.cod_ruolo
        WHERE a.cf = :cf
          AND (a.d_endval IS NULL OR a.d_endval >= :data_num)
    """, {"cf": cf, "data_num": data_num})

    dim_row = cur_dwh.fetchone()
    if not dim_row:
        # Se non trova corrispondenza, salta
        continue

    id_dip, id_unt, id_ruolo = dim_row

    # Recupero ID_PERIOD e ID_YEAR_MONTH da D_PERIOD
    cur_dwh.execute("""
        SELECT id_period, id_year_month
        FROM D_PERIOD
        WHERE day = :1
    """, (data_pres,))
    period_row = cur_dwh.fetchone()
    if not period_row:
        continue

    id_period, id_year_month = period_row

    # Verifica se partizione esiste e crea se necessario
    part_name = f"P_{id_year_month}"
    try:
        cur_dwh.execute(f"""
            ALTER TABLE FCT_PRESENZE ADD PARTITION {part_name} VALUES ({id_year_month})
        """)
        print(f"Creata partizione {part_name}")
    except oracledb.DatabaseError:
        # Se la partizione esiste già, ignora
        pass

    # Inserimento riga nella fact table
    cur_dwh.execute("""
        INSERT INTO FCT_PRESENZE (ID_PERIOD, ID_YEAR_MONTH, ID_DIP, ID_UNT, ID_RUOLO, OREPRES, ORESTRAOR, OREPERMES)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
    """, (id_period, id_year_month, id_dip, id_unt, id_ruolo, orepres, orestraor, orepermes))

    count += 1

# Commit finale
conn_dwh.commit()
print(f"Caricamento completato con successo! {count} record inseriti in FCT_PRESENZE.")


# --------------------------------------------
# Chiusura connessioni
# --------------------------------------------
conn_dwh.commit()
cur_stage.close()
cur_dwh.close()
conn_stage.close()
conn_dwh.close()


