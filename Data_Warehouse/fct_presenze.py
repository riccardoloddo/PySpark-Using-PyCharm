import oracledb
from datetime import datetime
from connessione import get_connections

def main():
    # --------------------------------------------
    # Connessioni
    # --------------------------------------------
    conn_stage, conn_dwh, cur_stage, cur_dwh = get_connections()

    # --------------------------------------------
    # Recupero ultimo ID_PER disponibile
    # --------------------------------------------
    cur_stage.execute("SELECT MAX(ID_PER) FROM GRP02_PRS")
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
    print(f"Record presenze recuperati: {len(rows_presenze)}")

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

        # Inserimento o aggiornamento tramite MERGE
        cur_dwh.execute("""
            MERGE INTO FCT_PRESENZE f
            USING (SELECT :id_period AS ID_PERIOD, :id_dip AS ID_DIP,
                          :id_unt AS ID_UNT, :id_ruolo AS ID_RUOLO
                   FROM dual) src
            ON (f.ID_PERIOD = src.ID_PERIOD AND f.ID_DIP = src.ID_DIP
                AND f.ID_UNT = src.ID_UNT AND f.ID_RUOLO = src.ID_RUOLO)
            WHEN MATCHED THEN UPDATE SET
                OREPRES = :orepres,
                ORESTRAOR = :orestraor,
                OREPERMES = :orepermes
            WHEN NOT MATCHED THEN INSERT (ID_PERIOD, ID_YEAR_MONTH, ID_DIP, ID_UNT, ID_RUOLO, OREPRES, ORESTRAOR, OREPERMES)
            VALUES (:id_period, :id_year_month, :id_dip, :id_unt, :id_ruolo, :orepres, :orestraor, :orepermes)
        """, {
            "id_period": id_period,
            "id_year_month": id_year_month,
            "id_dip": id_dip,
            "id_unt": id_unt,
            "id_ruolo": id_ruolo,
            "orepres": orepres,
            "orestraor": orestraor,
            "orepermes": orepermes
        })

        count += 1

    # Commit finale
    conn_dwh.commit()
    print(f"Caricamento completato con successo! {count} record inseriti/aggiornati in FCT_PRESENZE.")

if __name__ == "__main__":
    main()
