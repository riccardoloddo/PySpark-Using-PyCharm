# load_dim_ruolo.py
from connessione import get_connections

def main():
    conn_stage, conn_dwh, cur_stage, cur_dwh = get_connections()

    try:
        # Recupero ID_PERIOD più recente dal stage
        cur_stage.execute("SELECT MAX(ID_PER) FROM STAGE_SPARK.GRP02_RUO")
        id_period = cur_stage.fetchone()[0]
        print(f"Ultimo ID_PERIOD: {id_period}")

        # Recupero RUOLI dal stage
        cur_stage.execute("""
            SELECT DISTINCT COD_RUOLO, DESC_RUOLO
            FROM STAGE_SPARK.GRP02_RUO
            WHERE ID_PER = :id_per
        """, {"id_per": id_period})
        ruoli = cur_stage.fetchall()

        # Inserimento / aggiornamento ruoli nel DWH
        for cod_ruolo, desc_ruolo in ruoli:
            cur_dwh.execute("""
                MERGE INTO DWH_SPARK.DIM_RUOLO d
                USING (SELECT :cod_ruolo AS COD_RUOLO FROM dual) src
                ON (d.COD_RUOLO = src.COD_RUOLO)
                WHEN MATCHED THEN UPDATE SET
                    d.DESCR_RUOLO = :desc_ruolo,
                    d.D_UPD = SYSDATE,
                    d.LAST_ID_PER = :id_per
                WHEN NOT MATCHED THEN INSERT (ID_RUOLO, COD_RUOLO, DESCR_RUOLO, D_INS, D_UPD, LAST_ID_PER)
                VALUES (DWH_SPARK.SEQ_DIM_RUOLO.NEXTVAL, :cod_ruolo, :desc_ruolo, SYSDATE, SYSDATE, :id_per)
            """, {"cod_ruolo": cod_ruolo, "desc_ruolo": desc_ruolo, "id_per": id_period})

        conn_dwh.commit()
        print("DIM_RUOLO aggiornata correttamente.")

    finally:
        cur_stage.close()
        cur_dwh.close()
        conn_stage.close()
        conn_dwh.close()


if __name__ == "__main__":
    main()
