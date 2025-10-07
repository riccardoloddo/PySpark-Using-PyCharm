# load_dim_unita.py

from connessione import get_connections

def main():
    # --------------------------------------------
    # Connessioni
    # --------------------------------------------
    conn_stage, conn_dwh, cur_stage, cur_dwh = get_connections()

    try:
        # --------------------------------------------
        # Recupero ID_PERIOD più recente dal stage
        # --------------------------------------------
        cur_stage.execute("SELECT MAX(ID_PER) FROM STAGE_SPARK.GRP02_UNT")
        id_period = cur_stage.fetchone()[0]
        print(f"Ultimo ID_PERIOD: {id_period}")

        # --------------------------------------------
        # Recupero UNITA dal stage
        # --------------------------------------------
        cur_stage.execute(f"""
            SELECT DISTINCT COD_UNT, DESC_UNT
            FROM STAGE_SPARK.GRP02_UNT
            WHERE ID_PER = {id_period}
        """)
        unita = cur_stage.fetchall()  # lista di tuple (COD_UNT, DESC_UNT)

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

        # Commit finale
        conn_dwh.commit()
        print("DIM_UNITA aggiornata correttamente.")

    finally:
        # Chiusura connessioni
        cur_stage.close()
        cur_dwh.close()
        conn_stage.close()
        conn_dwh.close()

# Permette di eseguire il file direttamente
if __name__ == "__main__":
    main()
