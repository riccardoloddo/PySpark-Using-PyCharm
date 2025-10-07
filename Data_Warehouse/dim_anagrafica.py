# load_dim_anagrafica.py

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
        cur_stage.execute("SELECT MAX(ID_PER) FROM STAGE_SPARK.GRP02_ANA")
        id_period = cur_stage.fetchone()[0]
        print(f"Ultimo ID_PERIOD: {id_period}")

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
        # Generatore chiave surrogata (fallback)
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
                        "desc_liv": r[31],
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

    finally:
        # Chiusura connessioni
        cur_stage.close()
        cur_dwh.close()
        conn_stage.close()
        conn_dwh.close()

if __name__ == "__main__":
    main()
