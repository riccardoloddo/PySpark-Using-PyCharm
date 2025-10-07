import logging
import json
import os
from datetime import datetime
from table import TabellaDipendenti
from db_utils import DBUtils
from pyspark.sql.functions import current_date, col, to_date, when, current_timestamp, monotonically_increasing_id, lit, upper, lower, length, udf
from Color_Class import print_info, print_warn, print_error, print_step
from oracle_logger import OracleLogHandler

logger = logging.getLogger("flusso_oracle")

class GestoreFlussoDipendenti:
    def __init__(self, path_log_gestore, path_config_json):
        self.path_log_gestore = path_log_gestore

        # Logger
        self.logger = logging.getLogger("gestore")
        self.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(self.path_log_gestore, mode='w')
        formatter = logging.Formatter("%(message)s")
        file_handler.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)

        # Carico la configurazione JSON
        with open(path_config_json, 'r') as f:
            self.config_all = json.load(f)

    def genera_idper(self, path_csv):
        """
        Estrae ID_PER dai primi 6 caratteri del nome del file CSV.
        Se non è un numero valido, logga l’errore e interrompe l’esecuzione.
        """
        base_name = os.path.basename(path_csv)  # es. '202501_Anagrafica.csv'
        idper_str = base_name.split("_")[0]  # prende '202501'

        if not idper_str.isdigit() or len(idper_str) != 6:
            msg = f"ERRORE: Il file '{base_name}' non contiene un ID_PER valido (YYYYMM) all’inizio."
            self.logger.error(msg)
            raise ValueError(msg)  # interrompe l’esecuzione

        return int(idper_str)

    def processa_flusso(self, path_csv, path_log, flusso_corrente):
        # Genera l’ID_PER dal nome del file CSV
        id_per = self.genera_idper(path_csv)

        # Creo oggetto TabellaDipendenti passando ID_PER
        tabella = TabellaDipendenti(path_csv, path_log, id_per)

        # Recupero il dataframe grezzo
        df_grezzo = tabella.givemedataframe()

        # Recupero la configurazione per il flusso corrente
        if flusso_corrente not in self.config_all:
            msg = f"Flusso {flusso_corrente} non trovato nella configurazione JSON"
            self.logger.error(msg)
            raise ValueError(msg)

        config_corrente = self.config_all[flusso_corrente]["columns"]

        # Applico pulizia automatica
        tab_ok, tab_scarti = tabella.pulisci(config_corrente)

        # Log
        self.logger.info(
            f"ID_PER={id_per} - Tabelle create: df_grezzo={df_grezzo.count()}, "
            f"tab_ok={tab_ok.df.count()}, tab_scarti={tab_scarti.df.count()}, Data={datetime.now()}"
        )

        return df_grezzo, tab_ok, tab_scarti

    def load_to_oracle(self, tab_ok, tab_scarti=None, flusso_corrente=None, table_name=None, path_csv=None,
                       user="STAGE_SPARK", password="STAGE_SPARK", dsn="localhost:1521/orcl"):
        """
        Carica su Oracle un DataFrame pulito (tab_ok) e eventuali scarti (tab_scarti),
        prendendo ID_PER dal nome del file e i nomi delle colonne dal JSON.
        Gli scarti vanno su una tabella con lo stesso nome + '_SCARTI'.
        """
        if flusso_corrente not in self.config_all:
            msg = f"Flusso {flusso_corrente} non trovato nella configurazione JSON"
            self.logger.error(msg)
            raise ValueError(msg)

        config_corrente = self.config_all[flusso_corrente]["columns"]
        columns_oracle = list(config_corrente.keys())  # nomi colonne come su Oracle

        # Controllo consistenza colonne
        missing_cols = [c for c in columns_oracle if c not in tab_ok.df.columns]
        if missing_cols:
            msg = f"Incoerenza colonne: c'è incoerenza tra il .json e le tabelle Oracle {missing_cols}"
            self.logger.error(msg)
            raise ValueError(msg)

        # ---------- CARICAMENTO DATI PULITI ----------
        tab_ok_df = tab_ok.givemedataframe()
        id_per = self.genera_idper(path_csv)
        partition_name = f"P_{id_per}"
        tab_ok_df = tab_ok_df.withColumn("ID_PER", lit(id_per))
        tab_ok_oracle = tab_ok_df.select("ID_PER", *columns_oracle)
        rows_ok = [tuple(row) for row in tab_ok_oracle.collect()]

        db = DBUtils(user=user, password=password, dsn=dsn)
        db.ensure_partition_range(table_name, id_per)
        db.batch_insert(table_name, ["ID_PER"] + columns_oracle, rows_ok)
        self.logger.info(f"ID_PER={id_per} - Caricamento completato su Oracle tabella {table_name}")
        print_info(f"Caricamento completato tabella {table_name}!")

        # ---------- CARICAMENTO SCARTI (solo se presenti) ----------
        if tab_scarti is not None and hasattr(tab_scarti, "df") and tab_scarti.df.count() > 0:
            table_scarti = f"{table_name}_SCARTI"
            tab_scarti_df = tab_scarti.givemedataframe()
            # Tutte le colonne come stringhe
            for c in tab_scarti_df.columns:
                tab_scarti_df = tab_scarti_df.withColumn(c, tab_scarti_df[c].cast("string"))
            tab_scarti_df = tab_scarti_df.withColumn("ID_PER", lit(id_per))
            scarti_oracle = tab_scarti_df.select("ID_PER", *columns_oracle)
            rows_scarti = [tuple(row) for row in scarti_oracle.collect()]

            db.ensure_partition_range(table_scarti, id_per)
            db.batch_insert(table_scarti, ["ID_PER"] + columns_oracle, rows_scarti)
            self.logger.info(f"ID_PER={id_per} - Caricamento completato su Oracle tabella {table_scarti}")
            print_info(f"Caricamento completato tabella {table_scarti}!")

        db.close()

    def unisci_flussi(self, path_csv1, path_csv2, log, chiave_json1, chiave_json2):
        id_per = self.genera_idper(path_csv1)  # stesso ID_PER per entrambi

        tab1 = TabellaDipendenti(path_csv1, log, id_per)
        tab2 = TabellaDipendenti(path_csv2, log, id_per)

        df1 = tab1.givemedataframe()
        df2 = tab2.givemedataframe()

        config1 = self.config_all[chiave_json1]["columns"]
        config2 = self.config_all[chiave_json2]["columns"]

        tab_ok1, _ = tab1.pulisci(config1)
        tab_ok2, _ = tab2.pulisci(config2)

        df_ok1 = tab_ok1.givemedataframe()
        df_ok2 = tab_ok2.givemedataframe()

        # Join su COD_UNT
        df_join = df_ok1.join(df_ok2.select("COD_UNT", "DESC_UNT"), on="COD_UNT", how="left")
        df_join = df_join.withColumn("ID_PER", lit(id_per)).withColumn("DINS", current_timestamp())

        self.logger.info(f"[JOIN] Unite {df_ok1.count()} + {df_ok2.count()} -> {df_join.count()} righe")

        # Creo nuovo oggetto TabellaDipendenti per il join
        tab_join = TabellaDipendenti.__new__(TabellaDipendenti)
        tab_join.spark = tab_ok1.spark
        tab_join.df = df_join
        tab_join.idper = id_per
        tab_join.path_log = log

        return tab_join
