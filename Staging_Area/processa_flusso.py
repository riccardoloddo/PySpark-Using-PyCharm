import argparse
from gestore import GestoreFlussoDipendenti
from Color_Class import print_info, print_warn, print_error, print_step
from oracle_logger import OracleLogHandler
import logging
import re

parser = argparse.ArgumentParser()
parser.add_argument("--user", required=True)
parser.add_argument("--password", required=True)
parser.add_argument("--dsn", required=True)
parser.add_argument("--flusso", required=True)
parser.add_argument("--chiave_json", required=True)
parser.add_argument("--tabella", required=True)
parser.add_argument("--mese", required=False, help="Mese da caricare in formato YYYYMM")
args = parser.parse_args()

path_log_gestore = "../logs/tlog_gestore.log"
path_config_json = "../config/config_flussi.json"
path_csv = f"../data/{args.flusso}.csv"
path_log = "../logs/tlog.log"

match = re.match(r"(\d{6})_(\w+)", args.flusso)
if match:
    id_per = match.group(1)
    cod_flusso = match.group(2).upper()
else:
    raise ValueError(f"Formato flusso non valido: {args.flusso}. Atteso YYYYMM_nomeflusso")

# Creo un logger dedicato
logger = logging.getLogger("flusso_oracle")
logger.setLevel(logging.DEBUG)  # o INFO se vuoi solo livelli alti

# Aggancio il tuo handler Oracle
oracle_handler = OracleLogHandler(
    user=args.user,
    password=args.password,
    dsn=args.dsn,
    cod_flusso=cod_flusso.upper(),
    id_per=int(id_per)
)
logger.addHandler(oracle_handler)

# Ora loggo solo tramite questo logger
logger.info(f"Inizio elaborazione flusso {args.flusso}")
# logger.debug("DEBUG di test")
# logger.error("ERROR di test")

gestore = GestoreFlussoDipendenti(path_log_gestore, path_config_json)

# ---------------------------
# CASO NORMALE: un solo file
# ---------------------------
if args.flusso != "202501_unita_completa":
    df_grezzo, tab_ok, tab_scarti = gestore.processa_flusso(path_csv, path_log, args.chiave_json)
    gestore.load_to_oracle(tab_ok, tab_scarti, args.chiave_json, args.tabella, path_csv,
                           user=args.user, password=args.password, dsn=args.dsn)

    print_info(f"Flusso '{args.flusso}' caricato correttamente su Oracle.")

else:
    print_info("Attivato flusso combinato 'unita_completa'...")

    mese = args.mese if args.mese else "202501"

    path_csv1 = f"../data/{mese}_unitacr.csv"
    path_csv2 = f"../data/{mese}_unita_esp.csv"

    tab_ok_join = gestore.unisci_flussi(path_csv1, path_csv2, path_log, "unitacr", "unita_esp")

    gestore.load_to_oracle(tab_ok_join, flusso_corrente="unita_completa", table_name=args.tabella, path_csv=path_csv1,
                           user=args.user, password=args.password, dsn=args.dsn)

    print_info(f"Flusso combinato 'unita_completa' del mese {mese} caricato su Oracle.")

