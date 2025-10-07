import argparse
from gestore import GestoreFlussoDipendenti

parser = argparse.ArgumentParser()
parser.add_argument("--user", required=True)
parser.add_argument("--password", required=True)
parser.add_argument("--dsn", required=True)
parser.add_argument("--flusso", required=True)
parser.add_argument("--chiave_json", required=True)
parser.add_argument("--tabella", required=True)
args = parser.parse_args()

path_log_gestore = "../logs/tlog_gestore.log"
path_config_json = "../config/config_flussi.json"
path_csv = f"../data/{args.flusso}.csv"
path_log = "../logs/tlog.log"

gestore = GestoreFlussoDipendenti(path_log_gestore, path_config_json)

# ---------------------------
# CASO NORMALE: un solo file
# ---------------------------
if args.flusso != "unita_completa":
    df_grezzo, tab_ok, tab_scarti = gestore.processa_flusso(path_csv, path_log, args.chiave_json)
    gestore.load_to_oracle(tab_ok, args.chiave_json, args.tabella, path_csv,
                           user=args.user, password=args.password, dsn=args.dsn)
    print(f"[INFO] Flusso '{args.flusso}' caricato correttamente su Oracle.")

# ---------------------------------------------------------
# CASO SPECIALE: unisci unitacr.csv e unita_esp.csv
# ---------------------------------------------------------
else:
    print("[INFO] Attivato flusso combinato 'unita_completa'...")

    # Percorsi dei due CSV
    path_csv1 = "../data/202504_unitacr.csv"
    path_csv2 = "../data/202504_unita_esp.csv"

    # Chiavi JSON per la pulizia
    chiave_json1 = "unitacr"
    chiave_json2 = "unita_esp"

    # Usa la nuova funzione del gestore
    df_join = gestore.unisci_flussi(path_csv1, path_csv2, path_log, chiave_json1, chiave_json2)

    # Carica direttamente il DataFrame su Oracle
    gestore.load_dataframe_to_oracle(
        df_join,
        "unita_completa",       # chiave logica
        args.tabella,           # es. GRP02_UNT
        path_csv1,
        user=args.user,
        password=args.password,
        dsn=args.dsn
    )

    print("[INFO] Flusso combinato 'unita_completa' caricato su Oracle.")

