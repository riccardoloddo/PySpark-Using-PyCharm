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

df_grezzo, tab_ok, tab_scarti = gestore.processa_flusso(path_csv, path_log, args.chiave_json)
gestore.load_to_oracle(tab_ok, args.chiave_json, args.tabella, path_csv,
                       user=args.user, password=args.password, dsn=args.dsn)
print(f"[INFO] Flusso '{args.flusso}' caricato correttamente su Oracle.")

