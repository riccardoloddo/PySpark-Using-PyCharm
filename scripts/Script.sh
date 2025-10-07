#!/bin/bash

# -----------------------------
# Parametri da linea di comando
# -----------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --user) USER="$2"; shift 2 ;;
        --password) PASSWORD="$2"; shift 2 ;;
        --dsn) DSN="$2"; shift 2 ;;
        --flusso) FLUSSO="$2"; shift 2 ;;
        --chiave_json) CHIAVE_JSON="$2"; shift 2 ;;
        --tabella) TABELLA="$2"; shift 2 ;;
        *) echo "Parametro sconosciuto $1"; exit 1 ;;
    esac
done

# Parametri di default se mancanti
USER="${USER:-STAGE_SPARK}"
PASSWORD="${PASSWORD:-STAGE_SPARK}"
DSN="${DSN:-localhost:1521/orcl}"

# -----------------------------
# Percorsi
# -----------------------------
PATH_BASE="../"
PATH_CSV="${PATH_BASE}data/${FLUSSO}.csv"
PATH_LOG="${PATH_BASE}logs/script_log_$(date +%Y%m%d_%H%M%S).log"
PATH_CONFIG_JSON="${PATH_BASE}config/config_flussi.json"

# -----------------------------
# Funzione log
# -----------------------------
function write_log() {
    local MESSAGE="$1"
    local TYPE="${2:-INFO}"
    local TIMESTAMP
    TIMESTAMP=$(date "+%Y/%m/%d %H:%M:%S:%3N")
    echo "[$TIMESTAMP - ${0##*/} - $TYPE] $MESSAGE" >> "$PATH_LOG"
}

# -----------------------------
# Start script
# -----------------------------
write_log "Script avviato." "INFO"
echo -e "\n\033[1;30m==============================\033[0m"
echo -e "\033[1;32m       SCRIPT AVVIATO         \033[0m"
echo -e "\033[1;30m==============================\033[0m\n"

# -----------------------------
# Controllo parametri
# -----------------------------
if [[ -z "$FLUSSO" || -z "$CHIAVE_JSON" ]]; then
    echo -e "\033[1;31mErrore: parametri mancanti (flusso o chiave_json).\033[0m"
    write_log "Parametri mancanti: flusso=$FLUSSO, chiave_json=$CHIAVE_JSON" "ERROR"
    exit 1
fi

# -----------------------------
# Controllo esistenza file CSV e JSON
# -----------------------------
if [[ "$FLUSSO" == "202501_unita_completa" ]]; then
    echo -e "\033[1;34mFlusso combinato rilevato: salto controllo file CSV singolo.\033[0m"
    write_log "Flusso combinato 'unita_completa': salto controllo CSV." "INFO"
    # Controllo solo il JSON
    if [[ ! -f "$PATH_CONFIG_JSON" ]]; then
        echo -e "\033[1;31mFile JSON mancante: $PATH_CONFIG_JSON\033[0m"
        write_log "File JSON mancante: $PATH_CONFIG_JSON" "ERROR"
        exit 1
    fi
else
    for f in "$PATH_CSV" "$PATH_CONFIG_JSON"; do
        if [[ ! -f "$f" ]]; then
            echo -e "\033[1;31mFile mancante: $f\033[0m"
            write_log "File mancante: $f" "ERROR"
            exit 1
        fi
    done
    echo -e "\033[1;34mFile CSV e JSON trovati correttamente.\033[0m"
    write_log "File CSV ($PATH_CSV) e JSON ($PATH_CONFIG_JSON) trovati." "INFO"
fi

# -----------------------------
# Esecuzione script Python
# -----------------------------
PYTHON_EXE="C:/Users/lddrc/_Personale/Lavoro/Advancia_mio/Python/Progetto_flussi/.venv/Scripts/python.exe"  # o il tuo python path
PYTHON_SCRIPT="${PATH_BASE}Staging_Area/processa_flusso.py"

CMD="$PYTHON_EXE $PYTHON_SCRIPT --user $USER --password $PASSWORD --dsn $DSN --flusso $FLUSSO --chiave_json $CHIAVE_JSON --tabella $TABELLA"

# Se è stato settato il mese, aggiungilo al comando
if [ -n "$MESE" ]; then
    CMD="$CMD --mese $MESE"
fi

echo -e "\033[1;36mAvvio processo Python...\033[0m"
write_log "Avvio processo
Python con comando: $CMD" "INFO"

eval "$CMD"

# -----------------------------
# Fine esecuzione
# -----------------------------
echo -e "\033[1;30m==============================\033[0m"
echo -e "\033[1;32m       SCRIPT COMPLETATO      \033[0m"
echo -e "\033[1;30m==============================\033[0m"
write_log "Script completato con successo." "INFO"
