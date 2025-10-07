#!/bin/bash

# --- Directory dello script ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# --- Percorso di run_all.py ---
RUN_ALL="$PROJECT_DIR/Data_Warehouse/run_all.py"

echo "--------------------------------------------------"
echo "Eseguo run_all.py..."
echo "Progetto root: $PROJECT_DIR"
echo "File eseguibile: $RUN_ALL"
echo "--------------------------------------------------"

# --- Imposto PYTHONPATH alla root del progetto ---
export PYTHONPATH="$PROJECT_DIR"

# --- Esecuzione ---
python "$RUN_ALL"

if [ $? -eq 0 ]; then
    echo "--------------------------------------------------"
    echo "run_all.py eseguito correttamente!"
    echo "--------------------------------------------------"
else
    echo "--------------------------------------------------"
    echo "Errore durante l'esecuzione di run_all.py"
    echo "--------------------------------------------------"
fi
