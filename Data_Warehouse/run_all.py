# run_all.py

import dim_ruoli
import dim_unita
import dim_anagrafica
import fct_presenze
from Staging_Area import Color_Class as log

def main():
    log.print_info("Avvio caricamento DIM_RUOLO")
    dim_ruoli.main()

    log.print_info("Avvio caricamento DIM_UNITA")
    dim_unita.main()

    log.print_info("Avvio caricamento DIM_ANAGRAFICA")
    dim_anagrafica.main()

    log.print_info("Avvio caricamento FCT_PRESENZE")
    fct_presenze.main()

    log.print_info("Caricamento completo di tutte le tabelle. Popolato in DWH!")

if __name__ == "__main__":
    main()
