# load_dim_periodo.py

from connessione import get_connections
from datetime import datetime, timedelta
import unidecode

def main():
    # --------------------------------------------
    # Connessioni
    # --------------------------------------------
    _, conn_dwh, _, cur_dwh = get_connections()  # Serve solo il DWH

    try:
        # --------------------------------------------
        # Range di date da generare
        # Modifica p_from e p_to se necessario
        # --------------------------------------------
        p_from = 20250101
        p_to = 20251231

        start_date = datetime.strptime(str(p_from), "%Y%m%d")
        end_date = datetime.strptime(str(p_to), "%Y%m%d")

        # --------------------------------------------
        # Pulizia tabella (facoltativa)
        # --------------------------------------------
        cur_dwh.execute("TRUNCATE TABLE DWH_SPARK.D_PERIOD")
        print("Tabella D_PERIOD svuotata.")

        # --------------------------------------------
        # Popolamento tabella giorno per giorno
        # --------------------------------------------
        current_date = start_date
        count = 0

        while current_date <= end_date:
            id_period = int(current_date.strftime("%Y%m%d"))
            id_year_month = int(current_date.strftime("%Y%m"))

            # Calcolo campi derivati
            day = current_date
            day_of_week = int(current_date.strftime("%u"))  # 1=lunedì ... 7=domenica
            day_of_month = int(current_date.strftime("%d"))
            day_of_year = int(current_date.strftime("%j"))
            day_name = unidecode.unidecode(current_date.strftime("%A")).upper()
            day_names = current_date.strftime("%a").upper()
            flag_h = 1 if day_names in ("SAB", "DOM") else 0
            week_of_month = int((day_of_month - 1) / 7) + 1
            week_of_year = int(current_date.strftime("%V"))
            month_num = int(current_date.strftime("%m"))
            month_numc = f"{month_num:02d}"
            month_name = current_date.strftime("%B").upper()
            month_names = current_date.strftime("%b").upper()
            quarter = (month_num - 1) // 3 + 1
            semester = 1 if month_num < 7 else 2
            year_num = current_date.strftime("%Y")

            # Inserimento riga
            cur_dwh.execute("""
                INSERT INTO DWH_SPARK.D_PERIOD (
                    ID_PERIOD, ID_YEAR_MONTH, DAY, DAY_OF_WEEK, DAY_OF_MONTH, DAY_OF_YEAR,
                    DAY_NAME, DAY_NAMES, FLAG_H, WEEK_OF_MONTH, WEEK_OF_YEAR,
                    MONTH_NUM, MONTH_NUMC, MONTH_NAME, MONTH_NAMES, QUARTER, SEMESTER,
                    YEAR_NUM
                )
                VALUES (:1, :2, :3, :4, :5, :6,
                        :7, :8, :9, :10, :11,
                        :12, :13, :14, :15, :16, :17, :18)
            """, (
                id_period, id_year_month, day, day_of_week, day_of_month, day_of_year,
                day_name, day_names, flag_h, week_of_month, week_of_year,
                month_num, month_numc, month_name, month_names, quarter, semester, year_num
            ))

            count += 1
            current_date += timedelta(days=1)

        # Commit finale
        conn_dwh.commit()
        print(f"Caricamento completato con successo! {count} record inseriti in D_PERIOD.")

    finally:
        # Chiusura cursore e connessione
        cur_dwh.close()
        conn_dwh.close()

if __name__ == "__main__":
    main()
