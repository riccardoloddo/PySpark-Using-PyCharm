import logging
import oracledb
from datetime import datetime

class OracleLogHandler(logging.Handler):
    def __init__(self, user, password, dsn, cod_flusso, id_per):
        super().__init__()

        # Connessione Oracle (THIN)
        self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
        self.cursor = self.conn.cursor()

        self.cod_flusso = cod_flusso
        self.id_per = id_per
        self.id_run = self._get_next_id_run()

        # Contatore per gli step
        self.step_counter = 1

    def _get_next_id_run(self):
        """Restituisce il prossimo ID_RUN incrementale dalla tabella di log"""
        self.cursor.execute("SELECT NVL(MAX(ID_RUN), 0) + 1 FROM TLOG_LEVEL_ONE")
        row = self.cursor.fetchone()
        return row[0]

    def emit(self, record):
        """Viene chiamato automaticamente dai logging.info/debug/error"""
        try:
            msg = self.format(record)
            stato = "OK" if record.levelno < logging.ERROR else "KO"
            error_msg = msg if record.levelno >= logging.ERROR else None

            self.cursor.execute(
                """
                INSERT INTO TLOG_LEVEL_ONE 
                (ID_RUN, ID_PER, COD_FLUSSO, STEP, STMT, STATO, ERROR, DINS)
                VALUES (:1, :2, :3, :4, :5, :6, :7, SYSDATE)
                """,
                (self.id_run, self.id_per, self.cod_flusso, self.step_counter, msg, stato, error_msg)
            )
            self.conn.commit()

            # Incrementa lo step per il prossimo log
            self.step_counter += 1

        except Exception as e:
            # Se fallisce l'inserimento log su Oracle, lo stampiamo a console
            print(f"[WARN] Errore inserimento log su Oracle: {e}")

    def close(self):
        """Chiude connessione e cursore"""
        try:
            if getattr(self, "cursor", None):
                try:
                    self.cursor.close()
                except:
                    pass
            if getattr(self, "conn", None):
                try:
                    self.conn.close()
                except:
                    pass
        except Exception as e:
            print(f"[WARN] Errore nella chiusura connessione Oracle: {e}")
        super().close()
