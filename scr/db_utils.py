import oracledb
from Color_Class import print_info, print_warn, print_error, print_step
from oracle_logger import OracleLogHandler

class DBUtils:
    def __init__(self, user, password, dsn):
        self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
        self.cur = self.conn.cursor()

    def ensure_partition(self, table_name, partition_name, idper):
        """
        Truncate se la partizione esiste, altrimenti crea una nuova partizione LIST per idper.
        """
        # Controlla se la partizione esiste
        stmt_check = f"""
        SELECT COUNT(*)
        FROM user_tab_partitions
        WHERE table_name = '{table_name.upper()}'
          AND partition_name = '{partition_name.upper()}'
        """
        self.cur.execute(stmt_check)
        v_count = self.cur.fetchone()[0]

        if v_count == 1:
            # La partizione esiste → truncate
            self.cur.execute(f"ALTER TABLE {table_name} TRUNCATE PARTITION {partition_name}")
            print_info(f"Truncate partizione {partition_name}")
        else:
            # La partizione non esiste → add partition
            self.cur.execute(f"""
                ALTER TABLE {table_name}
                SPLIT PARTITION P_DEFAULT
                VALUES ({idper})
                INTO (PARTITION {partition_name}, PARTITION P_DEFAULT)
            """)
            print_info(f"Creata nuova partizione {partition_name}")

        self.conn.commit()

    def ensure_partition_range(self, table_name, idper):
        """
        Controlla se esiste una partizione RANGE per `idper`.
        Se esiste → TRUNCATE partizione.
        Se non esiste → crea una nuova partizione RANGE con VALUES LESS THAN (idper + 1).
        """
        # Nome della partizione attesa
        partition_name = f"P_{idper}"

        # Step 1: Controlla se la partizione esiste
        stmt_check = f"""
        SELECT COUNT(*)
        FROM user_tab_partitions
        WHERE table_name = '{table_name.upper()}'
          AND partition_name = '{partition_name.upper()}'
        """
        self.cur.execute(stmt_check)
        exists = self.cur.fetchone()[0] == 1

        if exists:
            # Partizione esistente → truncate
            self.cur.execute(f"ALTER TABLE {table_name} TRUNCATE PARTITION {partition_name}")
            print_info(f"Truncate partizione esistente: {partition_name}")
        else:
            # Nessuna partizione copre idper → crea nuova
            high_value = idper + 1  # limite superiore della nuova partizione
            self.cur.execute(f"""
            ALTER TABLE {table_name}
            ADD PARTITION {partition_name} VALUES LESS THAN ({high_value})
            """)
            print_info(f"Creata nuova partizione RANGE: {partition_name} con HIGH_VALUE={high_value}")

        self.conn.commit()

    def batch_insert(self, table_name, columns, rows):
        """
        Inserisce una lista di tuple in una tabella Oracle.
        """
        cols_str = ", ".join(columns)
        placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
        sql_insert = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
        self.cur.executemany(sql_insert, rows)
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()