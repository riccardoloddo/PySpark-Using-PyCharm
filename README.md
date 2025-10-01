# PySpark — Microprogetto di pulizia e validazione dati dipendenti

Questo repository contiene un **microprogetto in PySpark** per la lettura, la pulizia e la validazione di dataset relativi ai dipendenti.  
L’obiettivo è mostrare come organizzare una pipeline dinamica di data processing con Spark, mantenendo log delle operazioni e distinguendo i dati validi da quelli errati.
Si è implementato poi, tramite **OracleDB**, un metodo per inserire la tabelle filtrate nelle tabelle 02 del motorino.
L’intero processo è automatizzato tramite uno **script Bash**, che gestisce:
- il controllo dei parametri e dei file CSV/JSON,
- la verifica della chiave di configurazione JSON,
- l’esecuzione del processo Python con i parametri corretti,
- il logging di tutte le operazioni, con messaggi colorati per evidenziare lo stato delle operazioni.

---

## Ambiente di lavoro

Questo progetto è stato sviluppato principalmente in **PyCharm**, utilizzando **Git Bash** per l’esecuzione degli script di automazione.  

**Vantaggi principali:**
- **PyCharm**: gestione avanzata dei progetti Python, completamento automatico del codice, debugging integrato e supporto per virtual environment.  
- **Git Bash**: esecuzione di script Bash in ambiente Windows con comportamento coerente rispetto a Linux/Unix.  
- Pipeline automatizzate testabili direttamente dall’IDE senza dover cambiare terminale o ambiente.


## Funzionalità principali
- **Lettura di file CSV di input**: (es. `flusso.csv`, `unitacr.csv`)
- **Validazione dei campi principali**: Codice Fiscale, Data di Nascita, Nome, Salario, e altre colonne configurabili
- **Configurazione dinamica tramite file JSON**: (`config_flussi.json`) per gestire più flussi con regole diverse
- **Separazione automatica dei record**: Buoni / Scarti
- **Due livelli dedicati per i file di Log**: `tlog.log` per i dettagli delle singole operazioni, `tlog_gestore.log` per gestire diversi flussi
- **Possibilità di aggiungere nuovi flussi** senza modificare il codice
- **Unione e gestione di flussi multipli**

---

## Configurazione dinamica
Tutti i flussi e le regole di validazione sono definiti in `data/config_flussi.json`
Esempio di struttura:
``` 
{
  "flusso": {
    "columns": {
      "SALARIO": {"type": "positive_number"},
      "DN": {"type": "date", "format": "yyyy-MM-dd"},
      "NOME": {"type": "string_no_numbers"},
      "CF": {"type": "string_length", "length": 16}
    }
  },
    "unitacr": {
    "columns": {
      "COD_UNT": {"type": "positive_number"},
      "COD_ACR": {"type": "string_length", "length": 3}
    }
  }
 }
``` 

## Struttura del progetto
``` 
PySpark/
├─ src/ # codice sorgente (es. tabella_dipendenti.py, gestore_flussi.py)
├─ data/ # CSV di esempio (Flusso.csv, Flusso2.csv, config_flussi.json)
├─ logs/ # log di esempio (non committare log runtime)
├─ notebooks/ # eventuali Jupyter notebook
├─ tests/ # test (se aggiunti)
├─ docs/ # documentazione aggiuntiva
├─ .gitignore
├─ README.md
└─ requirements.txt
```

---

## Prerequisiti
- **Python 3.8+**
- [Apache Spark](https://spark.apache.org/) con PySpark installato  

Installazione rapida di PySpark:
```bash```
pip install pyspark

## Istruzioni per l'esecuzione

1. Posizionare i file CSV di input nella cartella `data/`. Il formato deve rispettare le seguenti regole:
	- Separatore dei campi coerente (es. `,`), uniforme per ciascun file.
	- La prima riga deve contenere i nomi delle colonne (header).

2. Configurazione dei flussi tramite JSON:  
	- Modificare o aggiungere nuove configurazioni nel file `config_flussi.json`.
	- Specificare per ciascuna colonna le regole di validazione da applicare, richiamando i metodi disponibili nella classe `Operazioni` (ad esempio `positive_number`, `string_no_numbers`, `date`, `email`, `string_length`).
	- Ogni colonna può avere una o più regole di validazione, applicate sequenzialmente durante il processo di pulizia.

3. Installare le dipendenze del progetto:
```bash
pip install -r requirements.txt
```

3. Eseguire gli script Python dalla cartella `src/`.  
```bash
python src/data_processor.py
```

5. Configurare correttamente i percorsi dei file nel codice:
	- `path_config_json`: percorso del file JSON di configurazione dei flussi  
	- `path_log_gestore`: percorso del log principale del gestore dei flussi  
	- `path_log`: percorso del log dettagliato per il singolo flusso  
	- `path_flusso`: percorso del file CSV da elaborare  
	- `flusso_corrente` o `chiave_json`: identificatore del flusso da processare

6. Creare un oggetto tramite la classe `GestoreFlussoDipendenti`. Richiamare il metodo `processa_flusso`, che restituisce tre DataFrame:  
	- `df_grezzo`: dati letti dal file originale  
	- `tab_ok`: dati validi  
	- `tab_scarti`: dati non validi  

8. I log generati durante l’esecuzione vengono salvati nella cartella `logs/`.  
9. Ogni oggetto `GestoreFlussoDipendenti` genera automaticamente una colonna `IDRUN` incrementale per identificare i flussi elaborati.

## Note

- Tutti i CSV devono essere disponibili nella cartella `data/` prima dell’esecuzione.  
- Il codice è modulare: è possibile aggiungere nuovi flussi o regole di validazione modificando i file in `src/`.  
- La separazione dei record validi e non validi avviene automaticamente in base alle regole definite.  
- Attualmente non è prevista storicizzazione dei dati: la colonna `IDRUN` serve solo per distinguere i flussi durante la singola esecuzione.
- Provato solo con tabella unità, ma è un pò diverso dalla logica del motorino. Non ho ID_PER e la struttura del flusso è leggermente diversa. 

## Possibili Miglioramenti

- Estendere le regole di validazione e migliorare i messaggi di log in caso di errore di configurazione JSON.  
- Valutare se utilizzare un singolo file JSON con tutte le configurazioni o un file dedicato per ciascun flusso.  
- Ottimizzare le funzioni nella classe `Operazioni`, valutando se suddividere alcune validazioni in più metodi indipendenti (es. controllo numerico e controllo di positività separati).
- Aggiungere un constraint al fiile .json. Ad esempio {"type": "foreign_key"} dove mi assicuro **integrità referenziale**. 