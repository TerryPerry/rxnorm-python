import sqlite3
#this is mostly an adaptation of the mysql scripts that RxNorm comes with
#database path here
basepath = r'C:\Program Files\RxNorm\rrf\\'
rxnormdb = r'C:\Program Files\RxNorm\rxnorm.db'
rxnatomPath = basepath+ 'RXNATOMARCHIVE.RRF'
rxnconsoPath = basepath + 'RXNCONSO.RRF'
rxnrelPath = basepath + 'RXNREL.RRF'
rxnsabPath = basepath + 'RXNSAB.RRF'
rxnsatPath = basepath + 'RXNSAT.RRF'
rxnstyPath = basepath + 'RXNSTY.RRF'
rxndocPath = basepath + 'RXNDOC.RRF'
rxncuichangesPath = basepath + 'RXNCUICHANGES.RRF'
rxncuiPath = basepath + 'RXNCUI.RRF'

con = sqlite3.connect(rxnormdb)
cur = con.cursor()
cur.executescript('''
DROP  TABLE IF EXISTS RXNATOMARCHIVE;
CREATE TABLE RXNATOMARCHIVE
(
   RXAUI             INTEGER NOT NULL,
   AUI               TEXT,
   STR               TEXT NOT NULL,
   ARCHIVE_TIMESTAMP TEXT NOT NULL,
   CREATED_TIMESTAMP TEXT NOT NULL,
   UPDATED_TIMESTAMP TEXT NOT NULL,
   CODE              INTEGER,
   IS_BRAND          TEXT,
   LAT               TEXT,
   LAST_RELEASED     TEXT,
   SAUI              TEXT,
   VSAB              TEXT,
   RXCUI             INTEGER,
   SAB               TEXT,
   TTY               TEXT,
   MERGED_TO_RXCUI   INTEGER,
   PRIMARY KEY (RXAUI, RXCUI, MERGED_TO_RXCUI)
)
;

            
DROP  TABLE IF EXISTS RXNCONSO;
CREATE TABLE RXNCONSO
(
   RXCUI             INTEGER NOT NULL,
   LAT               TEXT NOT NULL,
   TS                TEXT,
   LUI               TEXT,
   STT               TEXT,
   SUI               TEXT,
   ISPREF            TEXT,
   RXAUI             INTEGER NOT NULL,
   SAUI              TEXT,
   SCUI              TEXT,
   SDUI              TEXT,
   SAB               TEXT NOT NULL,
   TTY               TEXT NOT NULL,
   CODE              TEXT NOT NULL,
   STR               TEXT NOT NULL,
   SRL               TEXT,
   SUPPRESS          TEXT,
   CVF               TEXT,
   PRIMARY KEY (RXAUI)

)
;

DROP TABLE IF EXISTS RXNREL;
CREATE TABLE RXNREL
(
   RXCUI1    INTEGER,
   RXAUI1    INTEGER,
   STYPE1    TEXT,
   REL       TEXT ,
   RXCUI2    INTEGER ,
   RXAUI2    INTEGER,
   STYPE2    TEXT,
   RELA      TEXT ,
   RUI       INTEGER,
   SRUI      TEXT,
   SAB       TEXT NOT NULL,
   SL        TEXT,
   DIR       TEXT,
   RG        TEXT,
   SUPPRESS  TEXT,
   CVF       TEXT,
   PRIMARY KEY (RXAUI1, RXCUI1, RXAUI2, RXCUI2, REL, RUI)
)
;

DROP TABLE IF EXISTS RXNSAB;
CREATE TABLE RXNSAB
(
   VCUI           TEXT,
   RCUI           TEXT,
   VSAB           TEXT,
   RSAB           TEXT NOT NULL,
   SON            TEXT,
   SF             TEXT,
   SVER           TEXT,
   VSTART         TEXT,
   VEND           TEXT,
   IMETA          TEXT,
   RMETA          TEXT,
   SLC            TEXT,
   SCC            TEXT,
   SRL            integer,
   TFR            integer,
   CFR            integer,
   CXTY           TEXT,
   TTYL           TEXT,
   ATNL           TEXT,
   LAT            TEXT,
   CENC           TEXT,
   CURVER         TEXT,
   SABIN          TEXT,
   SSN            TEXT,
   SCIT           TEXT,
   PRIMARY KEY (RSAB)
)
;

DROP TABLE IF EXISTS RXNSAT;
CREATE TABLE RXNSAT
(
   RXCUI            INTEGER ,
   LUI              TEXT,
   SUI              TEXT,
   RXAUI            INTEGER,
   STYPE            TEXT,
   CODE             TEXT,
   ATUI             TEXT,
   SATUI            TEXT,
   ATN              TEXT NOT NULL,
   SAB              TEXT NOT NULL,
   ATV              TEXT,
   SUPPRESS         TEXT,
   CVF              TEXT,
   PRIMARY KEY (RXAUI, RXCUI, SAB, ATN, ATV)
)
;

DROP TABLE IF EXISTS RXNSTY;
CREATE TABLE RXNSTY
(
   RXCUI          INTEGER NOT NULL,
   TUI            TEXT,
   STN            TEXT,
   STY            TEXT,
   ATUI           TEXT,
   CVF            TEXT,
   PRIMARY KEY (RXCUI, STY)
)
;

DROP TABLE IF EXISTS RXNDOC;
CREATE TABLE RXNDOC (
    DOCKEY      TEXT NOT NULL,
    VALUE       TEXT,
    TYPE        TEXT NOT NULL,
    EXPL        TEXT
)
;
DROP  TABLE IF EXISTS  RXNCUICHANGES;
CREATE TABLE RXNCUICHANGES
(
      RXAUI         INTEGER,
      CODE          TEXT,
      SAB           TEXT,
      TTY           TEXT,
      STR           TEXT,
      OLD_RXCUI     INTEGER NOT NULL,
      NEW_RXCUI     INTEGER NOT NULL
)
;

DROP  TABLE IF EXISTS  RXNCUI;
 CREATE TABLE RXNCUI (
 CUI1 INTEGER,
 VER_START TEXT,
 VER_END   TEXT,
 CARDINALITY INTEGER,
 CUI2       INTEGER,
 PRIMARY KEY (CUI1, CUI2)
)
;''')

with open(rxnatomPath,encoding='UTF-8') as rxnatomData:
    for line in rxnatomData:
        tupline = line.split('|')
        del tupline[-1]
        cur.execute('''insert into RXNATOMARCHIVE values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',tupline)
with open(rxnconsoPath,encoding='UTF-8') as rxnconsoData:
    for line in rxnconsoData:
        tupline = line.split('|')
        del tupline[-1]
        cur.execute('''insert into RXNCONSO values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',tupline)
with open(rxnrelPath,encoding='UTF-8') as rxnrelData:
    for line in rxnrelData:
        tupline = line.split('|')
        del tupline[-1]
        cur.execute('''insert into RXNREL values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',tupline)
with open(rxnsabPath,encoding='UTF-8') as rxnsabData:
    for line in rxnsabData:
        tupline = line.split('|')
        del tupline[-1]
        cur.execute('''insert into RXNSAB values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',tupline)
with open(rxnsatPath,encoding='UTF-8') as rxnsatData:
    for line in rxnsatData:
        tupline = line.split('|')
        del tupline[-1]
        cur.execute('''insert into RXNSAT values (?,?,?,?,?,?,?,?,?,?,?,?,?)''',tupline)
with open(rxnstyPath,encoding='UTF-8') as rxnstyData:
    for line in rxnstyData:
        tupline = line.split('|')
        del tupline[-1]
        cur.execute('''insert into RXNSTY values (?,?,?,?,?,?)''',tupline)
with open(rxndocPath,encoding='UTF-8') as rxndocData:
    for line in rxndocData:
        tupline = line.split('|')
        del tupline[-1]
        cur.execute('''insert into RXNDOC values (?,?,?,?)''',tupline)
with open(rxncuichangesPath,encoding='UTF-8') as rxncuichangesData:
    for line in rxncuichangesData:
        tupline = line.split('|')
        del tupline[-1]
        cur.execute('''insert into RXNCUICHANGES values (?,?,?,?,?,?,?)''',tupline)
with open(rxncuiPath,encoding='UTF-8') as rxncuiData:
    for line in rxncuiData:
        tupline = line.split('|')
        del tupline[-1]
        cur.execute('''insert into RXNCUI values (?,?,?,?,?)''',tupline)
    
cur.execute('''CREATE INDEX X_RXNCONSO_STR ON RXNCONSO(STR);''')
cur.execute('''CREATE INDEX X_RXNCONSO_RXCUI ON RXNCONSO(RXCUI);''')
cur.execute('''CREATE INDEX X_RXNCONSO_TTY ON RXNCONSO(TTY);''')
cur.execute('''CREATE INDEX X_RXNCONSO_CODE ON RXNCONSO(CODE);''')

cur.execute('''CREATE INDEX X_RXNSAT_RXCUI ON RXNSAT(RXCUI);''')
cur.execute('''CREATE INDEX X_RXNSAT_ATV ON RXNSAT(ATV);''')
cur.execute('''CREATE INDEX X_RXNSAT_ATN ON RXNSAT(ATN);''')

cur.execute('''CREATE INDEX X_RXNREL_RXCUI1 ON RXNREL(RXCUI1);''')
cur.execute('''CREATE INDEX X_RXNREL_RXCUI2 ON RXNREL(RXCUI2);''')
cur.execute('''CREATE INDEX X_RXNREL_RELA ON RXNREL(RELA);''')

cur.execute('''CREATE INDEX X_RXNATOMARCHIVE_RXAUI ON RXNATOMARCHIVE(RXAUI);''')
cur.execute('''CREATE INDEX X_RXNATOMARCHIVE_RXCUI ON RXNATOMARCHIVE(RXCUI);''')
cur.execute('''CREATE INDEX X_RXNATOMARCHIVE_MERGED_TO ON RXNATOMARCHIVE(MERGED_TO_RXCUI);''')

con.commit()
con.close()

