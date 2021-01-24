import pandas as pd
import numpy as np
import re

PARSE_TABLE = pd.read_csv('./parse-table.csv')

FIELDS_TYPES_MAPPING = dict(
    dateFields=['DATA','DATVEN'],
    intFields=['PRAZOT','TOTNEG','QUATOT','FATCOT'],
    floatFields=['PREABE','PREMAX','PREMIN','PREMED','PREULT','PREOFC','PREOFV','VOLTOT','PREEXE','PTOEXE'],
    stringFields=['TIPREG','CODBDI','CODNEG','TPMERC','NOMRES','ESPECI','MODREF','INDOPC','CODISI','DISMES'],
)

def parseField(fieldName: str, fieldType: str, value):
    if fieldName in FIELDS_TYPES_MAPPING['intFields']:
        try:
            return value.astype(np.int)
        except:
            return None
    elif fieldName in FIELDS_TYPES_MAPPING['floatFields']:
        integerDigitNumber = int(re.findall('\((\d+)\)', fieldType)[0])
        return np.apply_along_axis(lambda row: float(f'{row[0][:integerDigitNumber]}.{row[0][integerDigitNumber:]}'), 0, value[None,:])
    return np.apply_along_axis(lambda row: row[0].strip(), 0, value[None,:])
