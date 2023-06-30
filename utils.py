import pandas as pd
import re
from datetime import date

PARSE_TABLE = pd.read_csv('./parse-table.csv')

FIELDS_TYPES_MAPPING = dict(
    dateFields=['DATA','DATVEN'],
    intFields=['PRAZOT','TOTNEG','QUATOT','FATCOT'],
    floatFields=['PREABE','PREMAX','PREMIN','PREMED','PREULT','PREOFC','PREOFV','VOLTOT','PREEXE','PTOEXE'],
    stringFields=['TIPREG','CODBDI','CODNEG','TPMERC','NOMRES','ESPECI','MODREF','INDOPC','CODISI','DISMES'],
)

def parseInt(value: str):
    try:
        return int(value)
    except:
        return None

def parseFloat(value: str, fieldType: str):
    integerDigitNumber = int(re.findall('\((\d+)\)', fieldType)[0])
    return float(f'{value[:integerDigitNumber]}.{value[integerDigitNumber:]}')

def parseDate(value: str):
    value = value.strip()
    if len(value) == 8:
        year = int(value[:4])
        month = int(value[4:6])
        day = int(value[6:])
        return date(year, month, day)

def getFieldParser(fieldName: str, fieldType: str):
    if fieldName in FIELDS_TYPES_MAPPING['dateFields']:
        return parseDate
    elif fieldName in FIELDS_TYPES_MAPPING['intFields']:
        return parseInt
    elif fieldName in FIELDS_TYPES_MAPPING['floatFields']:
        return lambda value: parseFloat(value, fieldType)
    return lambda value: value.strip()
