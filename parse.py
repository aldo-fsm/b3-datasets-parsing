import fire
import re
import os
from utils import PARSE_TABLE, FIELDS_TYPES_MAPPING, getFieldParser
import numpy as np
import pandas as pd
import re
from tqdm import tqdm
from joblib import Parallel, delayed
from zipfile import ZipFile
from pandas.core.frame import DataFrame

def toCsv(inputPath: str, outputPath: str):
    """
    Convert dataset from "inputPath" to csv and save it into "outputPath"
    :param inputPath: path to raw dataset to be parsed
    :param outputPath: target path to save resulting file
    """
    fileName = inputPath.split('/')[-1].strip()
    outputPath = outputPath.strip()
    if os.path.isdir(outputPath):
        filenameWithoutExtension = re.sub(r'\.(txt|zip)$', '', fileName, flags=re.IGNORECASE)
        outputFileName = f'{filenameWithoutExtension}.csv'
        outputPath = os.path.join(outputPath, outputFileName)
    if os.path.isfile(outputPath): raise FileExistsError()

    lines = readFile(inputPath)

    parsed = toDataFrame(lines)
    print('DataFrame parsed')
    print('shape:', parsed.shape)
    print(f'Writing to {outputPath}')
    parsed.to_csv(outputPath)

def toParquet(inputPath: str, outputPath: str, compression='snappy'):
    """
    Convert dataset from "inputPath" to parquet and save it into "outputPath"
    :param inputPath: path to raw dataset to be parsed
    :param outputPath: target path to save resulting file
    :param compression: {{'snappy', 'gzip', 'brotli', None}}, default 'snappy'
        Name of the compression to use. Use None for no compression.
    """
    print(f'inputPath: {inputPath}')
    print(f'outputPath: {outputPath}')
    print(f'compression: {compression}')
    fileName = inputPath.split('/')[-1].strip()
    outputPath = outputPath.strip()
    if os.path.isdir(outputPath):
        filenameWithoutExtension = re.sub(r'\.(txt|zip)$', '', fileName, flags=re.IGNORECASE)
        outputFileName = f'{filenameWithoutExtension}.parquet'
        outputPath = os.path.join(outputPath, outputFileName)
    if os.path.isfile(outputPath): raise FileExistsError()

    lines = readFile(inputPath)

    parsed = toDataFrame(lines)
    print('DataFrame parsed')
    print('shape:', parsed.shape)
    print(f'Writing to {outputPath}')
    parsed.to_parquet(outputPath, engine='pyarrow', compression=compression)

def toDataFrame(lines) -> DataFrame:
    print('Creating char matrix...')
    rawRecords = np.array([list(line) for line in tqdm(lines)])[1:-1,:]

    extractedFields = {}
    print('Extracting fields...')
    for i, field in tqdm(PARSE_TABLE.iterrows()):
        extractedFields[field.fieldName] = rawRecords[:, field.startPos-1:field.endPos]

    def concatArrayString(key, value):
        return (key, np.apply_along_axis(lambda row: ''.join(row), 1, value))
    concatenatedFields =  Parallel(4)(
        delayed(concatArrayString)(key, value) for key, value in tqdm(list(extractedFields.items()))
    )

    print('Creating DataFrame...')
    df = pd.DataFrame({ key: value for key, value in concatenatedFields })

    print('Parsing fields...')
    for i, field in tqdm(list(PARSE_TABLE.iterrows())):
        df[field.fieldName] = df[field.fieldName].apply(getFieldParser(field.fieldName, field.type))
        if field.fieldName in FIELDS_TYPES_MAPPING['intFields']:
            df[field.fieldName] = df[field.fieldName].astype(pd.Int64Dtype())

    return df

def readFile(path):
    if re.match(r'.+\.zip$', path, flags=re.IGNORECASE):
        with ZipFile(path) as zipfile:
            filename = zipfile.filelist[0].filename
            with zipfile.open(filename, 'r') as f:
                return [line.decode('ISO-8859-1') for line in f.readlines()]
    with open(path, encoding='ISO-8859-1') as f:
        return f.readlines()

if __name__ == "__main__":
    fire.Fire({
        "to-csv": toCsv,
        "to-parquet": toParquet,
    })