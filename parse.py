import fire
import re
import os
from utils import PARSE_TABLE, getFieldParser
import numpy as np
import pandas as pd
import re
from tqdm import tqdm
from joblib import Parallel, delayed

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
        outputFileName = re.sub('\.\w+$', '.csv', fileName)
        outputPath = os.path.join(outputPath, outputFileName)
    if os.path.isfile(outputPath): raise FileExistsError()
    
    with open(inputPath) as f:
        lines = f.readlines()

    parsed = toDataFrame(lines)
    print('DataFrame parsed')
    print('shape:', parsed.shape)
    print(f'Writing to {outputPath}')
    parsed.to_csv(outputPath)

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

    return df

if __name__ == "__main__":
    fire.Fire({
        "to-csv": toCsv
    })