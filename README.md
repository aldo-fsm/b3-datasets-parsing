# b3-datasets-parsing
Parses datasets of stock prices provided by B3 from txt to csv or parquet

Raw Datasets download: http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/

## Setup
```console
pipenv install
pipenv shell
```
## Usage
```console
# Convert to CSV
python parse.py to-csv <input-path> <output-path>

# Convert to Parquet
python parse.py to-parquet <input-path> <output-path>
```
