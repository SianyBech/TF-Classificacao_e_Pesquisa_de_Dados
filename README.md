# Sistema de Matrículas — ETL, arquivo binário e índice

## Passos para rodar (exemplo)
1. Converter CSV bruto para CSV limpo (long):

```bash
python src/etl.py data_raw/dee-5406.csv data_clean/matriculas_clean.csv
```

2. Gerar arquivo binário + índice:

```bash
python src/writer.py data_clean/matriculas_clean.csv bin_files/matriculas.dat bin_files/index_mun.pkl
```

3. Pesquisar (exemplo):

```bash
python src/search_cli.py --bin bin_files/matriculas.dat --index bin_files/index_mun.pkl --municipio "Porto Alegre" --ano-inicio 2018 --ano-fim 2021 --sexo F --page 1
```

## Observações
- Se o seu CSV tiver outro encoding, no `etl.py` ajuste `encoding='latin1'` para o apropriado.
- Se houver muitos anos/colunas, o ETL já converte wide->long automaticamente ao detectar anos no header.
- Para performance com muitos registros: considere construir um B+ Tree ou dividir índice em arquivos por bloco. Aqui implementamos um índice em `pickle` (fácil de entender e suficiente para o escopo do trabalho).