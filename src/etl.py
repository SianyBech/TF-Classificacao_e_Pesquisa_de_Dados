# src/etl.py
import csv
import re
import os
from typing import List
from src.utils import parse_number_robust, normalize_text

def detect_years_from_header(header: List[str]) -> List[tuple]:
    """
    Recebe header do arquivo e identifica colunas que representam ano(s).
    Retorna lista de (col_index, year, meta) onde meta pode conter 'sexo' se deduzido.
    Ex: header item '...Sexo/Feminino 2024 (Matrículas)' -> year 2024, sexo 'Feminino'
    """
    year_cols = []
    year_regex = re.compile(r'(?P<year>20\d{2})')
    for i, h in enumerate(header):
        m = year_regex.search(h)
        if m:
            year = int(m.group('year'))
            # tentar achar sexo na string
            sex = None
            if re.search(r'feminino|female|fem', h, re.I):
                sex = 'F'
            elif re.search(r'masculino|male|masc', h, re.I):
                sex = 'M'
            year_cols.append((i, year, sex, h))
    return year_cols

def etl_transform(in_path: str, out_path: str, delimiter=',', quotechar='"'):
    """
    Lê CSV original, detecta colunas de ano e converte para formato longo:
    municipio, ibge, latitude, longitude, year, sexo, quantidade
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(in_path, newline='', encoding='cp1252') as fin:
        # Pula a primeira linha do CSV, que contém caracteres inválidos.
        next(fin)
        reader = csv.reader(fin, delimiter=delimiter, quotechar=quotechar)
        header = next(reader)
        year_cols = detect_years_from_header(header)

        # achar índices previsíveis para municipio, ibge, lat, long
        # tenta identificar pelas palavras no header
        def find_col(possible_keywords):
            for i, h in enumerate(header):
                for k in possible_keywords:
                    if k.lower() in h.lower():
                        return i
            return None

        idx_mun = find_col(['município', 'municipio', 'municipio/']) or 0
        idx_ibge = find_col(['ibge', 'codigo ibge', 'codigo_ibge'])
        idx_lat = find_col(['lat', 'latitude'])
        idx_lon = find_col(['lon', 'longitude'])

        with open(out_path, 'w', newline='', encoding='utf-8') as fout:
            writer = csv.writer(fout)
            writer.writerow(['municipio', 'ibge', 'latitude', 'longitude', 'year', 'sexo', 'quantidade'])
            for row in reader:
                # proteção contra linhas curtas
                if len(row) < 1:
                    continue
                municipio_raw = row[idx_mun] if idx_mun is not None and idx_mun < len(row) else ""
                municipio = " ".join(municipio_raw.split())
                ibge = row[idx_ibge] if idx_ibge is not None and idx_ibge < len(row) else ""
                lat = row[idx_lat] if idx_lat is not None and idx_lat < len(row) else ""
                lon = row[idx_lon] if idx_lon is not None and idx_lon < len(row) else ""

                for (col_idx, year, sex, raw_header) in year_cols:
                    if col_idx >= len(row):
                        continue
                    raw_val = row[col_idx]
                    val = parse_number_robust(raw_val)
                    if val is None:
                        continue
                    sex_final = sex if sex is not None else ''  # pode ficar vazio se não detectado
                    writer.writerow([municipio, ibge, lat, lon, year, sex_final, val])

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ETL CSV -> long clean CSV")
    parser.add_argument("infile", help="arquivo CSV bruto")
    parser.add_argument("outfile", help="arquivo CSV limpo (long)")
    args = parser.parse_args()
    etl_transform(args.infile, args.outfile)