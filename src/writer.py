# src/writer.py
import struct
import os
import pickle
from typing import Dict, List

# Definimos um formato de registro fixo simples:
# - ibge: unsigned int (4 bytes)
# - year: unsigned short (2 bytes)
# - sexo: 1 char (1 byte) -> use 'M','F' ou ' ' (space)
# - quantidade: double (8 bytes) -> permite grandes valores ou decimais
# - offset para nome de município (não: aqui armazenamos nome fixo 64 bytes)
# - municipio: 64 bytes (utf-8 padded)
# - latitude: double (8 bytes)
# - longitude: double (8 bytes)
#
# Total RECORD_SIZE calculado dinamicamente.
import math

MUN_LEN = 64
RECORD_STRUCT = f"I H c d {MUN_LEN}s d"
RECORD_SIZE = struct.calcsize(RECORD_STRUCT)

def pad_mun_bytes(s: str, n=MUN_LEN) -> bytes:
    b = s.encode('utf-8')[:n]
    return b + b' '*(n - len(b))

def write_from_clean_csv(csv_path: str, bin_path: str, index_path: str):
    """
    Lê CSV limpo (long format) e escreve registros no arquivo binário.
    Também cria índice simples: municipio_normalizado -> list offsets
    """
    import csv
    from src.utils import normalize_text
    os.makedirs(os.path.dirname(bin_path), exist_ok=True)
    index = {}
    with open(csv_path, newline='', encoding='utf-8') as fin, open(bin_path, 'wb') as fout:
        reader = csv.DictReader(fin)
        for row in reader:
            ibge = int(row['ibge']) if row.get('ibge') and row['ibge'] != '' else 0
            year = int(row['year'])
            sex = row.get('sexo','').strip()[:1].upper() or ' '
            q = float(row.get('quantidade', 0))
            municipio = row.get('municipio','')
            try:
                lat = float(row.get('latitude') or 0.0)
            except:
                lat = 0.0
            try:
                lon = float(row.get('longitude') or 0.0)
            except:
                lon = 0.0

            offset = fout.tell()
            packed = struct.pack(RECORD_STRUCT,
                                  ibge,
                                  year,
                                  sex.encode('utf-8'),
                                  q,
                                  pad_mun_bytes(municipio),
                                  lat)
            # note: RECORD_STRUCT had only one double at end - adjust unpack accordingly
            # For clarity, we'll append longitude as 8 bytes after pack
            fout.write(packed)
            fout.write(struct.pack("d", lon))

            # atualizar índice
            key = normalize_text(municipio)
            index.setdefault(key, []).append(offset)

    # salvar índice
    with open(index_path, 'wb') as fidx:
        pickle.dump(index, fidx)

def read_record_at(bin_path: str, offset: int):
    """
    Lê registro no offset fornecido e retorna dict.
    """
    import struct
    with open(bin_path, 'rb') as f:
        f.seek(offset)
        data = f.read(RECORD_SIZE)
        # Unpack fields
        ibge, year, sex_b, quantidade, mun_b, lat = struct.unpack(RECORD_STRUCT, data)
        # longitude está logo após os bytes lidos
        lon_b = f.read(8)
        if len(lon_b) == 8:
            lon = struct.unpack("d", lon_b)[0]
        else:
            lon = 0.0
        return {
            "ibge": ibge,
            "year": year,
            "sexo": sex_b.decode('utf-8').strip(),
            "quantidade": quantidade,
            "municipio": mun_b.decode('utf-8').strip(),
            "latitude": lat,
            "longitude": lon
        }

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_clean")
    parser.add_argument("bin_out")
    parser.add_argument("index_out")
    args = parser.parse_args()
    write_from_clean_csv(args.csv_clean, args.bin_out, args.index_out)