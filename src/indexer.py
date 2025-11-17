# src/indexer.py
import pickle
from typing import List, Dict
from src.utils import normalize_text
from src.writer import read_record_at

def load_index(index_path: str) -> Dict[str, List[int]]:
    with open(index_path, 'rb') as f:
        return pickle.load(f)

def search_by_municipio(bin_path: str, index: Dict[str, list], municipio_query: str,
                        year_from=None, year_to=None, sexo=None, page=1, page_size=20):
    """
    Retorna uma página de resultados (lista de registros como dicts) e total_hits.
    """
    key = normalize_text(municipio_query)
    offsets = index.get(key, [])
    records = []
    for off in offsets:
        rec = read_record_at(bin_path, off)
        if year_from is not None and rec['year'] < year_from:
            continue
        if year_to is not None and rec['year'] > year_to:
            continue
        if sexo is not None and sexo != '' and rec['sexo'] != sexo:
            continue
        records.append(rec)
    # ordenar por year
    records.sort(key=lambda r: r['year'])
    total = len(records)
    start = (page-1)*page_size
    end = start + page_size
    return records[start:end], total