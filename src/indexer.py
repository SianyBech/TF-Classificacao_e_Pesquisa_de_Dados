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

def calculate_enrollment_difference(bin_path: str, index: Dict[str, list], municipio_query: str,
                                    year_from: int, year_to: int, sexo=None):
    """
    Calcula a diferença no número de matrículas entre dois anos.
    """
    key = normalize_text(municipio_query)
    offsets = index.get(key, [])
    
    records_from = []
    records_to = []

    for off in offsets:
        rec = read_record_at(bin_path, off)
        
        # Aplica filtro de sexo se especificado
        if sexo and rec['sexo'] != sexo:
            continue
            
        if rec['year'] == year_from:
            records_from.append(rec)
        
        if rec['year'] == year_to:
            records_to.append(rec)

    # Soma as quantidades para os anos de início e fim
    qty_from = sum(r['quantidade'] for r in records_from)
    qty_to = sum(r['quantidade'] for r in records_to)

    difference = qty_to - qty_from
    
    variation_percentage = 0
    if qty_from > 0:
        variation_percentage = (difference / qty_from) * 100
    elif difference > 0:
        # Se começou do zero e aumentou, a variação é "infinita", podemos mostrar 100% para simplificar
        variation_percentage = 100

    return {
        'year_from': year_from,
        'qty_from': qty_from,
        'year_to': year_to,
        'qty_to': qty_to,
        'difference': difference,
        'variation': variation_percentage
    }