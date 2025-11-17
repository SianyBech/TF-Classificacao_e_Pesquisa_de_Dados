# src/utils.py
import unicodedata
import re

def normalize_text(s: str) -> str:
    """Lowercase, strip, remove accents and collapse whitespace."""
    if s is None:
        return ""
    s = s.strip().lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r'\s+', ' ', s)
    return s

def parse_number_robust(token: str):
    """
    Tenta converter strings numéricas que podem usar '.' como separador de milhares e ',' como decimal.
    Estratégia:
      - Se contém '.' e ',' -> assume '.' milhares e ',' decimal: remove '.' e substitui ',' por '.'
      - Se contém ',' e não '.' -> substitui ',' por '.'
      - Caso contrário, tenta float direto.
    Retorna int quando número for inteiro (sem parte fracionária), senão float.
    Se falhar, retorna None.
    """
    if token is None:
        return None
    t = str(token).strip()
    if t == '':
        return None
    t = t.replace('"', '').replace("'", "")
    # limpar espaços
    t = t.strip()
    # tratar valores como "1.234" (pode ser milhar) ou "1,234" (decimal)
    if t.count('.') > 0 and t.count(',') > 0:
        t2 = t.replace('.', '').replace(',', '.')
    elif t.count(',') > 0 and t.count('.') == 0:
        t2 = t.replace(',', '.')
    elif t.count('.') > 0 and t.count(',') == 0:
        t2 = t.replace('.', '')
    else:
        t2 = t
    # remover possíveis milhares com espaços
    t2 = t2.replace(' ', '')
    # se ainda tiver caracteres não numericos (exceto - and .), remove
    t2 = re.sub(r'[^0-9\.\-]', '', t2)
    try:
        f = float(t2)
        if abs(f - int(f)) < 1e-9:
            return int(round(f))
        return f
    except:
        return None