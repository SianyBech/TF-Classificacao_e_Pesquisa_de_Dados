# src/search_cli.py
import argparse
from src.indexer import load_index, search_by_municipio
from src.writer import read_record_at

def main():
    parser = argparse.ArgumentParser(description="Pesquisar matrículas por município (arquivo binário/index).")
    parser.add_argument("--bin", required=True, help="caminho para bin_files/matriculas.dat")
    parser.add_argument("--index", required=True, help="caminho para bin_files/index_mun.pkl")
    parser.add_argument("--municipio", required=True, help="nome do município (ex: Porto Alegre)")
    parser.add_argument("--ano-inicio", type=int, default=None)
    parser.add_argument("--ano-fim", type=int, default=None)
    parser.add_argument("--sexo", choices=['M','F'], default=None)
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--page-size", type=int, default=20)

    args = parser.parse_args()
    index = load_index(args.index)
    results, total = search_by_municipio(args.bin, index, args.municipio,
                                        year_from=args.ano_inicio, year_to=args.ano_fim,
                                        sexo=args.sexo, page=args.page, page_size=args.page_size)
    print(f"Total registros encontrados: {total}")
    print(f"Mostrando página {args.page} (tamanho {args.page_size}) => {len(results)} registros")
    for r in results:
        print(f"{r['municipio']} | IBGE {r['ibge']} | Ano {r['year']} | Sexo {r['sexo']} | Qtd {r['quantidade']}")

if __name__ == "__main__":
    main()