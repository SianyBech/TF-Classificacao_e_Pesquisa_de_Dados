# process_data.py
import os
import glob
from src.etl import etl_transform
from src.writer import write_from_clean_csv

def main():
    """
    Script para orquestrar o ETL e a indexação dos dados de matrículas.
    """
    # Diretórios
    DATA_RAW_DIR = 'data_raw'
    DATA_CLEAN_DIR = 'data_clean'
    BIN_FILES_DIR = 'bin_files'

    # Arquivos de entrada e saída
    raw_files = glob.glob(os.path.join(DATA_RAW_DIR, 'dee-*.csv'))
    clean_csv_path = os.path.join(DATA_CLEAN_DIR, 'matriculas_clean.csv')
    bin_path = os.path.join(BIN_FILES_DIR, 'matriculas.dat')
    index_path = os.path.join(BIN_FILES_DIR, 'index_mun.pkl')

    # 1. Executa o ETL
    print(f"Iniciando ETL dos arquivos: {raw_files}")
    etl_transform(raw_files, clean_csv_path)
    print(f"ETL concluído. Arquivo limpo salvo em: {clean_csv_path}")

    # 2. Cria o arquivo binário e o índice
    print("Iniciando a criação do arquivo binário e do índice...")
    write_from_clean_csv(clean_csv_path, bin_path, index_path)
    print("Processo de indexação concluído.")
    print(f"Arquivo binário salvo em: {bin_path}")
    print(f"Índice salvo em: {index_path}")

if __name__ == "__main__":
    main()