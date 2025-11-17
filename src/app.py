from flask import Flask, render_template, request
import csv
from src.indexer import load_index, search_by_municipio, calculate_enrollment_difference

app = Flask(__name__, template_folder='../templates')

# Carregar o índice e os dados na inicialização
BIN_PATH = 'bin_files/matriculas.dat'
INDEX_PATH = 'bin_files/index_mun.pkl'
index = load_index(INDEX_PATH)

@app.route('/')
def index_page():
    municipios = set()
    try:
        # Lê os municípios diretamente do arquivo CSV bruto, conforme solicitado.
        # Usa encoding 'cp1252' que é comum em arquivos CSV governamentais no Brasil.
        with open('data_raw/dee-5406.csv', 'r', encoding='cp1252', newline='') as f:
            # O arquivo CSV fornecido tem um caractere inválido na primeira linha, então pulamos ela.
            next(f)
            reader = csv.reader(f)
            # Pulamos a linha do cabeçalho.
            next(reader)
            # Adiciona cada município a um set para garantir valores únicos.
            for row in reader:
                if row:
                    municipios.add(row[0])
    except (FileNotFoundError, StopIteration):
        # Em caso de erro na leitura do arquivo, a lista de municípios ficará vazia.
        pass
    return render_template('index.html', municipios=sorted(list(municipios)))

@app.route('/search')
def search():
    municipio = request.args.get('municipio', '')
    ano_inicio = request.args.get('ano_inicio', type=int)
    ano_fim = request.args.get('ano_fim', type=int)
    sexo = request.args.get('sexo', '')

    if not municipio:
        return render_template('results.html', error="O nome do município é obrigatório.")

    # Calcula a diferença de matrículas
    diff_data = calculate_enrollment_difference(
        BIN_PATH,
        index,
        municipio,
        year_from=ano_inicio,
        year_to=ano_fim,
        sexo=sexo
    )

    return render_template('results.html',
                           diff_data=diff_data,
                           municipio=municipio,
                           ano_inicio=ano_inicio,
                           ano_fim=ano_fim,
                           sexo=sexo)

if __name__ == '__main__':
    app.run(debug=True)

# Forçar recarregamento do servidor
