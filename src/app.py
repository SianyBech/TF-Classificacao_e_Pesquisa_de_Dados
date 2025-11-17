from flask import Flask, render_template, request
from src.indexer import load_index, search_by_municipio

app = Flask(__name__, template_folder='../templates')

# Carregar o índice e os dados na inicialização
BIN_PATH = 'bin_files/matriculas.dat'
INDEX_PATH = 'bin_files/index_mun.pkl'
index = load_index(INDEX_PATH)

@app.route('/')
def index_page():
    return render_template('index.html')

@app.route('/search')
def search():
    municipio = request.args.get('municipio', '')
    ano_inicio = request.args.get('ano_inicio', type=int)
    ano_fim = request.args.get('ano_fim', type=int)
    sexo = request.args.get('sexo', '')

    if not municipio:
        return render_template('results.html', error="O nome do município é obrigatório.")

    results, total = search_by_municipio(
        BIN_PATH,
        index,
        municipio,
        year_from=ano_inicio,
        year_to=ano_fim,
        sexo=sexo
    )

    return render_template('results.html',
                           results=results,
                           total=total,
                           municipio=municipio,
                           ano_inicio=ano_inicio,
                           ano_fim=ano_fim,
                           sexo=sexo)

if __name__ == '__main__':
    app.run(debug=True)
