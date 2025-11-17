# Sistema de Consulta de Matrículas

Este projeto é uma aplicação web para consultar dados de matrículas de estudantes, com filtros por município, ano e sexo.

## Instruções de Uso

Siga os passos abaixo para configurar e executar a aplicação em seu ambiente local.

### 1. Clone o Repositório

```bash
git clone https://github.com/SianyBech/TF-Classificacao_e_Pesquisa_de_Dados.git
cd TF-Classificacao_e_Pesquisa_de_Dados
```

### 2. Crie e Ative um Ambiente Virtual

É recomendado utilizar um ambiente virtual para isolar as dependências do projeto.

```bash
# Criar o ambiente virtual
python -m venv venv

# Ativar o ambiente (Windows)
.\venv\Scripts\activate
```

### 3. Instale as Dependências

Com o ambiente virtual ativado, instale as bibliotecas necessárias.

```bash
pip install -r requirements.txt
```

### 4. Processe os Dados Brutos

Este passo é crucial. Ele converte os arquivos CSV da pasta `data_raw` para o formato binário otimizado que a aplicação utiliza para as consultas.

```bash
python process_data.py
```

Você verá uma saída indicando que o ETL e a indexação foram concluídos.

### 5. Execute a Aplicação

Agora, inicie o servidor da aplicação Flask.

```bash
python -m src.app
```

A aplicação estará disponível no seu navegador no endereço: [http://127.0.0.1:5000](http://127.0.0.1:5000)