# Documentação do Sistema de Consulta de Matrículas

Este documento descreve a arquitetura e o funcionamento interno do sistema de consulta de matrículas, detalhando o fluxo de dados desde os arquivos brutos até a apresentação na interface web.

## Visão Geral

O objetivo do sistema é fornecer uma maneira rápida e eficiente de consultar dados históricos de matrículas escolares, filtrando por município, período e sexo. Para alcançar a performance desejada, o sistema não consulta os arquivos CSV diretamente a cada busca. Em vez disso, ele pré-processa os dados brutos em uma estrutura otimizada para leitura rápida.

O processo é dividido em duas etapas principais:
1.  **Processamento e Indexação de Dados**: Uma etapa offline, executada uma única vez (ou sempre que os dados brutos forem atualizados), que prepara os dados para consulta.
2.  **Aplicação Web**: Um servidor web que utiliza os dados pré-processados para responder rapidamente às consultas do usuário.

---

## Fluxo de Dados e Processamento

Este fluxo é orquestrado pelo script `process_data.py`, que automatiza todas as etapas de preparação dos dados.

### Etapa 1: ETL (Extração, Transformação e Carga)

-   **Responsável**: `src/etl.py`
-   **Entrada**: Arquivos CSV brutos no formato "wide" (ex: `dee-5406.csv`, `dee-5407.csv`) localizados na pasta `data_raw/`.
-   **Saída**: Um único arquivo CSV limpo (`data_clean/matriculas_clean.csv`) no formato "long".

**Como funciona:**
1.  **Extração**: O script lê múltiplos arquivos CSV da pasta `data_raw/`. Ele é projetado para lidar com o encoding `cp1252`, comum em arquivos governamentais brasileiros, e para ignorar a primeira linha que pode conter caracteres inválidos.
2.  **Transformação**:
    *   **Detecção de Colunas**: O cabeçalho de cada arquivo é analisado para identificar automaticamente as colunas que contêm dados anuais. Ele extrai o ano e também o sexo (Masculino/Feminino) a partir do nome da coluna.
    *   **Pivotagem (Wide para Long)**: Os dados são transformados do formato "wide" (onde cada ano é uma coluna) para o formato "long" (onde há uma linha para cada combinação de município, ano e sexo). Isso torna a filtragem e a agregação de dados muito mais simples.
    *   **Cálculo de Variação**: Durante a transformação, o script calcula a variação no número de matrículas em relação ao ano anterior para cada município e sexo, classificando-a como `aumento`, `diminuicao`, `estavel` ou `novo`.
3.  **Carga**: O resultado é salvo em um único arquivo CSV (`matriculas_clean.csv`), que servirá de base para a próxima etapa.

### Etapa 2: Indexação e Binarização

-   **Responsável**: `src/writer.py`
-   **Entrada**: O arquivo CSV limpo (`data_clean/matriculas_clean.csv`).
-   **Saída**:
    1.  `bin_files/matriculas.dat`: Um arquivo binário contendo todos os registros.
    2.  `bin_files/index_mun.pkl`: Um arquivo de índice.

**Como funciona:**
1.  **Binarização**: O script lê o `matriculas_clean.csv` linha por linha e converte cada uma em um **registro de tamanho fixo**. Cada campo (IBGE, ano, sexo, quantidade, etc.) é empacotado em um formato binário. Esses registros são escritos sequencialmente no arquivo `matriculas.dat`.
    *   **Vantagem**: Acessar dados em um arquivo binário de tamanho fixo é extremamente rápido. Em vez de ler e interpretar o arquivo inteiro, o sistema pode pular diretamente para a posição exata (offset) de um registro desejado.

2.  **Criação do Índice**:
    *   Enquanto escreve o arquivo binário, o script armazena a posição (offset) de cada registro.
    *   Ele cria um dicionário em memória (um índice) que mapeia o nome normalizado de cada município a uma lista de offsets. Por exemplo: `{'porto_alegre': [0, 176, 352, ...]}`.
    *   Ao final, esse dicionário é salvo no arquivo `index_mun.pkl` usando a biblioteca `pickle` do Python.

---

## Aplicação Web (Flask)

A aplicação web é responsável por receber as requisições do usuário, buscar os dados e apresentar os resultados.

-   **Responsável**: `src/app.py` (controlador principal), `src/indexer.py` (lógica de busca).

### Como funciona uma consulta:

1.  **Requisição do Usuário**: O usuário acessa a página, seleciona um município, um intervalo de anos e um sexo, e clica em "Buscar". O navegador envia uma requisição para o endpoint `/search` do servidor Flask.

2.  **Carregamento do Índice**: Ao ser iniciado, o servidor Flask carrega o arquivo de índice (`index_mun.pkl`) para a memória e o mantém disponível para todas as requisições.

3.  **Busca no Índice**:
    *   O `app.py` recebe os parâmetros da consulta (município, anos, sexo).
    *   Ele chama as funções em `src/indexer.py`, passando o nome do município.
    *   O `indexer.py` normaliza o nome do município (remove acentos, converte para minúsculas) e usa o índice em memória para obter a lista de todos os offsets de registro para aquele município.

4.  **Leitura do Arquivo Binário**:
    *   Com a lista de offsets, o `indexer.py` abre o arquivo binário `matriculas.dat`.
    *   Para cada offset, ele vai diretamente para aquela posição no arquivo e lê o número exato de bytes correspondente a um registro.
    *   Os bytes lidos são desempacotados de volta para os campos originais (ano, sexo, quantidade, etc.).

5.  **Filtragem e Cálculo**:
    *   Os registros recuperados são então filtrados em memória para corresponder ao intervalo de anos e ao sexo especificados pelo usuário.
    *   A função `calculate_enrollment_difference` é chamada para calcular a diferença total de matrículas entre o ano inicial e o final do período solicitado.

6.  **Renderização da Resposta**:
    *   O `app.py` recebe os dados calculados.
    *   Ele renderiza o template HTML `results.html`, injetando os dados nos locais apropriados.
    *   O HTML final é enviado de volta ao navegador do usuário, que exibe a página de resultados.

Este design garante que as consultas sejam muito rápidas, pois evitam a leitura e interpretação de arquivos de texto grandes a cada requisição, utilizando em vez disso acesso direto a um arquivo binário otimizado.