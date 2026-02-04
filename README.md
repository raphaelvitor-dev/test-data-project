# test-data-project

## **Introdução**
Muito prazer, meu nome é **Raphael Vitor**, e irei explicar a lógica por trás do desenvolvimento do software.  
Adianto que o teste proposto, foi um ótimo projeto para meu aprendizado e nessa última semana, evolui muito nas tecnologias solicitadas. 
O código não é 100% perfeito, mas me esforcei ao máximo para me adaptar a stack solicitada, já que venho estudando muito o ecossistema .NET.
Priorizei o tratamento dos dados e o pipeline lógico para organização dos arquivos. Provavelmente não conseguirei completar a API e interface,  
porém caso consiga uma oportunidade, meu aprendizado será continuo e com máximo esforço.

## **Bibliotecas Utilizadas:**
1. **Requests:** Para fazer as requisições e acessar os dados na API pública
2. **BeautifulSoup4:** A resposta da API gera um HTML. Utilizamos BeautifulSoup4 para Parsear.
3. **OS:** Para manipulação do Sistema Operacional e Lógicas
4. **ZipFile:** Para compactar e extrair arquivos
5. **Pandas:** Para processar os dados dos arquivos recebidos na API
6. **Decimal:** Para tratar valores monetários caso necessário
7. **FastAPI:** Para criar endpoints bem definidos

## **COMO EXECUTAR O PROGRAMA**
1. Requer Python 3.14
2. Crie o ambiente virtual com python -m venv .venv
3. Execute .venv/Scripts/activate
4. Execute pip install -r requirements.txt
5. Execute python main.py
6. **ATENÇÃO** Para a criação das tabelas, altere o diretório nos 3 arquivos da pasta sql/03_import
7. Devido ao prazo, encerro por aqui o teste. A pasta api está incompleta pois comecei a desenvolvê-la e devido ao prazo optei por não continuar.


## Config.py
Para melhorar o desacoplamento, implementei variáveis globais, usadas como parâmetro em mais de uma função no arquivo
config.py. Adiante explico melhor o que cada variável faz.

## Downloader:
Antes de começarmos, baseei a lógica do programa em um schema presumido do DB.  
Os arquivos têm práticas rigorosas de nomeclatura no DataBase, então construi a minha lógica em cima disso.
  
Tentei deixar o programa o mais modular possível e dividi-lo em funções, separando as responsabilidades.  
  
Começando pelo downloader, analisei a estrutura de diretórios no FTP remoto da ANS.  
O diretório: https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/  
segue uma estrutura de pastas por ano, e dentro de cada ano possui um trimestre.  
  
A primeira função **get_latest_year(url)**, trata a url, e captura o ano mais recente no banco de dados criando a seguinte lógica: na lista de strings links_by_year[], para cada href que o bs4 capturar, tiver 4 digitos e terminar com "/",
armazenamos esse href a lista. Tratamos essa lista com outra lista de inteiros, transformando essas strings em inteiros para realizar os cálculos e expressões.  
Armazenamos o maior ano na variável max_year = max(year_int) e por fim, transformo novamente com a variável max_href = f"{max_year}/" para adicionar ao final do link,
o ano mais recente.  
**Essa função nos garante sempre o ano mais recente para as consultas.**

A segunda função, **download_files(files_url, min_files)**, faz uma requisição aos arquivos da seguinte maneira:   

Parsea com o BeautifulSoup4 todos os hrefs de arquivos, cria um dicionário, que salva os HREFS que terminam com .zip de trás para frente (Mais recente ao mais antigo) na lista zip_files[] por ano e url,  
e caso o número de elementos nessa lista seja menor que o parâmetro min_files, o parser volta 1 ano, diminuindo o número do ano na url e preenche com o número de arquivos que faltam.  
Isso garante que caso a consulta seja realizada no início do ano, por exemplo, consulte e baixe os trimestres anteriores também. Destaco que pensando num futuro hipotético desse software, caso o usuário  
queira consultar mais de 3 Trimestres, isso será possível alterando a variável min_files no arquivo de variáveis e lógica global config.py.  
Os arquivos baixados então são salvos no computador criando as pastas: Ano em que o arquivo atual de trimestre está e dentro o Trimestre atual(YYYY/QQ). Trato o nome da pasta trimestre, capturando o nome do arquivo atual,  
e renomeando a pasta com os primeiros 2 caracteres do nome do arquivo, formando, por exemplo, o formato QQ("3T").

A terceira função **extract_files** cria um parser entre as pastas e extrai cada um dos arquivos baixados.

Com o downloader construído dessa forma, enquanto os dados no DB tiverem essa nomeclatura, o downloader será resiliente a variações.

## Processador de dados (process_files):
A parte de processamento e normalização dos dados envolve tanto o teste 1.3 quanto o teste 2. O processamento e leitura dos dados, foi construido incrementalmente.  
Isso nos dá pelo menos um controle, evitando estouro de memória. O número de chunks (nesse contexto são as linhas) processados no programa, pode ser alterado no arquivo config.py.
Para manter o padrão do processamento, arquivos excel não suportam leitura por chunk. Então implementei uma lógica para convertê-los em csv, e após isso iniciar a leitura por chunk.
O formato de TRIMESTRES foi tratado no arquivo Downloader. As datas inconsistentes foram tratadas no comando pd.to_datetime. Utilizei DATE, porque o formato ja vem do DB dessa forma.

### TRATAMENTO CNPJ

Pesquisei sobre como o algoritmo do CNPJ funciona. Na internet é possivel achá-lo, então criei uma função que verifica se o CNPJ é válido ou não.
Optei por não excluir os CNPJs inválidos, mas sim no arquivo final, criar uma coluna chamada cnpj_inválido, que retorna um boolean True or False.
Dessa maneira, é possível realizar Queries, filtrando apenas CNPJs verdadeiros. Além disso, ordenei por data e desdupliquei os CNPJs, integrando o mais atual aos dados.

### DESPEPSAS AGREGADAS
Para gerar a tabela Despesas Agregadas, agrupei por Razão Social e UF, adicionando a tabela a Média, Desvio Padrão e Total de Despesas. Os valores estão com precisão de 2 casas decimais.
A transformação em Decimal foi Realizada nas QUERIES SQL, pois os métodos do pandas para gerar média, soma e desvio funcionam melhor com float.

## QUERIES SQL
Para as queries SQL para criação das tabelas, organizei em 01_DDL, 02_STAGING, 03_IMPORT, 04_INDEXES e 05_ANALYSIS, seguindo essa ordem. Para as QUERIES de análise mais complexa,
foi utilizado a ajuda da IA Claude, com revisão após o scaffolding do código. Para a Query 1 analítica, pressupõe-se que dados serão perdidos devido à ambiguidade. A Query 2 soma
as despesas das operadoras e por UF e mostra os estados com maiores despesas. A query 3 calcula a média geral, a média por trimestre, e filtra quantas operadoras ficaram com a média
acima dessa média trimestral. Como não deu tempo de desenvolver a API e Interface, sem scripts, as queries podem ser utilizadas direto no pgAdmin do postgresql.

## CONSIDERAÇÕES FINAIS
Com esse projeto, expandi os meus conhecimentos, a minha lógica de programação e aprendi conceitos não antes visto. Foi um verdadeiro desafio, ainda mais para quem estudava outra stack.
Dito isso, espero contribuir e me desenvolver caso receba uma oportunidade!








 


