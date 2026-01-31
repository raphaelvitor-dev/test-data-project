import os
from bs4 import BeautifulSoup
import requests
import zipfile



# Trata a URL para entrar no ano mais recente da base de dados:
def get_latest_year(url):
    try:
        #Aqui, fazemos a requisição GET na api, e se o status http for 200, começamos nossa lógica
        response = requests.get(url)
        if response.status_code == 200:
            print("Success! (GET URL)")

            # Aqui, realizamos o parse pelo HTML (o retorno de response transformado em texto), utilizando o BeautifulSoup, para listar todos os links da tag <a/> em HTML.
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a")
            links_by_year = []

            #Pegamos então, cada um dos atributos href das tags <a/>

            for link in links:
                href = link.get("href")

                #Aqui, filtramos o nome do href pela condição: Começa com 4 dígitos e tem uma barra no final, para capturar o ano de nossa consulta

                if href and href.endswith("/") and href[:4].isdigit():
                    links_by_year.append(href)

            #O ano é igual ao href capturado, porém removemos a barra no final com rstrip para href na lista "links_by_year", juntamente com o maior ano na lista

            year_int = [int(year.rstrip('/')) for year in links_by_year]
            max_year = max(year_int)

            #Aqui tratamos a URL que iremos começar nossa consulta adicionando o ano mais recente ao final
            max_href = f"{max_year}/"
            year_url = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/" + max_href

            return year_url

        #Aqui, tratei de forma mais genérica o erro que aparecer para verificar o status HTTP e debugar mais facilmente e verificar o que retorna.
        else:
            print("Error! Check the status code: " + str(response.status_code))
            return None

    # Da mesma forma, nesse trecho.
    except Exception as e:
        print(f"ERROR: {e}")
        return None


# =======================================================#

#Começo a função declarando-a com dois parâmetros, a url base de onde estão os anos a serem consultados, e o mínimo de trimestres que devem ser consultados. No caso serão 3.
def download_files(files_url, min_files):
    try:
        '''
        Nesse bloco, pego o ano mais recente, em formato de string, o ano recente em formato de int para operações,
        o máximo de anos para gerar uma condição de parada em um loop while posterior e um contador "decrease_year" para iterar, caso o ano desejado
        tenham menos de 3 trimestres, voltando um ano antes e completando nossa busca.
        
        PS:
        Preferi tratar a função para mais de 3 trimestres, caso desejemos consultar mais dados (A longo, médio e curto prazo) e facilitar a manutenção.
        '''

        year_str = files_url.rstrip("/").split("/")[-1]
        current_year = int(year_str)
        MAX_BACK_YEARS = 10
        CHUNK_SIZE = 1024 * 1024
        decrease_year = 1

        # Aqui, fazemos a requisição GET na api, e se o status http for 200, começamos nossa lógica
        response = requests.get(files_url)
        zip_files = []
        if response.status_code == 200:
            print("Success!")

            # Parse no HTML novamente, para capturarmos as tags <a/>
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a")

            '''
            Aqui, salvamos de trás para frente os hrefs na lista links, pois o BeautifulSoup salva em ordem HTML (ex:1,2,3,4), e os arquivos de trimestres mais recentes
            são os últimos (ex: 4,3,2,1).
            '''
            for link in reversed(links):
                href = link.get("href")
                if href and href.endswith(".zip") and len(zip_files) < min_files:

                    '''Nesse bloco, salvo os arquivos em formato de dict, pois precisava parsear mais adiante, o ano e a url dinamicamente para criar os diretórios no pc.
                       a chave "year" é o nome do diretório.
                    '''
                    zip_files.append({
                        "url": files_url + href,
                        "year": current_year
                    })



        '''
        Aqui, caso um ano ainda não tenha 3 trimestres, vamos fazer uma requisição para o ano anterior
        até completarmos o número de trimestres desejados para consulta.
        '''
        while len(zip_files) < min_files and decrease_year <= MAX_BACK_YEARS:

            BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

            previous_year = int(year_str) - decrease_year
            previous_url = f"{BASE_URL}{str(previous_year)}/"
            response = requests.get(previous_url)

            #Aqui segue a mesma lógica, se o status da requisição não for 200, lançamos um erro e encerramos.
            if response.status_code != 200:
                raise Exception(f"Error! {response.status_code}")

            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a")

            '''
            A pasta ano pode existir, mas ainda sim não ter arquivos dentro. Esse boolean foi criado para verificar essa condição,
            e caso seja falso, voltamos o ano anterior novamente para completar a consulta
            '''

            found_zip = False

            '''
            Aqui, ainda salvamos os itens de forma reversa em links para capturarmos os mais recentes, filtrar os arquivos que tenham valor
            no href, terminam com .zip e caso a lista zip_files ainda não tenha o número necessários de arquivos, salvando o ano prévio até completar
            a consulta.
            '''
            for link in reversed(links):
                href = link.get("href")
                if href and href.endswith(".zip") and len(zip_files) < min_files:
                    zip_files.append({
                        "url": previous_url + href,
                        "year": previous_year,
                    })
                    found_zip = True
            if not found_zip:
                continue
            # No final do loop, caso necessário adicionamos 1 ao decrementador para percorrer os anos anteriores.
            decrease_year += 1

        print("Downloading items...")
        for url_item in zip_files:

            #Caso o item capturado seja o ano atual, utilizamos a variável current_year. Senão, utilizamos o year informado no dict "zip_files"
            if url_item["year"] == current_year:
                year_folder = current_year
            else:
                year_folder = url_item["year"]
            '''
            Aqui verificamos se o diretório Trimestres/Ano/NomedoTrimestre existe. Senão criamos. O trimestre está no formato QQ,
            então dividimos a url pelas "/", capturamos o nome do arquivo que por convenção está no formato ex: 1T2023, e capturamos
            os dois primeiros caracteres para criar o nome do diretório abaixo:
            '''
            os.makedirs(
                os.path.join("Trimestres", str(year_folder), url_item["url"].split("/")[-1][:2]), exist_ok=True
            )

            #O nome do arquivo é capturado pela url
            filename = url_item["url"].split("/")[-1]
            file_path = os.path.join("Trimestres", str(year_folder), filename[:2], filename)

            '''
            Aqui é a parte do download. Baixamos dinamicamente percorrendo os items da lista com a url, ativamos o stream=True
            para controlar a memória do download e escrevemos os binários dos arquivos no diretório file_path, encerrando assim
            o programa downloader.
            '''
            response = requests.get(url_item["url"], stream=True)

            if response.status_code != 200:
                raise Exception("Error!" + str(response.status_code))


            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)

        response = requests.get("https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv")
        if response.status_code != 200:
            raise Exception("Error!" + str(response.status_code))

        #Baixa a lista de operadoras ativas
        csv_path = os.path.join("Trimestres", "Relatorio_cadop.csv")

        with open(csv_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)

        print("Download Successful!")
        return None
    except Exception as e:
        print(f"Error! {e} ")
        return None


# ===================================================================#

def extract_files():
    base_folder = "Trimestres"

    '''Criamos uma estrutura para percorrer apenas 
       as pastas e subpastas, encontrar os arquivos que terminam com .zip, e extrai-los na pasta do
       trimestre'''

    for year in os.listdir(base_folder):
        year_folder = os.path.join(base_folder, year)
        if not os.path.isdir(year_folder):
            continue

        for quarter in os.listdir(year_folder):
            quarter_folder = os.path.join(year_folder, quarter)

            if not os.path.isdir(quarter_folder):
                continue

            for file in os.listdir(quarter_folder):
                if file.lower().endswith(".zip"):
                    zip_archive = os.path.join(quarter_folder, file)

                    with zipfile.ZipFile(zip_archive, "r") as zip_ref:
                        print("Extracting files...")
                        zip_ref.extractall(quarter_folder)

files = get_latest_year("https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/")
download_files(files, 15)
extract_files()


