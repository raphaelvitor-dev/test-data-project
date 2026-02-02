import pandas as pd
import os
import pyarrow.parquet as pq
import pyarrow as pa
import config as cfg
from decimal import Decimal
import zipfile

from config import chunksize


def get_file_paths(min_files):
    base_dir = "Trimestres"
    year_dict = []

    while len(year_dict) < min_files:
        years = sorted([y for y in os.listdir(base_dir) if y.isdigit()], reverse=True)

        # PREPARA A LISTA DE DIRETÓRIOS POR ANO E TRIMESTRE
        for year in years:
            if len(year_dict) >= min_files:
                break
            year_path = os.path.join(base_dir, year)
            quarters = sorted([q for q in os.listdir(year_path)], reverse=True)

            for quarter in quarters:

                if len(year_dict) >= min_files:
                    break

                if os.path.isdir(os.path.join(year_path, quarter)):
                    quarter_path = os.path.join(year_path, quarter)

                    for file in os.listdir(quarter_path):
                        if len(year_dict) >= min_files:
                            break

                        if file.endswith(".csv") or file.endswith(".xlsx") or file.endswith(".txt"):
                            file_path = os.path.join(quarter_path, file)
                            year_dict.append({
                                "year": year,
                                "quarter": quarter,
                                "file_path": file_path
                            })



    return year_dict

#================================================================#

#Essa função verifica se existe a descrição solicitada no teste: "Despesas com Eventos/Sinistros"
def check_files(year_dict, chunksize=cfg.chunksize):
    for file in year_dict:
        file_path = file["file_path"]

        # Aqui, eu converto o arquivo excel para csv, para que o reader do pandas consiga processá-lo por chunks
        if file_path.endswith(".xlsx"):
            csv_path = file_path.replace(".xlsx", ".csv")

            if not os.path.exists(csv_path):
                pd.read_excel(file_path).to_csv(csv_path,sep=";",index=False)

            file_path = csv_path

        if file_path.endswith(".csv"):
            reader = pd.read_csv(file_path,sep=";",chunksize=chunksize)

        elif file_path.endswith(".txt"):
            reader = pd.read_csv(file_path,sep="\t",chunksize=chunksize)

        else:
            continue

        found = False


        for chunk in reader:
            chunk.columns = (chunk.columns.str.strip().str.lower().str.replace("_", ""))

            mask = chunk["descricao"].astype(str).str.contains(r"Despesas\s+com\s+Eventos\s*/\s*Sinistros",case=False,na=False,regex=True)

            if mask.any():
                found = True
                break

        if not found:
            os.remove(file_path)

#================================================================#


#Essa função lê os arquivos dos trimestres, e os junta em um arquivo final, por chunks para não estourar a memória.
def proccess_quarter_data_parquet(year_dict, chunksize=cfg.chunksize):
    parquet_path = os.path.join(
        "Trimestres",
        "consolidado_despesas.parquet"
    )

    #O schema da leitura por chunks, estava sendo alterado a cada chunk. Isso causa um bug no parquet writer, e interrompe o processo. Normalizando o schema, tudo volta ao normal.
    schema = pa.schema([
        ("data", pa.large_string()),
        ("regans", pa.int64()),
        ("cdcontacontabil", pa.int64()),
        ("descricao", pa.large_string()),
        ("vlsaldoinicial", pa.decimal128(15, 2)),
        ("vlsaldofinal", pa.decimal128(15, 2)),
        ("valordespesas", pa.decimal128(15, 2)),
        ("cnpj", pa.large_string()),
        ("razaosocial", pa.large_string()),
        ("ano", pa.large_string()),
        ("trimestre", pa.large_string()),
    ])

    parquet_writer = None

    try:

        for meta in year_dict:
            file_path = meta["file_path"]
            year = meta["year"]
            quarter = meta["quarter"]



            # Aqui, eu peço para o pandas ler o arquivo por partes (o chunksize definido no parametro da funcao lê de tantas em tantas linhas) senão a memória estoura
            if file_path.endswith(".csv"):
                reader = pd.read_csv(
                    file_path,
                    sep=";",
                    chunksize=chunksize
                )

            elif file_path.endswith(".txt"):
                reader = pd.read_csv(
                    file_path,
                    sep="\t",
                    chunksize=chunksize
                )

            else:
                continue

            for df in reader:
                # Primeiro removo as colunas vazias pois não há dados nelas.
                df = df.dropna(axis=1, how="all")

                # transformo os nomes das colunas em letra minuscula
                df.columns = (df.columns.str.strip().str.lower().str.replace("_", ""))




                # Padroniza os valores financeiros para que utilizem . ao invés da vírgula
                for col in ["vlsaldoinicial", "vlsaldofinal"]:
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.replace("R\\$ ?", "", regex=True)
                        .str.replace(".", "", regex=False)
                        .str.replace(",", ".", regex=False)
                        .fillna("0")
                        .apply(Decimal)
                    )

                # Cria as colunas requisitadas pelo teste. Como no arquivo dos trimestres ainda não tem cnpj e razaosocial, vamos tratar os dois no join com o arquivo de cadastro
                df["valordespesas"] = df["vlsaldoinicial"] - df["vlsaldofinal"]
                df["cnpj"] = ""
                df["razaosocial"] = ""
                df["ano"] = year
                df["trimestre"] = quarter
                df["descricao"] = df["descricao"].astype(str).str.lower()

                table = pa.Table.from_pandas(
                    df,
                    schema=schema,
                    preserve_index=False
                )

                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(
                        parquet_path,
                        schema,
                        compression="snappy"
                    )

                parquet_writer.write_table(table)

    #Aqui os recursos na memória são liberados e os dados pendentes são gravados
    finally:
        if parquet_writer:
            parquet_writer.close()

#========================================================================#

def proccess_quarter_data_csv(year_dict, chunksize=cfg.chunksize):
    output_path = os.path.join("Trimestres", "consolidado_despesas.csv")
    zip_path = os.path.join("Trimestres", "consolidado_despesas.zip")
    first_write = True


    for file in year_dict:
        file_path = file["file_path"]
        year = file["year"]
        quarter = file["quarter"]

        if file_path.endswith(".csv"):
            data_frame = pd.read_csv(file_path,sep=";",chunksize=chunksize)
        elif file_path.endswith(".txt"):
            data_frame = pd.read_csv(file_path,sep="\t",chunksize=chunksize)
        else:
            continue

        for df in data_frame:

                df = df.dropna(axis=1, how="all")

                df.columns = (
                    df.columns.str.strip()
                    .str.lower()
                    .str.replace("_", "")
                )

                for col in ["vlsaldoinicial", "vlsaldofinal"]:
                    df[col] = (
                        df[col].astype(str)
                        .str.replace("R\\$ ?", "", regex=True)
                        .str.replace(".", "", regex=False)
                        .str.replace(",", ".", regex=False)
                        .fillna("0")
                        .apply(Decimal)
                    )

                df["valordespesas"] = df["vlsaldoinicial"] - df["vlsaldofinal"]
                df["cnpj"] = ""
                df["razaosocial"] = ""
                df["ano"] = year
                df["trimestre"] = quarter
                df["descricao"] = df["descricao"].astype(str).str.lower()

                df.to_csv(output_path,sep=";",index=False,mode="w" if first_write else "a",header=first_write)

                first_write = False

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:

        zf.write(output_path, arcname="consolidado_despesas.csv")



#======================================================================#
def process_registrations():
    despesas_path = os.path.join("Trimestres", "consolidado_despesas.csv")
    registros_path = os.path.join("Trimestres", "Relatorio_cadop.csv")
    output_path = os.path.join("Trimestres", "despesas_agregadas.csv")

    df_despesas = pd.read_csv(despesas_path, sep=";")
    df_registro = pd.read_csv(registros_path, sep=";")

    #Normalização dos nomes das colunas
    df_registro.columns = df_registro.columns.str.strip().str.lower().str.replace("_", "")

    #Criação de colunas:

    #Normalizo os nomes para os arquivos serem compativeis no join
    df_registro = df_registro.rename(columns={
        "REGISTRO_OPERADORA" : "regans",
        "CNPJ" : "cnpj"
    })
    #transforma cnpjs (identificadores) em strings, pois caso o cnpj tenha 0 antes do numero int, seu valor muda completamente.
    df_despesas["cnpj"] = df_despesas["cnpj"].astype(str).str.replace(r"\D", "", regex=True)
    df_registro["cnpj"] = df_registro["cnpj"].astype(str).str.replace(r"\D", "", regex=True)

    # Join pelo CNPJ
    df_join = df_despesas.merge(
        df_registro,
        on="cnpj",
        how="left"
    )

    df_join.to_csv("consolidado_despesas_com_cadastro.csv", sep=";", index=False)











