import pandas as pd
import os
import pyarrow.parquet as pq
import pyarrow as pa
import config as cfg
from decimal import Decimal
import zipfile

from config import chunksize

#==========================================================#

def cnpj_valido(cnpj: str) -> bool:
    if not cnpj:
        return False

    cnpj = "".join(filter(str.isdigit, str(cnpj)))

    if len(cnpj) != 14:
        return False

    # elimina sequências inválidas
    if cnpj == cnpj[0] * 14:
        return False

    pesos1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    pesos2 = [6] + pesos1

    def calc_digito(cnpj, pesos):
        soma = sum(int(d) * p for d, p in zip(cnpj, pesos))
        resto = soma % 11
        return "0" if resto < 2 else str(11 - resto)

    dig1 = calc_digito(cnpj[:12], pesos1)
    dig2 = calc_digito(cnpj[:12] + dig1, pesos2)

    return cnpj[-2:] == dig1 + dig2

#==================================================#

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

def proccess_quarter_data_csv(year_dict, chunksize=cfg.chunksize):
    output_path = os.path.join("Trimestres", "consolidado_despesas.csv")
    groupby_path = os.path.join("Trimestres", "groupby")
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
                df["valor_suspeito"] = df["valordespesas"] < 0
                df["ano"] = year
                df["trimestre"] = quarter
                df["descricao"] = df["descricao"].astype(str).str.lower()
                df = df.rename(columns={"descricao": "descricaogastos"})

                df.to_csv(output_path,sep=";",index=False,mode="w" if first_write else "a",header=first_write)

                first_write = False

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:

        zf.write(output_path, arcname="consolidado_despesas.csv")
    if os.path.exists(output_path):
        os.remove(output_path)



#======================================================================#
def process_registrations():
    despesas_path = os.path.join("Trimestres", "consolidado_despesas.zip")
    registros_path = os.path.join("Trimestres", "Relatorio_cadop.csv")
    output_path = os.path.join("Trimestres", "despesas_agregadas.csv")
    groupby_path = os.path.join("Trimestres", "despesas_agregadas.csv")
    zip_path = os.path.join("Trimestres", "dados_totais.zip")

    #Lê os dataframes
    if despesas_path.endswith(".zip"):
        df_despesas = pd.read_csv(despesas_path, sep=";", compression="zip")
        df_registro = pd.read_csv(registros_path, sep=";")
    elif despesas_path.endswith(".txt"):
        df_despesas = pd.read_csv(despesas_path, sep="\t")
        df_registro = pd.read_csv(registros_path, sep="\t")
    else:
        df_despesas = pd.read_csv(despesas_path, sep=";")
        df_registro = pd.read_csv(registros_path, sep=";")

    #Tratamento do df registro

    df_registro.columns = df_registro.columns.str.strip().str.lower().str.replace("_", "")

    df_registro = df_registro.rename(columns={"registrooperadora": "regans"})

    # transforma cnpjs (identificadores) em strings, pois caso o cnpj tenha 0 antes do numero int, seu valor muda completamente.
    df_registro["cnpj"] = df_registro["cnpj"].astype(str).str.replace(r"\D", "", regex=True)

    #df_registro = df_registro[df_registro["cnpj"].apply(cnpj_valido)]
    df_registro = (df_registro.sort_values("regans").drop_duplicates(subset=["regans"], keep="first"))

    # Join pelo Registro Ans, pois nao ha cnpj no arquivo de trimestre gerado.
    df_join = df_despesas.merge(
        df_registro,
        on="regans",
        how="left"
    )

    df_join["cnpj"] = df_join["cnpj"].fillna("")
    df_join["cnpj"] = df_join["cnpj"].apply(
        lambda x: x if cnpj_valido(x) else "CNPJ INVÁLIDO"
    )


    df_join.to_csv(output_path, sep=";", index=False)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:

        zf.write(output_path, arcname="consolidado_despesas.csv")

    #Agrupa por Razão Social e UF.
    df_join["valordespesas"] = df_join["valordespesas"].astype(float)

    df_agrupado = df_join.groupby(["razaosocial", "uf"]).agg(
        totaldespesas=pd.NamedAgg(column="valordespesas", aggfunc="sum"),
        mediatrimestral=pd.NamedAgg(column="valordespesas", aggfunc="mean"),
        desviopadrao=pd.NamedAgg(column="valordespesas", aggfunc="std")
    ).reset_index()

    df_agrupado[["totaldespesas", "mediatrimestral", "desviopadrao"]] = (
        df_agrupado[["totaldespesas", "mediatrimestral", "desviopadrao"]].round(2)
    )

    #Ordena do Maior para o Menor
    df_agrupado = df_agrupado.sort_values(by="totaldespesas", ascending=False)
    df_agrupado.to_csv(groupby_path, sep=";", index=False)
    if os.path.exists(registros_path):
        os.remove(despesas_path)
        os.rename(zip_path, despesas_path)


















