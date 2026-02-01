import pandas as pd
import os
import config as cfg
import pprint as pp
from decimal import Decimal


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

                        if file.endswith(".csv"):
                            file_path = os.path.join(quarter_path, file)
                            year_dict.append({
                                "year": year,
                                "quarter": quarter,
                                "file_path": file_path
                            })



    return year_dict

#================================================================#

def proccess_data(year_dict):




    dfs = []
    handled_dfs = []
    for i in year_dict:
        if i["file_path"].endswith(".csv"):

            dfs.append(pd.read_csv(i["file_path"], sep=";"))



    for i, df in enumerate(dfs):

        #Exclui colunas vazias
        df.dropna(axis=1, how="all")

        #Transforma os nomes da coluna para minusculo
        df.columns = df.columns.str.strip().str.lower()

        #retira os caracteres "_" dos nomes
        df.columns = df.columns.str.replace("_", "")

        #Passa valores financeiros para decimal para maior precisão e remove a ","
        df["vlsaldoinicial"] = (
            df["vlsaldoinicial"]
            .str.replace("R\\$ ?", "", regex=True)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["vlsaldofinal"] = (
            df["vlsaldofinal"]
            .str.replace("R\\$ ?", "", regex=True)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["vlsaldoinicial"] = df["vlsaldoinicial"].apply(Decimal)
        df["vlsaldofinal"] = df["vlsaldofinal"].apply(Decimal)


        #Cria colunas requisitadas
        df["valordespesas"] = df["vlsaldoinicial"] - df["vlsaldofinal"]
        df["cnpj"] = ""
        df["razaosocial"] = ""
        df["ano"] = year_dict[i]["year"]
        df["trimestre"] = year_dict[i]["quarter"]


        handled_dfs.append(df)
    print(handled_dfs[3]["trimestre"])






df_pathlist = get_file_paths(cfg.MIN_FILES)
proccess_data(df_pathlist)


