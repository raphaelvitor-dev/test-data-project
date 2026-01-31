import pandas as pd
import os
import config as cfg
import pprint


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

    for i in year_dict:
        if i["file_path"].endswith(".csv"):
            df = pd.read_csv(i["file_path"], sep=",")

        elif i["file_path"].endswith(".xlsx"):
            df = pd.read_excel(i["file_path"])

        elif i["file_path"].endswith(".txt"):
            df = pd.read_csv(i["file_path"], sep="\t")
