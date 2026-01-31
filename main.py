import downloader as dl
import config as cfg
import os
import zipfile
import process_files as pf
import pprint as pp


def main():


    files = dl.get_latest_year(cfg.BASE_URL)
    dl.download_files(files, cfg.MIN_FILES)
    dl.extract_files()
    lista = pf.get_file_paths(cfg.MIN_FILES)
    print("Arquivos a serem processados")

    for file in lista:
        pp.pprint(file["file_path"])

if __name__ == "__main__":
    main()