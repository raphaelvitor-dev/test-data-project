import downloader as dl
import config as cfg
import os
import zipfile
import process_files as pf
import pprint as pp



def main():

    if not os.path.exists("Trimestres"):
        files = dl.get_latest_year(cfg.BASE_URL)
        dl.download_files(files, cfg.MIN_FILES)
        dl.extract_files()
        lista = pf.get_file_paths(cfg.MIN_FILES)
        pf.check_files(lista)
        print("Processing Files...")
        pf.process_quarter_data_csv(lista)
        pf.process_registrations()



if __name__ == "__main__":
    main()