import pandas as pd
import os


def read_files(min_files):
    base_dir = "Trimestres"
    years = []

    for year in sorted(os.listdir(base_dir), reverse=True):
        year_path = os.path.join(base_dir, year)

        if not os.path.isdir(year_path):
            continue

        quarters = []

        for quarter in sorted(os.listdir(year_path), reverse=True):
            quarter_path = os.path.join(year_path, quarter)

            if os.path.isdir(quarter_path):
                quarters.append(quarter)

        years.append({
            "year": year,
            "quarters": quarters
        })

    print(years)









read_files(None)
