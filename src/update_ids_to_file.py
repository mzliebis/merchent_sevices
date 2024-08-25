import warnings
import pandas as pd
warnings.simplefilter(action='ignore', category=FutureWarning)
import numpy as np
import sys, os
from pathlib import Path
from nameparser import HumanName
pd.set_option('display.max_columns', None)
pd.options.mode.chained_assignment = None  # default='warn'


def check_before_using_dict(column, dictionary, df):
    unique_column_values = df[column].dropna().unique()

    keys = dictionary.keys()
    missing = [i for i in unique_column_values if i not in keys]

    if bool(missing):
        print("*--------------Alert Missing Mapping-------------------------------------*")
        print(f"column: {column}")


        print(f"dict: {dictionary}")
        print(f"missing values: {missing}")
        sys.exit("")


def read_file(file_name, path):
    return pd.read_excel(path / file_name)

def read_file_csv(file_name, path):
    return pd.read_csv(path / file_name)





if __name__ == '__main__':

    # can modify



    path_out = Path(Path.cwd().parent / "out" )

    path_google_drive_dd_file = \
        Path("G:/My Drive/programming/reconciliation/dd_gift_ids.csv")

    path_google_drive_cc_file = \
        Path("G:/My Drive/programming/reconciliation/cc_gift_ids.csv")



    toggle = input("enter dd to append ids for direct debit:")
    if toggle == "dd":


        dd_merged = read_file_csv("dd_merged.csv", path_out)
        dd_merged = dd_merged[['Gift_Date_x', 'Gift_Date_y', 'Gift_ID']]

        dd_merged.to_csv(path_google_drive_dd_file, mode="a", index=False, header=False)
        print(dd_merged.to_markdown())

    toggle = input("enter cc to append ids for cc:")
    if toggle == "cc":
        cc_merged = read_file_csv("cc_merged.csv", path_out)
        cc_merged = cc_merged[['date_x', 'date_y', 'Gift_ID']]

        print(cc_merged.to_markdown())
        cc_merged.to_csv(path_google_drive_cc_file, mode = "a", index = False, header = False)
