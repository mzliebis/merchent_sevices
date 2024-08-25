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


    path_out = Path(Path.cwd().parent / "out" )

    # path_google_drive_dd_file = \
    #     Path("G:/My Drive/programming/reconciliation/dd_gift_ids.csv")
    #
    # path_google_drive_cc_file = \
    #     Path("G:/My Drive/programming/reconciliation/cc_gift_ids.csv")

    cc_df = read_file_csv("cc_final.csv", path_out)
    dd_df = read_file_csv("dd_final.csv", path_out)

    merge_df = cc_df.merge(dd_df, on=['Fund'], how='outer')

    print(cc_df.to_markdown())
    print(dd_df.to_markdown())

    merge_df = merge_df.fillna(0)
    merge_df['Gross']  =   merge_df['CC_Gross_Amount'] + merge_df['Gross_Amount']
    merge_df['Fees']  =   merge_df['CC_Fees'] + merge_df['DD_Fees']
    merge_df['Net']  =   merge_df['Gross'] + merge_df['Fees']

    # dict for fund name to code
    path_google_drive_data = Path("G:/My Drive/programming/data")
    temp = read_file_csv("fund_to_code.csv", path_google_drive_data)
    d = dict(zip(temp.Fund, temp.Code))
    check_before_using_dict('Fund', d, merge_df)
    merge_df['Account #'] = merge_df['Fund'].map(d)

    merge_df = merge_df[['Account #', 'Fund', 'Gross', 'Fees', 'Net']]

    merge_df.to_csv(Path(Path.cwd().parent / "out" / "total.csv", index=False))


    print(merge_df.to_markdown())
