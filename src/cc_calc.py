
import warnings
import pandas as pd
warnings.simplefilter(action='ignore', category=FutureWarning)
import numpy as np
import sys, os
from pathlib import Path
from datetime import timedelta, date
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



def clean_marketplace_dd(df):
    df = df.rename(columns={4: "date",
                            6: "type",
                            7: "name",
                            12: "gross_amount",
                            13: "fee",
                            14: "net_amount"})

    df = df[['date', 'type', 'name', 'gross_amount', 'fee', 'net_amount']]
    df['date'] = df['date'].dt.strftime('%Y-%m-%d').astype('str')
    df['net_amount'] = df['net_amount'].str.replace('$', '', regex=False).str.replace(',', '',regex=False).astype(float)
    df['gross_amount'] = df['gross_amount'].str.replace('$', '', regex=False).str.replace(',', '',regex=False).astype(float)
    df['fee'] = (df['fee'].str.replace('(', '-', regex=False)
                 .str.replace(')', '', regex=False)
                 .str.replace(',', '', regex=False)
                 .astype(float))

    return df


def clean_marketplace_cc(df, match_on):


    column_labels = ['c1', 'c2', 'c3', 'c4', 'date', 'c6', 'cc_type', 'cc_name', 'c9', 'c10', 'cc_number', 'c12',
                     'CC_Gross_Amount', 'CC_Fees', 'net_amount']
    df.columns = column_labels
    df = df[['date', 'cc_type', 'cc_name', "cc_number", 'CC_Gross_Amount',
             'CC_Fees', 'net_amount']]


    df['net_amount'] = df['net_amount'].str.replace('$', '', regex=False).str.replace(',', '',regex=False).astype(float)
    df['cc_number'] = df['cc_number'].str[-4:]
    df['date'] = df['date'].dt.strftime('%Y-%m-%d').astype('str')
    df['date'] =  pd.to_datetime(df['date'])

    df['CC_Gross_Amount'] = (df['CC_Gross_Amount']
                                .str.replace('$', '', regex=False)
                                .str.replace(',', '', regex=False)
                                .astype(float))

    df['CC_Fees'] = (df['CC_Fees'].str.replace('(', '-', regex=False)
                 .str.replace(')', '', regex=False)
                 .str.replace('$', '', regex=False)
                 .str.replace(',', '', regex=False)
                 .astype(float))

    df['key'] = df.groupby(match_on, dropna=False).cumcount()

    return df



def get_re_credit_card(fn):
    p = Path(Path.cwd().parent / "data" / "raisers_edge")
    df = pd.read_csv(Path(p / fn), encoding='latin1')

    df = df.fillna(value=np.nan)

    grp = df.groupby('Gf_Pay_method')
    credit_card_df = grp.get_group("Credit Card")

    # remove PayPal
    mask = credit_card_df['Gf_Credit_Type'] == "PayPal"
    credit_card_df = credit_card_df[~mask]


    return credit_card_df


def clean_re_cc(df, s_date, e_date):

    df = df.rename(columns={"Gf_Gift_ID": "Gift_ID",
                            "Gf_Type": "Gift_Type",
                            "Gf_Credit_Type": "cc_type",
                            "Gf_Date": "date",
                            "Gf_Cardholder_name": "cc_name",
                            "Gf_Credit_Card_Number": "cc_number",
                            "Gf_Amount": "CC_Gross_Amount",
                            "Gf_Pay_method": "pay_method",
                            "Gf_Fund": "Fund",
                            "Gf_CnBio_First_Name": "Const_First_Name",
                            "Gf_CnBio_Last_Name": "Const_Last_Name"})

    df['date'] = pd.to_datetime(df['date']).astype('str')
    df['date'] = pd.to_datetime(df['date'])

    mask = (df['date'] >= s_date) & (df['date'] <= e_date)
    df = df[mask]


    # remove recuring gift intilazation
    mask = df['Gift_Type'] == "Recurring Gift"
    df = df[~mask]


    df['cc_name'] = df['cc_name'].astype('str')
    df['cc_number'] = df['cc_number'].str[-4:]


    df['CC_Gross_Amount'] = (df['CC_Gross_Amount']
                             .str.replace('$', '', regex=False)
                             .str.replace(',', '', regex=False)
                             .astype(float))


    # remove giftsd with zero $ amount
    df = df[df['CC_Gross_Amount'] != 0]

    df['cc_name_last'] = df['cc_name'].apply(lambda x: HumanName(x).last)
    df['key'] = df.groupby(cc_match_on, dropna=False).cumcount()



    df = df[['Gift_ID', "Const_First_Name", "Const_Last_Name",
             'date', 'cc_type', 'cc_name', 'cc_number',
             'CC_Gross_Amount', 'pay_method', 'Fund', 'key']]


    # remove gifts that have been reconciled
    path_google_drive_data = Path("G:/My Drive/programming/reconciliation")
    ids_df = read_file_csv("cc_gift_ids.csv", path_google_drive_data)
    mask = df['Gift_ID'].isin(ids_df['Gift_ID'])
    df = df[~mask]

    #print(df.to_markdown())



    return df


def get_market_place_data_from_file(fn, sheet_name, skip_rows):
    path_market_place_data = Path(Path.cwd().parent / "data" / "market_place")
    df = pd.read_excel( path_market_place_data / fn, header=None, sheet_name=sheet_name, skiprows=skip_rows)
    df = df[:-1]



    return df


if __name__ == '__main__':

    mp_fn = "2024-08-05.xls"
    mp_sheet_name = "Sheet1"
    mp_sheet_skip_rows = 13



    cc_match_on = ['cc_number', 'CC_Gross_Amount']

    # get marketplace data and clean
    mp_cc = get_market_place_data_from_file(mp_fn, mp_sheet_name, mp_sheet_skip_rows)

    mp_cc = clean_marketplace_cc(mp_cc, cc_match_on)

    s_date = mp_cc['date'].min()- timedelta(days=1)
    e_date = mp_cc['date'].max() + timedelta(days=1)
    print(f"start date: {s_date}")
    print(f"end date: {e_date}")

    print("\n%------------------------- Marketplace Data -------------------------%")
    print(mp_cc.to_markdown())


    # get re data and clean
    re_cc= get_re_credit_card("RE.CSV")
    re_cc = clean_re_cc(re_cc, s_date, e_date)


    print("\n%------------------------- RE Data -------------------------%")
    print(re_cc.to_markdown())


    cc_merge = mp_cc.merge(re_cc, on=['cc_number', 'CC_Gross_Amount', 'key'], how='left')



    # find gifts that were in RE but not in Marketplace
    mask = re_cc['Gift_ID'].isin(cc_merge['Gift_ID'])
    print("\n%------------------------- IN RE Not IN Marketplace -------------------------%")
    print(re_cc[~mask].to_markdown())


    cc_merge.to_csv(Path(Path.cwd().parent / "out" / "cc_merged.csv"))


    cc_final = cc_merge[['CC_Gross_Amount', 'CC_Fees', 'Fund']]
    cc_final = cc_final.fillna(0)
    cc_final = cc_final.groupby(['Fund']).sum().reset_index()



    print("\n%---------------------------- Breakdown ------------------------------------%")

    print(cc_final)
    cc_final.to_csv(Path(Path.cwd().parent / "out" / "cc_final.csv"))


    # IDs
    ids_df = cc_merge[['date_x', 'date_y', 'Gift_ID']]
    mask = ids_df['Gift_ID'].notna()
    ids_df = ids_df[mask]
    ids_df.to_csv(Path(Path.cwd().parent / "out" / "cc_ids.csv"))
