
import warnings
import pandas as pd
warnings.simplefilter(action='ignore', category=FutureWarning)
import numpy as np
import sys, os
from pathlib import Path
from nameparser import HumanName
pd.set_option('display.max_columns', None)
pd.options.mode.chained_assignment = None  # default='warn'
from datetime import timedelta


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







def clean_marketplace_dd(df, dd_match_on):

    df = df.rename(columns={4: "date",
                            6: "type",
                            7: "name",
                            10: "account_number",
                            12: "dd_gross_amount",
                            13: "dd_DD_Fees",
                            14: "net_amount"})

    df = df[['date', 'type', 'name', 'account_number', 'dd_DD_Fees', 'dd_gross_amount']]
    df['date'] = df['date'].dt.strftime('%Y-%m-%d').astype('str')

    df['dd_gross_amount'] = (df['dd_gross_amount'].
                             str.replace('$', '', regex=False).str.replace(',', '', regex=False).astype(float))

    df['dd_DD_Fees'] = (df['dd_DD_Fees']
                 .str.replace('(', '-', regex=False)
                 .str.replace(')', '', regex=False)
                 .str.replace(',', '', regex=False)
                 .astype(float))

    df['account_number'] = df['account_number'].str[-4:]
    df['name_last'] = df['name'].apply(lambda x: HumanName(x).last).str.title()

    df['Key'] = df.groupby(dd_match_on, dropna=False).cumcount()


    return df


def get_re_df(fn):
    p = Path(Path.cwd().parent / "data" / "raisers_edge")
    df = pd.read_csv(Path(p / fn), encoding='latin1')

    df = df.fillna(value=np.nan)

    grp = df.groupby('Gf_Pay_method')
    direct_debit_df = grp.get_group("Direct Debit")

    return direct_debit_df



def clean_re_dd(df, match_on, start_date, end_date):
    df = df.rename(columns={"Gf_Date": "Gift_Date",
                            "Gf_CnBio_ID": "Constituent_ID",
                            "Gf_Gift_ID": "Gift_ID",
                            "Gf_Amount": "Gross_Amount",
                            "Gf_Pay_method": "Pay_Method",
                            "Gf_Fund": "Fund"})

    # format date and filter for date range
    df['Gift_Date'] = pd.to_datetime(df['Gift_Date'])

    mask = (df['Gift_Date'] >= start_date) & (df['Gift_Date'] <= end_date)
    df = df[mask]

    df['Gross_Amount'] = (df['Gross_Amount']
                             .str.replace('$', '', regex=False)
                             .str.replace(',', '', regex=False)
                             .astype(float))

    # remove gifts that have been reconciled
    path_google_drive_data = Path("G:/My Drive/programming/reconciliation")
    ids_df = read_file_csv("dd_gift_ids.csv", path_google_drive_data)
    mask = df['Gift_ID'].isin(ids_df['Gift_ID'])
    df = df[~mask]


    df = df[['Constituent_ID' ,'Gift_ID', 'Gift_Date', 'Gross_Amount', 'Pay_Method', 'Fund']]
    df['Key'] = df.groupby(match_on, dropna=False).cumcount()

    print("\n ---------- DD RE Data")
    print(df.to_markdown())
    return df


def get_market_place_data_from_files(fn):
        p = Path(Path.cwd().parent / "data" / "market_place")

        dd_sheet_number = input("enter sheet number:")
        dd_sheet_name = "Sheet"+dd_sheet_number

        df = pd.read_excel(p / fn, header=None, sheet_name= dd_sheet_name, skiprows= 7)

        return df

def clean_market_place_data(df, dd_match_on):

        df = df.rename(columns={4: "Gift_Date",
                                6: "DD_Marker",
                                7: "DD_Name",
                                10: "Account_Number",
                                12: "Gross_Amount",
                                13: "DD_Fees",
                                14: "Net_Amount"})

        # filter for misc
        df = df[df['DD_Marker'] == "Direct debit"]

        #df['Gift_Date'] = pd.to_datetime(df['Gift_Date']).astype('str')

        df['Gift_Date'] = df['Gift_Date'].dt.strftime('%Y-%m-%d').astype('str')
        df['Gift_Date'] = pd.to_datetime(df['Gift_Date'])

        df['Name_Last'] = df['DD_Name'].apply(lambda x: HumanName(x).last).str.title()
        df['Name_First'] = df['DD_Name'].apply(lambda x: HumanName(x).first).str.title()

        df['DD_Fees'] = (df['DD_Fees'].str.replace('(', '-', regex=False)
                 .str.replace(')', '', regex=False)
                 .str.replace('$', '', regex=False)
                 .str.replace(',', '', regex=False)
                 .str.replace(')', '', regex=False)
                 .str.replace('$', '', regex=False)
                 .str.replace(',', '', regex=False)
                 .astype(float))

        df['Gross_Amount'] = df['Gross_Amount'].str.replace(',', '', regex=False).astype(float)

        df['Account_Number'] = df['Account_Number'].str[-4:]

        df['Key'] = df.groupby(dd_match_on, dropna=False).cumcount()

        df = df[['Gift_Date', 'Name_Last', 'Name_First', 'Account_Number', 'Gross_Amount', 'DD_Fees', 'Net_Amount', 'Key']]

        print("\n ---------- MP Data")
        print(df.to_markdown())

        return df



if __name__ == '__main__':

    mp_fn = "2024-08-05.xls"
    dd_match_on = ['Gross_Amount']

    mp_dd = get_market_place_data_from_files(mp_fn)

    mp_dd = clean_market_place_data(mp_dd, dd_match_on)

    s_date = mp_dd['Gift_Date'].min()
    e_date = mp_dd['Gift_Date'].max() + timedelta(days=1)

    print(f"start date: {s_date}")
    print(f"end date: {e_date}")

    re_dd = get_re_df("RE.CSV")
    re_dd = clean_re_dd(re_dd, dd_match_on, s_date, e_date)


    dd_merged = mp_dd.merge(re_dd, on=['Gross_Amount', 'Key'], how='left')

    print("\n ---------- DD Merged")
    print(dd_merged.to_markdown())
    dd_merged.to_csv(Path(Path.cwd().parent / "out" / "dd_merged.csv"))

    mask = re_dd['Gift_ID'].isin(dd_merged['Gift_ID'])
    print("\n ---------- RE no match")
    print(re_dd[~mask].to_markdown())

    dd_final = dd_merged[['Gross_Amount', 'DD_Fees', 'Fund']]
    dd_final = dd_final.fillna(0)
    dd_final = dd_final.groupby(['Fund']).sum().reset_index()

    print("\n ---------- DD Final")
    print(dd_final.to_markdown())
    dd_final.to_csv(Path(Path.cwd().parent / "out" / "dd_final.csv"))


   #
   #  if not is_empty_mp_dd:
   #      mp_dd = clean_marketplace_dd(mp_dd, dd_match_on)
   #      re_dd = process_re_dd(re_dd, dd_match_on)
   #      dd_final = mp_dd.merge(re_dd, on=['date', 'name_last', 'dd_gross_amount', 'key'], how='left')
   #      dd_final.to_csv(Path(Path.cwd().parent / "out" / "dd.csv"))
   #      dd_final = dd_final[['dd_gross_amount', 'dd_DD_Fees', 'fund']]
   #      dd_final = dd_final.fillna(0)
   #      dd_aggregate = dd_final.groupby(['fund']).sum().reset_index()
   #
   #

   #  re_cc = process_re_cc(re_cc)
   #  print(re_cc.to_markdown())
   # # print(re_cc.to_markdown())
   # #
   # #

   #
   #  #on = ['cc_number', 'cc_gross_amount', 'date', 'key']
   #
   #
   # #
   #  cc_final = mp_cc.merge(re_cc, on=['cc_number', 'cc_gross_amount', 'key'], how='left')
   #  print(cc_final['ID'])
   #
   #  mask = re_cc['ID'].isin(cc_final['ID'])
   #  print(re_cc[~mask].to_markdown())
   #
   #
   #  cc_final.to_csv(Path(Path.cwd().parent / "out" / "cc.csv"))
   #
   #
   # #
   #  cc_final = cc_final[['cc_gross_amount', 'cc_DD_Fees', 'fund']]
   # #
   #  cc_final = cc_final.fillna(0)
   #  cc_final = cc_final.groupby(['fund']).sum().reset_index()
   #
   #
   #
   #
   #  print(cc_final)
   #  cc_final.to_csv(Path(Path.cwd().parent / "out" / "cc_final.csv"))

   #
   #
   #
   #  #
    #print(dd_final)
    #
   #
   #  cc_aggregate = cc_final.groupby(['fund']).sum().reset_index()
   #
   #  if not is_empty_mp_dd:
   #      total_aggregate = dd_aggregate.merge(cc_aggregate, on=['fund'], how='outer')
   #  else:
   #      total_aggregate = cc_aggregate
   #
   #  total_aggregate = total_aggregate.fillna(0)
   #
   #  if not is_empty_mp_dd:
   #      total_aggregate['total_gross'] = (total_aggregate['dd_gross_amount'] + total_aggregate['cc_gross_amount']).round(2)
   #      total_aggregate['total_DD_Fees'] = (total_aggregate['dd_DD_Fees'] + total_aggregate['cc_DD_Fees']).round(2)
   #      total_aggregate['total_net'] = (total_aggregate['total_gross'] + total_aggregate['total_DD_Fees']).round(2)
   #      total_aggregate = total_aggregate[['fund', 'total_gross', 'total_DD_Fees', 'total_net']]
   #  else:
   #      total_aggregate['total_gross'] = total_aggregate['cc_gross_amount'].round(2)
   #      total_aggregate['total_DD_Fees'] = total_aggregate['cc_DD_Fees'].round(2)
   #      total_aggregate['total_net'] = (total_aggregate['total_gross'] + total_aggregate['total_DD_Fees']).round(2)
   #      total_aggregate = total_aggregate[['fund', 'total_gross', 'total_DD_Fees', 'total_net']]
   #  print(cc_aggregate)
   #  print(total_aggregate)
   #
   #  # set up dict for fund name to code
   #  path_out = Path("G:/My Drive/deposit slips/")
   #  path_google_drive_data = Path("G:/My Drive/programming/data")
   #  temp = read_file_csv("fund_to_code.csv", path_google_drive_data)
   #  d = dict(zip(temp.fund, temp.code))
   #  check_before_using_dict('fund', d, total_aggregate)
   #  total_aggregate.insert(0, 'fund_id', total_aggregate['fund'].map(d))
   #
   # # dd_final.to_csv(Path(Path.cwd().parent / "out" / "match_dd.csv"))
   #  #cc_final.to_csv(Path(Path.cwd().parent / "out" / "match_cc.csv"))
   #  total_aggregate.to_csv(Path(Path.cwd().parent / "out" / "final.csv", index=False))
   #  #print(total_aggregate)
