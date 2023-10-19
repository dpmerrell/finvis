
import pandas as pd

def parse_money_str(money_str):
    return float("".join(money_str[1:].split(",")))

def parse_money_obj(money_obj):
    if pd.isna(money_obj):
        return 0
    elif isinstance(money_obj, (float, int)):
        return money_obj
    elif isinstance(money_obj, str):
        return parse_money_str(money_obj)
    else:
        raise ValueError("Object"+str(obj)+"can't be parsed into money!")

def parse_money_col(col):
    return col.map(parse_money_obj)

def canonicalize_amount(df):
    if "money_in" in df.columns and "money_out" in df.columns:
        df["money_in"] = parse_money_col(df["money_in"])
        df["money_out"] = parse_money_col(df["money_out"])

        print("PARSED MONEY")
        print(df)

        df["amount"] = df["money_in"] - df["money_out"]
        df.drop(columns=["money_in", "money_out"], inplace=True)
    else:
        df["amount"] = parse_money_col(df["amount"])

    return df 


def categorize_txn(txn_row, categories_dict):
    """
    Categorize a transaction, using data from the transaction itself
    combined with information from the configuration dict.
    """
    # Loop through categories
    for cat, d in categories_dict.items():
        # Loop through criteria
        for k, v_ls in d.items():
            for v in v_ls:
                if v in txn_row[k]:
                    return cat
    return "__uncategorized__"
  
 
def impute_transactor(txn_row, transactors_dict):

    # Loop through possible transactors 
    for tor, d in transactors_dict.items():
        # Loop through criteria
        for k, v_ls in d.items():
            for v in v_ls:
                if v in txn_row[k]:
                    return tor
    return "__out__" 


def canonicalize_df(df, acct_dict):

    # Collect columns as specified in the config file
    cols_dict = acct_dict["cols"]
    
    old_cols = list(cols_dict.values()) 
    canonical_df = df.loc[:,old_cols]

    old_to_new = {v: k for k, v in cols_dict.items()}
    canonical_df.rename(old_to_new, inplace=True, axis="columns")
  
    print("RENAMED COLUMNS")
    print(canonical_df)
    # Make sure the "amount" column is canonicalized
    canonical_df = canonicalize_amount(canonical_df)

    print("CANONICALIZED AMOUNT")
    print(canonical_df)
    # Add an account name column
    canonical_df["account"] = acct_dict["name"]

    # Convert the date column to Datetimes; sort by date
    canonical_df["date"] = pd.to_datetime(canonical_df["date"])
    canonical_df.sort_values(["date"], inplace=True)

    # Handle missing values
    canonical_df.loc[pd.isna(canonical_df["original_category"]), "original_category"] = ""
    canonical_df.index = range(canonical_df.shape[0])

    # Categorize the transactions
    catfunc = lambda row: categorize_txn(row, acct_dict["categories"])
    canonical_df["category"] = canonical_df.apply(catfunc, axis="columns")
    
    # Impute the transactors
    torfunc = lambda row: impute_transactor(row, acct_dict["transactors"])
    canonical_df["transactor"] = canonical_df.apply(torfunc, axis="columns")

    return canonical_df


def unify_dfs(df_ls):
    unified_df = pd.concat(df_ls, ignore_index=True)
    unified_df.sort_values(["date"], inplace=True)
    return unified_df


if __name__=="__main__":

    import argparse
    import yaml

    parser = argparse.ArgumentParser()
    parser.add_argument("unified_tsv")
    parser.add_argument("--txn_csvs", nargs="+",
                        help="One or more CSVs containing transactions")
    parser.add_argument("--acct_yamls", nargs="+", 
                        help="YAML config files corresponding to the TSVs")
    args = parser.parse_args()

    # Validate input
    if len(args.txn_csvs) != len(args.acct_yamls):
        raise ValueError("Must provide equal numbers of txn_csvs and acct_yamls")

    # Load each CSV
    df_ls = []
    for txn_csv, acct_yaml in zip(args.txn_csvs, args.acct_yamls):
        # Load the account YAML
        with open(acct_yaml, "r") as f:
            acct_config_dict = yaml.safe_load(f)

        # Load the transaction data
        read_csv_opts = acct_config_dict["read_csv_opts"]
        txn_df = pd.read_csv(txn_csv, **read_csv_opts)

        # Convert the dataframe to a canonical form 
        canonical_df = canonicalize_df(txn_df, acct_config_dict)

        df_ls.append(canonical_df)

    unified_df = unify_dfs(df_ls)

    # Output a TSV containing categorized transactions 
    unified_df.to_csv(args.unified_tsv, sep="\t", index=False)


