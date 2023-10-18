
import plotly.express as px
import pandas as pd
import numpy as np
import argparse
import yaml


def parse_df(df, cols_dict):

    # Collect columns as specified in the config file
    old_cols = list(cols_dict.values()) 
    parsed_df = df.loc[:,old_cols]

    old_to_new = {v: k for k, v in cols_dict.items()}
    parsed_df.rename(old_to_new, inplace=True, axis="columns")

    # Other formatting
    parsed_df["date"] = pd.to_datetime(parsed_df["date"])
    parsed_df.sort_values(["date"], inplace=True)
    parsed_df.loc[pd.isna(parsed_df["original_category"]), "original_category"] = ""
    parsed_df.index = range(parsed_df.shape[0])

    return parsed_df


def date_to_group(date, interval="monthly"):
    if interval == "biweekly":
        return "Y{}-BW{:02d}".format(date.year, date.week//2)
    else:
        return "{}-{:02d}".format(date.year, date.month)


def categorize_expense(expense_row, categories_dict):
    """
    Categorize an expense, using data from the expense itself
    combined with information from the configuration dict.
    """
    category = "uncategorized"

    # Loop through categories
    for cat, d in categories_dict.items():
        # Loop through criteria
        for k, v_ls in d.items():
            for v in v_ls:
                if v in expense_row[k]:
                    return cat
    return category


def construct_categorized_df(parsed_df, data_config_dict):
   
    # Construct dataframe with category-specific columns 
    applyfunc = lambda row: categorize_expense(row, data_config_dict["categories"])
    parsed_df["category"] = parsed_df.apply(applyfunc, axis="columns")
    categorized_df = parsed_df["category"].str.get_dummies() * (parsed_df[["amount"]].values*data_config_dict["expense_sign"])
    
    return categorized_df, parsed_df["category"].values


def construct_plotting_df(categorized_df, session_config_dict):
    
    # Assign time intervals to a new column
    mapfunc = lambda d: date_to_group(d, session_config_dict["interval"])
    categorized_df["interval"] = parsed_df["date"].map(mapfunc)
    
    # Groupby time interval and category
    gp = categorized_df.groupby(["interval"])
    agg = gp.sum()
    
    # Remove all negative entries
    agg[agg < 0] = 0

    # Remove uniformly-zero columns
    kept_cols = [c for c in agg.columns if not all(agg[c] == 0)]
    agg = agg[kept_cols]

    # Sort columns by totals
    sums = agg.sum(axis=0)
    srt_cols = agg.columns[np.argsort(sums.values)][::-1]
    agg = agg[srt_cols]
    
    return agg 


if __name__=="__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("expenditure_tsv")
    parser.add_argument("out_categorized_tsv")
    parser.add_argument("out_html")
    parser.add_argument("--data_config", help="YAML config file specifying how to parse the TSV", default="configs/example_account.yaml")
    parser.add_argument("--session_config", help="YAML config file specifying how to display the data", default="configs/session.yaml")
    args = parser.parse_args()
    
    # Load the data config YAML
    with open(args.data_config, "r") as f:
        data_config_dict = yaml.safe_load(f)

    # Load the session config YAML
    with open(args.session_config, "r") as f:
        session_config_dict = yaml.safe_load(f)

    # Load the expenditure data
    sep = data_config_dict["sep"]
    expenditure_df = pd.read_csv(args.expenditure_tsv, sep=sep, na_values="")

    # Keep the columns we want
    parsed_df = parse_df(expenditure_df, data_config_dict["cols"])

    # Categorize expenses
    categorized_df, cat_vec = construct_categorized_df(parsed_df, data_config_dict)
   
    # Output a TSV containing categorized expenses 
    parsed_df["category"] = cat_vec
    parsed_df.to_csv(args.out_categorized_tsv, sep="\t", index=False)

    # Assign time intervals
    plotting_df = construct_plotting_df(categorized_df, session_config_dict) 
    print(plotting_df)
 
    # Plot a stacked line plot
    fig = px.area(plotting_df, title="Categorized Expense Tracking")
    fig.write_html(args.out_html)


