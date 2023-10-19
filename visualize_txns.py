
import plotly.express as px
import pandas as pd
import numpy as np
import argparse
import yaml


def date_to_interval(date, interval="monthly"):
    if interval == "biweekly":
        return "Y{}-BW{:02d}".format(date.year, date.week//2)
    else:
        return "{}-{:02d}".format(date.year, date.month)


def construct_area_plot(categorized_df, session_config_dict, expenses=True):
    
    print("CATEGORIZED DF")
    print(categorized_df)
    # Assign time intervals to a new column
    mapfunc = lambda d: date_to_interval(d, session_config_dict["interval"])
    categorized_df["interval"] = categorized_df["date"].map(mapfunc)
    categorized_df.drop(columns=["date"], inplace=True)

    # Groupby time interval and category
    gp = categorized_df.groupby(["interval"])
    agg = gp.sum()
    
    # If plotting *expenses*, set positive values to 0
    if expenses:
        agg[agg > 0] = 0
        agg *= -1
    else:
        agg[agg < 0] = 0

    # Remove uniformly-zero columns
    kept_cols = [c for c in agg.columns if not all(agg[c] == 0)]
    agg = agg[kept_cols]

    # Sort columns by totals
    sums = agg.sum(axis=0)
    srt_cols = agg.columns[np.argsort(sums.values)][::-1]
    agg = agg[srt_cols]
    
    # Plot a stacked line plot
    fig = px.area(agg, title="Categorized Expense Tracking")
    return fig 


def construct_categorized_df(canonical_df):
   
    # Construct dataframe with category-specific columns 
    categorized_df = canonical_df["category"].str.get_dummies() * canonical_df[["amount"]].values
    categorized_df["date"] = pd.to_datetime(canonical_df["date"])
    
    return categorized_df 


if __name__=="__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("canonicalized_tsv")
    parser.add_argument("out_html")
    parser.add_argument("--session_yaml", 
                        help="YAML config file specifying how to display the data", 
                        default="configs/session.yaml")
    args = parser.parse_args()
    
    # Load the session config YAML
    with open(args.session_yaml, "r") as f:
        session_config_dict = yaml.safe_load(f)

    df = pd.read_csv(args.canonicalized_tsv, sep="\t")
    categorized_df = construct_categorized_df(df)

    # Construct a dataframe suitable for plotting
    fig = construct_area_plot(categorized_df, session_config_dict, expenses=True) 
    fig.write_html(args.out_html)


