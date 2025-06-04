import os
import argparse

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from db.interface import DBInterface

# DB_FILE = '/Users/aidan/mnt/ab_data/ngafid/benchmarks.db'
# DB_URL = f'sqlite:///{DB_FILE}'
DB_URL = f"mysql+mysqlconnector://swift_reporter:KonOm8oeOKxlAvdL@localhost:3307/swift"

def main():
    parser = argparse.ArgumentParser(description="description")

    parser.add_argument("-m", "--metrics", nargs='+', type=str, required=True, help='Columns to plot', dest='metrics')
    parser.add_argument("-t", "--table", type=str, default="Training_History", dest="table")
    parser.add_argument("-o", "--ordinal", type=str, help="The column name of the ordinal used in training (i.e. step, epoch).", dest="ordinal")
    parser.add_argument("-w", "--where", type=str, help="SQL where condition for pulling from said table.", dest="where")
    parser.add_argument("-T", "--title", type=str, default="Learning Curve", dest="title")
    parser.add_argument("-s", "--save-location", type=str, default=None, dest="save_loc")

    args = parser.parse_args()

    if 'sqlite' in DB_URL and not os.path.exists(DB_FILE):
        raise ValueError(f'Unable to open file: {DB_FILE}!')

    db = DBInterface(DB_URL)
    cols = f'{args.ordinal},'
    cols = cols + ','.join(args.metrics)
    sql = f'SELECT {cols} FROM {args.table} WHERE {args.where}'
    df = pd.read_sql(sql, con=db.get_engine())

    df_melted = df.melt(id_vars=args.ordinal, var_name='metrics', value_name='value')

    sns.lineplot(data=df_melted, x=args.ordinal, y='value', hue='metrics')
    # plt.xticks(range(df_melted[args.ordinal].min(), df_melted[args.ordinal].max() + 1, 1))
    plt.title(args.title)

    if args.save_loc is None:
        plt.show()
    else:
        plt.savefig(args.save_loc)
        print(f"Saved plot to {args.save_loc}!")


if __name__ == "__main__":
    main()
