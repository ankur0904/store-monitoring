import pandas as pd

# Read the data from 3 CSV files named 1.csv, 2.csv, and 3.csv
df1 = pd.read_csv('1.csv')
df2 = pd.read_csv('2.csv')
df3 = pd.read_csv('3.csv')

# iterate over the rows of the dataframes
for index, row in df1.iterrows():
    print(row['store_id'], row['status'], row['timestamp_utc'])