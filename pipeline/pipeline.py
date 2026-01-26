import sys

import pandas as pd

print("arguement", sys.argv)

month = int(sys.argv[1])

#some data
#df = pd.DataFrame({"A": [1,2], "B":[3,4]})
#make it fun
df = pd.DataFrame({"day": [1,2], "num_passengers":[3,4]})

df["month"] = month

#my idea
#df["month"] = "July"

print(df.head())

#save to parquet file
df.to_parquet(f"output_{month}.parquet")



print(f"hello pipeline, month={month}")