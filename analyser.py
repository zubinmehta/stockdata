import pandas as pd

sbin_df = pd.read_csv("ynse/SBIN.NS")

print sbin_df

closes = sbin_df['Close']

print closes

print type(closes)

print closes.min()
print closes.max()

#closes.plot()

