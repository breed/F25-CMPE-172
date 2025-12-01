import pandas as pd
import sys

# From a URL:
# tables = pd.read_html("https://example.com/page.html")

print(sys.argv)
# From a local file:
tables = pd.read_html(sys.argv[1])   # returns a list of DataFrames

for i, df in enumerate(tables):
    df.to_csv(f"table_{i}.csv", index=False)
