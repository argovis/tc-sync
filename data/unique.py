# quick script to check if a given ID always corresponds to the same name. If nothing prints out, then it does.

import pandas, sys

df = pandas.read_csv(sys.argv[1])

dfs = [v for k, v in df.groupby(['ID'])['NAME']]

for d in dfs:
	names = d.unique()
	if len(names) != 1:
		print(names)