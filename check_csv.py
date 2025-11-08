import os, pandas as pd
print('CWD:', os.getcwd())
for f in ['movies.csv','ratings.csv']:
    print('\nFILE:', f, 'exists=', os.path.exists(f), 'size=', os.path.getsize(f) if os.path.exists(f) else 'N/A')
    if os.path.exists(f):
        df = pd.read_csv(f)
        print('shape:', df.shape)
        print(df.head(3).to_string(index=False))
