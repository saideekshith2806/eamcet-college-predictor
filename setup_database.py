import pandas as pd
import sqlite3
import os

DATA_PATH = r"C:\WORK\MINOR project\output\eamcet_clean.csv"
DB_PATH   = r"C:\WORK\MINOR project\eamcet.db"

print("Loading CSV...")
df = pd.read_csv(DATA_PATH)
print(f"Rows loaded: {len(df)}")

# Clean up column names
df.columns = [c.strip().upper() for c in df.columns]

print("Connecting to database...")
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create the main cutoffs table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS cutoffs (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        inst_code     TEXT,
        college_name  TEXT,
        place         TEXT,
        dist_code     TEXT,
        college_type  TEXT,
        branch_code   TEXT,
        branch_name   TEXT,
        year          INTEGER,
        phase         TEXT,
        category      TEXT,
        gender        TEXT,
        closing_rank  INTEGER
    )
''')

print("Inserting data into database...")
df_insert = df.rename(columns={
    'INST_CODE'    : 'inst_code',
    'COLLEGE_NAME' : 'college_name',
    'PLACE'        : 'place',
    'DIST_CODE'    : 'dist_code',
    'COLLEGE_TYPE' : 'college_type',
    'BRANCH_CODE'  : 'branch_code',
    'BRANCH_NAME'  : 'branch_name',
    'YEAR'         : 'year',
    'PHASE'        : 'phase',
    'CATEGORY'     : 'category',
    'GENDER'       : 'gender',
    'CLOSING_RANK' : 'closing_rank'
})

# Only keep columns that exist in our table
keep = ['inst_code','college_name','place','dist_code','college_type',
        'branch_code','branch_name','year','phase','category','gender','closing_rank']
keep = [c for c in keep if c in df_insert.columns]
df_insert = df_insert[keep]

df_insert.to_sql('cutoffs', conn, if_exists='replace', index=False)

# Create indexes so searches are fast
print("Creating indexes for fast search...")
cursor.execute('CREATE INDEX IF NOT EXISTS idx_branch   ON cutoffs(branch_name)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_category ON cutoffs(category)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_year     ON cutoffs(year)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_inst     ON cutoffs(inst_code)')

conn.commit()

# Verify it worked
print("\nVerifying database...")
result = cursor.execute('SELECT COUNT(*) FROM cutoffs').fetchone()
print(f"Total rows in DB     : {result[0]}")

years = cursor.execute('SELECT DISTINCT year FROM cutoffs ORDER BY year').fetchall()
print(f"Years in DB          : {[y[0] for y in years]}")

colleges = cursor.execute('SELECT COUNT(DISTINCT inst_code) FROM cutoffs').fetchone()
print(f"Unique colleges      : {colleges[0]}")

branches = cursor.execute('SELECT COUNT(DISTINCT branch_name) FROM cutoffs').fetchone()
print(f"Unique branches      : {branches[0]}")

categories = cursor.execute('SELECT DISTINCT category FROM cutoffs ORDER BY category').fetchall()
print(f"Categories           : {[c[0] for c in categories]}")

# Test a real query - what would OC BOYS rank 5000 get?
print("\n--- TEST QUERY ---")
print("Testing: OC BOYS with rank 5000, CSE branch")
test = cursor.execute('''
    SELECT inst_code, college_name, branch_name, 
           AVG(closing_rank) as avg_cutoff
    FROM cutoffs
    WHERE category = 'OC'
      AND gender = 'BOYS'
      AND branch_name LIKE '%COMPUTER SCIENCE%'
      AND closing_rank >= 5000
    GROUP BY inst_code, college_name, branch_name
    ORDER BY avg_cutoff ASC
    LIMIT 5
''').fetchall()

for row in test:
    print(f"  {row[0]} | {row[2][:40]} | avg cutoff: {int(row[3])}")

conn.close()
print("\n" + "="*50)
print("DATABASE READY! File saved as eamcet.db")
print("="*50)
