import pandas as pd
import sqlite3
import os

# ── paths (same folder as your other scripts) ──────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
FEE_FILE  = os.path.join(BASE_DIR, 'datasets', 'college_fee_list (3).xlsx')
DB_PATH   = os.path.join(BASE_DIR, 'eamcet.db')

print("Loading fee file...")
# header is on row 7 (0-indexed = 6), data starts row 9
df = pd.read_excel(FEE_FILE, header=6, skiprows=[7, 8])

print("Raw columns:", list(df.columns))

# rename to clean names
df = df.rename(columns={
    'Institute Code' : 'inst_code',
    'Branch\nCode'   : 'branch_code',   # sometimes has newline
    'Branch Code'    : 'branch_code',
    'fee'            : 'fee',
    'Convener Seats' : 'convener_seats',
})

# fallback: if branch_code column didn't match, use positional (col index 10)
if 'branch_code' not in df.columns:
    df.columns.values[10] = 'branch_code'
    df.columns.values[11] = 'fee'

# keep only what we need
df = df[['inst_code', 'branch_code', 'fee']].copy()
df = df.dropna(subset=['inst_code', 'branch_code', 'fee'])
df['inst_code']   = df['inst_code'].astype(str).str.strip().str.upper()
df['branch_code'] = df['branch_code'].astype(str).str.strip().str.upper()
df['fee']         = pd.to_numeric(df['fee'], errors='coerce')
df = df.dropna(subset=['fee'])
df['fee'] = df['fee'].astype(int)

print(f"Loaded {len(df)} fee records")
print(df.head(5).to_string())

# ── write to DB ─────────────────────────────────────────────────────────────
conn   = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS fees (
        inst_code   TEXT,
        branch_code TEXT,
        fee         INTEGER,
        PRIMARY KEY (inst_code, branch_code)
    )
''')

# clear old data and reload fresh
cursor.execute('DELETE FROM fees')
df.to_sql('fees', conn, if_exists='append', index=False)

cursor.execute('CREATE INDEX IF NOT EXISTS idx_fees ON fees(inst_code, branch_code)')
conn.commit()

count = cursor.execute('SELECT COUNT(*) FROM fees').fetchone()[0]
print(f"\nDone! {count} rows in fees table.")
conn.close()
