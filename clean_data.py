import pandas as pd
import pdfplumber
import os

DATA_FOLDER = r"C:\WORK\MINOR project\data"
OUTPUT_FOLDER = r"C:\WORK\MINOR project\output"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

CATEGORY_COLS = [
    'OC_BOYS', 'OC_GIRLS',
    'BC_A_BOYS', 'BC_A_GIRLS',
    'BC_B_BOYS', 'BC_B_GIRLS',
    'BC_C_BOYS', 'BC_C_GIRLS',
    'BC_D_BOYS', 'BC_D_GIRLS',
    'BC_E_BOYS', 'BC_E_GIRLS',
    'SC_BOYS', 'SC_GIRLS',
    'ST_BOYS', 'ST_GIRLS',
    'EWS_BOYS', 'EWS_GIRLS'
]


def clean_excel(filepath, year):
    print(f"\nReading Excel: {filepath}")
    raw = pd.read_excel(filepath, header=None)

    # Find header row - fillna fixes float NaN issue
    header_row = 1
    for i in range(min(5, len(raw))):
        row_vals = raw.iloc[i].fillna('').astype(str).str.upper()
        if any('INST' in v or 'BRANCH' in v for v in row_vals):
            header_row = i
            break

    df = pd.read_excel(filepath, header=header_row)

    # Fix duplicate column names
    cols = list(df.columns)
    seen = {}
    new_cols = []
    for c in cols:
        c_str = str(c).strip()
        if c_str in seen:
            seen[c_str] += 1
            new_cols.append(f"{c_str}_{seen[c_str]}")
        else:
            seen[c_str] = 0
            new_cols.append(c_str)
    df.columns = new_cols

    print(f"  First 12 columns: {new_cols[:12]}")

    # Rename key columns
    rename_map = {}
    for col in df.columns:
        cu = str(col).upper().strip()
        if ('INST' in cu and 'CODE' in cu) or cu == 'INST CODE':
            rename_map[col] = 'INST_CODE'
        elif 'INSTITUTE NAME' in cu or 'COLLEGE NAME' in cu:
            rename_map[col] = 'COLLEGE_NAME'
        elif 'PLACE' in cu:
            rename_map[col] = 'PLACE'
        elif 'DIST' in cu:
            rename_map[col] = 'DIST_CODE'
        elif 'TYPE' in cu:
            rename_map[col] = 'COLLEGE_TYPE'
        elif 'BRANCH' in cu and 'CODE' in cu:
            rename_map[col] = 'BRANCH_CODE'
        elif 'BRANCH NAME' in cu:
            rename_map[col] = 'BRANCH_NAME'

    df = df.rename(columns=rename_map)

    # Assign category columns after BRANCH_NAME
    if 'BRANCH_NAME' in df.columns:
        branch_idx = list(df.columns).index('BRANCH_NAME')
        all_cols = list(df.columns)
        for j, cat in enumerate(CATEGORY_COLS):
            idx = branch_idx + 1 + j
            if idx < len(all_cols):
                all_cols[idx] = cat
        df.columns = all_cols

    df['YEAR'] = year
    df['PHASE'] = 'FINAL'

    keep = ['INST_CODE', 'COLLEGE_NAME', 'PLACE', 'DIST_CODE',
            'COLLEGE_TYPE', 'BRANCH_CODE', 'BRANCH_NAME', 'YEAR', 'PHASE'] + CATEGORY_COLS
    keep = [c for c in keep if c in df.columns]
    df = df[keep]

    df = df.dropna(subset=['INST_CODE'])
    df = df[df['INST_CODE'].astype(str).str.strip().str.match(r'^[A-Z]{2,8}$')]

    print(f"  Clean rows: {len(df)}")
    return df


def clean_pdf(filepath, year, phase='FINAL'):
    print(f"\nReading PDF: {filepath}")
    all_rows = []

    with pdfplumber.open(filepath) as pdf:
        print(f"  Pages: {len(pdf.pages)}")
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if table and len(table) > 1:
                    for row in table[1:]:
                        if row and len(row) > 5:
                            cleaned = [str(c).strip().replace('\n', ' ') if c else '' for c in row]
                            all_rows.append(cleaned)

    if not all_rows:
        print("  WARNING: No rows extracted!")
        return pd.DataFrame()

    print(f"  Raw rows: {len(all_rows)}")
    df = pd.DataFrame(all_rows)

    col_names = ['INST_CODE', 'COLLEGE_NAME', 'PLACE', 'DIST_CODE',
                 'COED', 'COLLEGE_TYPE', 'YEAR_ESTB', 'BRANCH_CODE', 'BRANCH_NAME']
    col_names += CATEGORY_COLS
    while len(col_names) < df.shape[1]:
        col_names.append(f'EXTRA_{len(col_names)}')
    col_names = col_names[:df.shape[1]]
    df.columns = col_names

    df['YEAR'] = year
    df['PHASE'] = phase

    df = df[df['INST_CODE'].str.match(r'^[A-Z]{2,8}$', na=False)]

    keep = ['INST_CODE', 'COLLEGE_NAME', 'PLACE', 'DIST_CODE',
            'COLLEGE_TYPE', 'BRANCH_CODE', 'BRANCH_NAME', 'YEAR', 'PHASE'] + CATEGORY_COLS
    keep = [c for c in keep if c in df.columns]
    df = df[keep]

    print(f"  Clean rows: {len(df)}")
    return df


def melt_to_long(df):
    cat_cols_present = [c for c in CATEGORY_COLS if c in df.columns]
    id_cols = ['INST_CODE', 'COLLEGE_NAME', 'PLACE', 'DIST_CODE',
               'COLLEGE_TYPE', 'BRANCH_CODE', 'BRANCH_NAME', 'YEAR', 'PHASE']
    id_cols = [c for c in id_cols if c in df.columns]

    df_long = df.melt(
        id_vars=id_cols,
        value_vars=cat_cols_present,
        var_name='CATEGORY_RAW',
        value_name='CLOSING_RANK'
    )

    df_long['GENDER'] = df_long['CATEGORY_RAW'].apply(
        lambda x: 'GIRLS' if str(x).endswith('_GIRLS') else 'BOYS'
    )
    df_long['CATEGORY'] = df_long['CATEGORY_RAW'].apply(
        lambda x: str(x).replace('_GIRLS', '').replace('_BOYS', '')
    )
    df_long = df_long.drop(columns=['CATEGORY_RAW'])
    return df_long


def clean_ranks(df):
    df['CLOSING_RANK'] = pd.to_numeric(df['CLOSING_RANK'], errors='coerce')
    df = df.dropna(subset=['CLOSING_RANK'])
    df = df[df['CLOSING_RANK'] > 0]
    df = df[df['CLOSING_RANK'] <= 200000]
    df['CLOSING_RANK'] = df['CLOSING_RANK'].astype(int)
    return df


# ============================================================
# MAIN
# ============================================================
print("=" * 60)
print("EAMCET DATA CLEANER - STARTING")
print("=" * 60)

all_dfs = []

# 2024
try:
    df = clean_excel(os.path.join(DATA_FOLDER, "2024FinalPhase.xlsx"), 2024)
    all_dfs.append(df)
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"ERROR 2024: {e}")

# 2023
try:
    df = clean_excel(os.path.join(DATA_FOLDER, "03_TSEAMCET_2023_FINALPHASE_LastRanks.xlsx"), 2023)
    all_dfs.append(df)
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"ERROR 2023: {e}")

# 2022
try:
    df = clean_pdf(os.path.join(DATA_FOLDER, "2022 final phase.pdf"), 2022)
    if not df.empty:
        all_dfs.append(df)
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"ERROR 2022: {e}")

# 2021
try:
    df = clean_pdf(os.path.join(DATA_FOLDER, "2021 first phase.pdf"), 2021, phase='PHASE_1')
    if not df.empty:
        all_dfs.append(df)
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"ERROR 2021: {e}")

# 2020
try:
    df = clean_pdf(os.path.join(DATA_FOLDER, "TS-EAMCET-2020-cutoff-final-phase.pdf"), 2020)
    if not df.empty:
        all_dfs.append(df)
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"ERROR 2020: {e}")

# Combine
print("\n" + "=" * 60)
print("COMBINING ALL DATA...")

if not all_dfs:
    print("ERROR: Nothing loaded!")
else:
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"Combined wide shape: {combined.shape}")

    print("Converting to long format...")
    long_df = melt_to_long(combined)

    print("Cleaning ranks...")
    long_df = clean_ranks(long_df)

    combined.to_csv(os.path.join(OUTPUT_FOLDER, "eamcet_wide.csv"), index=False)
    long_df.to_csv(os.path.join(OUTPUT_FOLDER, "eamcet_clean.csv"), index=False)

    print("\n" + "=" * 60)
    print("DONE! Files saved in output folder.")
    print(f"Total records : {len(long_df)}")
    print(f"Years covered : {sorted(long_df['YEAR'].unique())}")
    print(f"Colleges      : {long_df['INST_CODE'].nunique()}")
    print(f"Branches      : {long_df['BRANCH_NAME'].nunique()}")
    print("=" * 60)
    print("\nSample rows:")
    print(long_df[['INST_CODE', 'BRANCH_NAME', 'CATEGORY', 'GENDER', 'CLOSING_RANK', 'YEAR']].head(10).to_string())
