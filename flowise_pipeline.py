import pandas as pd
import time, psutil, os, csv

def measure_task(func, data):
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / 1024 / 1024
    t0 = time.time()
    result = func(data)
    elapsed = round(time.time() - t0, 3)
    mem_after = process.memory_info().rss / 1024 / 1024
    return result, elapsed, round(mem_after - mem_before, 2)

# ── Flowise agent tools (one per preprocessing step) ──────────────────
def tool_impute(df):
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c].fillna(df[c].median(), inplace=True)
        else:
            df[c].fillna(df[c].mode()[0] if not df[c].mode().empty else \'\', inplace=True)
    return df

def tool_deduplicate(df):
    return df.drop_duplicates()

def tool_convert_types(df):
    for c in df.columns:
        try:
            df[c] = pd.to_numeric(df[c], errors=\'ignore\')
        except:
            pass
    return df

def tool_outliers(df):
    num_cols = df.select_dtypes(include=\'number\').columns
    for c in num_cols:
        Q1, Q3 = df[c].quantile(0.25), df[c].quantile(0.75)
        IQR = Q3 - Q1
        df[c] = df[c].clip(Q1 - 1.5*IQR, Q3 + 1.5*IQR)
    return df

def tool_normalize(df):
    num_cols = df.select_dtypes(include=\'number\').columns
    for c in num_cols:
        mn, mx = df[c].min(), df[c].max()
        if mx > mn:
            df[c] = (df[c] - mn) / (mx - mn)
    return df

def run_flowise_agent(dataset_path, dataset_name):
    print(f"\n[Flowise Agent] Processing {dataset_name}...")
    if dataset_name == \'adult\':
        df = pd.read_csv(dataset_path, header=None)
        df.columns = ["age","workclass","fnlwgt","education","education_num",
                      "marital_status","occupation","relationship","race",
                      "sex","capital_gain","capital_loss","hours_per_week",
                      "native_country","income"]
        df.replace(" ?", pd.NA, inplace=True)
    else:
        df = pd.read_csv(dataset_path)

    total_time, total_mem = 0, 0
    tools = [
        ("T1_impute",      tool_impute),
        ("T2_deduplicate", tool_deduplicate),
        ("T3_convert",     tool_convert_types),
        ("T4_outliers",    tool_outliers),
        ("T5_normalize",   tool_normalize),
    ]
    for name, tool in tools:
        df, t, m = measure_task(tool, df.copy())
        total_time += t
        total_mem  += m
        print(f"  {name}: {t}s")

    print(f"  Total: {round(total_time,3)}s | rows: {len(df)}")
    return round(total_time, 3), round(total_mem, 2), len(df)

# ── Run both datasets ──────────────────────────────────────────────────
DATA_A = os.path.expanduser(\'~/airflow-paper/data/adult_income.csv\')
DATA_B = os.path.expanduser(\'~/airflow-paper/data/smartphone.csv\')
OUT    = os.path.expanduser(\'~/airflow-paper/logs/flowise_metrics.csv\')

results = []
for name, path in [(\'adult\', DATA_A), (\'smartphone\', DATA_B)]:
    t, m, rows = run_flowise_agent(path, name)
    results.append({\'tool\': \'flowise\', \'dataset\': name,
                    \'execution_time_s\': t, \'memory_mb\': m, \'rows\': rows})

with open(OUT, \'w\', newline=\'\') as f:
    w = csv.DictWriter(f, fieldnames=[\'tool\',\'dataset\',\'execution_time_s\',\'memory_mb\',\'rows\'])
    w.writeheader()
    w.writerows(results)

print(f"\nResults saved to {OUT}")
for r in results:
    print(f"  {r[\'tool\']} | {r[\'dataset\']} | {r[\'execution_time_s\']}s | {r[\'memory_mb\']}MB")
