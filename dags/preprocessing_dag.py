from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time, os, csv, psutil, threading

# ── Metric recorder ──────────────────────────────────────────────────
metrics = {}

def measure(tool, dataset, func):
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / 1024 / 1024
    cpu_samples = []
    stop = threading.Event()
    def sample_cpu():
        while not stop.is_set():
            cpu_samples.append(psutil.cpu_percent(interval=0.5))
    t = threading.Thread(target=sample_cpu)
    t.start()
    start = time.time()
    func()
    elapsed = round(time.time() - start, 3)
    stop.set(); t.join()
    mem_after = process.memory_info().rss / 1024 / 1024
    key = f"{tool}_{dataset}"
    metrics[key] = {
        "tool": tool, "dataset": dataset,
        "execution_time_s": elapsed,
        "memory_mb": round(mem_after - mem_before, 2),
        "cpu_peak_pct": round(max(cpu_samples) if cpu_samples else 0, 2)
    }
    log_metrics(metrics[key])

def log_metrics(entry):
    path = os.path.expanduser("~/airflow-paper/logs/metrics.csv")
    fields = ["tool","dataset","execution_time_s","memory_mb","cpu_peak_pct"]
    file_exists = os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not file_exists:
            w.writeheader()
        w.writerow(entry)

# ── Dataset A: Adult Income ───────────────────────────────────────────
DATA_A = os.path.expanduser("~/airflow-paper/data/adult_income.csv")
OUT_A  = os.path.expanduser("~/airflow-paper/data/adult_income_cleaned.csv")

def t1_load_adult():
    import pandas as pd
    df = pd.read_csv(DATA_A, header=None)
    df.columns = ["age","workclass","fnlwgt","education","education_num",
                  "marital_status","occupation","relationship","race",
                  "sex","capital_gain","capital_loss","hours_per_week",
                  "native_country","income"]
    df.replace(" ?", pd.NA, inplace=True)
    df.to_csv(OUT_A, index=False)
    print(f"[A] Loaded {len(df)} rows")

def t2_missing_adult():
    import pandas as pd
    def run():
        df = pd.read_csv(OUT_A)
        before = df.isna().sum().sum()
        for c in df.columns:
            if not pd.api.types.is_numeric_dtype(df[c]):
                df[c].fillna(df[c].mode()[0], inplace=True)
            else:
                df[c].fillna(df[c].median(), inplace=True)
        df.to_csv(OUT_A, index=False)
        print(f"[A] Fixed {before} missing values")
    measure("airflow", "adult", run)

def t3_duplicates_adult():
    import pandas as pd
    def run():
        df = pd.read_csv(OUT_A)
        before = len(df)
        df.drop_duplicates(inplace=True)
        df.to_csv(OUT_A, index=False)
        print(f"[A] Removed {before - len(df)} duplicates")
    measure("airflow", "adult", run)

def t4_outliers_adult():
    import pandas as pd
    def run():
        df = pd.read_csv(OUT_A)
        num_cols = df.select_dtypes(include="number").columns
        for c in num_cols:
            Q1, Q3 = df[c].quantile(0.25), df[c].quantile(0.75)
            IQR = Q3 - Q1
            df[c] = df[c].clip(Q1 - 1.5*IQR, Q3 + 1.5*IQR)
        df.to_csv(OUT_A, index=False)
        print(f"[A] Outliers handled")
    measure("airflow", "adult", run)

def t5_normalize_adult():
    import pandas as pd
    def run():
        df = pd.read_csv(OUT_A)
        num_cols = df.select_dtypes(include="number").columns
        for c in num_cols:
            mn, mx = df[c].min(), df[c].max()
            if mx > mn:
                df[c] = (df[c] - mn) / (mx - mn)
        df.to_csv(OUT_A, index=False)
        print(f"[A] Normalized {len(num_cols)} columns")
    measure("airflow", "adult", run)

# ── Dataset B: Smartphone ─────────────────────────────────────────────
DATA_B = os.path.expanduser("~/airflow-paper/data/smartphone.csv")
OUT_B  = os.path.expanduser("~/airflow-paper/data/smartphone_cleaned.csv")

def t1_load_smart():
    import pandas as pd
    df = pd.read_csv(DATA_B)
    df.to_csv(OUT_B, index=False)
    print(f"[B] Loaded {len(df)} rows")

def t2_missing_smart():
    import pandas as pd
    def run():
        df = pd.read_csv(OUT_B)
        before = df.isna().sum().sum()
        for c in df.columns:
            if not pd.api.types.is_numeric_dtype(df[c]):
                df[c].fillna(df[c].mode()[0], inplace=True)
            else:
                df[c].fillna(df[c].median(), inplace=True)
        df.to_csv(OUT_B, index=False)
        print(f"[B] Fixed {before} missing values")
    measure("airflow", "smartphone", run)

def t3_duplicates_smart():
    import pandas as pd
    def run():
        df = pd.read_csv(OUT_B)
        before = len(df)
        df.drop_duplicates(inplace=True)
        df.to_csv(OUT_B, index=False)
        print(f"[B] Removed {before - len(df)} duplicates")
    measure("airflow", "smartphone", run)

def t4_outliers_smart():
    import pandas as pd
    def run():
        df = pd.read_csv(OUT_B)
        num_cols = df.select_dtypes(include="number").columns
        for c in num_cols:
            Q1, Q3 = df[c].quantile(0.25), df[c].quantile(0.75)
            IQR = Q3 - Q1
            df[c] = df[c].clip(Q1 - 1.5*IQR, Q3 + 1.5*IQR)
        df.to_csv(OUT_B, index=False)
        print(f"[B] Outliers handled")
    measure("airflow", "smartphone", run)

def t5_normalize_smart():
    import pandas as pd
    def run():
        df = pd.read_csv(OUT_B)
        num_cols = df.select_dtypes(include="number").columns
        for c in num_cols:
            mn, mx = df[c].min(), df[c].max()
            if mx > mn:
                df[c] = (df[c] - mn) / (mx - mn)
        df.to_csv(OUT_B, index=False)
        print(f"[B] Normalized {len(num_cols)} columns")
    measure("airflow", "smartphone", run)

# ── DAG Definition ────────────────────────────────────────────────────
with DAG(
    dag_id     = "ml_preprocessing_pipeline",
    schedule   = "@once",
    start_date = datetime(2026, 1, 1),
    catchup    = False,
    tags       = ["paper", "mlops"],
) as dag:
    a1 = PythonOperator(task_id="A1_load_adult",       python_callable=t1_load_adult)
    a2 = PythonOperator(task_id="A2_missing_adult",    python_callable=t2_missing_adult)
    a3 = PythonOperator(task_id="A3_duplicates_adult", python_callable=t3_duplicates_adult)
    a4 = PythonOperator(task_id="A4_outliers_adult",   python_callable=t4_outliers_adult)
    a5 = PythonOperator(task_id="A5_normalize_adult",  python_callable=t5_normalize_adult)
    b1 = PythonOperator(task_id="B1_load_smart",       python_callable=t1_load_smart)
    b2 = PythonOperator(task_id="B2_missing_smart",    python_callable=t2_missing_smart)
    b3 = PythonOperator(task_id="B3_duplicates_smart", python_callable=t3_duplicates_smart)
    b4 = PythonOperator(task_id="B4_outliers_smart",   python_callable=t4_outliers_smart)
    b5 = PythonOperator(task_id="B5_normalize_smart",  python_callable=t5_normalize_smart)
    a1 >> a2 >> a3 >> a4 >> a5
    b1 >> b2 >> b3 >> b4 >> b5
