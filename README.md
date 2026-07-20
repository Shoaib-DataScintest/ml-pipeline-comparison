# ML Data Pipeline Automation: Comparative Study

[![IEEE Access Submission](https://img.shields.io/badge/Target-IEEE%20Access-blue)]()
[![Status](https://img.shields.io/badge/Status-Under%20Review-yellow)]()

## Paper Title
**Low-Code AI Workflow Tools for ML Data Pipeline Automation:  
A Comparative Study of n8n, Flowise, and Apache Airflow**

## Authors
- **Muhammad Shoaib Inksar** — MS Data Science, University of Management and Technology, Lahore, Pakistan
- **Dr. Sundas Shahzeen** — Department of Data Science, UMT, Lahore, Pakistan

## Abstract
This paper presents the first systematic empirical comparison of n8n, Flowise,
and Apache Airflow for automated ML data preprocessing pipelines. We implement
five identical preprocessing tasks across all three platforms using two benchmark
datasets (UCI Adult Income: 48,842 rows; Smartphone Specs: 1,023 rows) and
evaluate them across six quantitative metrics and six qualitative dimensions.

## Key Findings
| Tool | Execution Time (DS-A) | Execution Time (DS-B) | Node Count | Qual. Score |
|------|----------------------|----------------------|------------|-------------|
| Flowise | 0.160s ⚡ fastest | 0.017s ⚡ fastest | 5 | 22.5/30 |
| Airflow | 0.823s | 0.097s | 10 | 24.0/30 |
| n8n | 2.461s | 0.104s | 4 ✅ fewest | 24.5/30 |

- All three tools achieve **100% data quality task completion**
- Flowise is **5.1× faster** than Airflow on large-scale data
- n8n requires the **fewest nodes** (4) — lowest implementation complexity

## Repository Structure
```
ml-pipeline-comparison/
├── main.tex                    # Full IEEE-format LaTeX paper
├── references.bib              # BibTeX references (20 sources)
├── dags/
│   └── preprocessing_dag.py   # Apache Airflow DAG (10 PythonOperators)
├── flowise_pipeline.py         # Flowise agent pipeline (Python)
└── logs/
    ├── airflow_metrics.csv     # Airflow experiment results
    ├── n8n_metrics.csv         # n8n experiment results
    └── flowise_metrics.csv     # Flowise experiment results
```

## Preprocessing Tasks
1. **T1** — Missing Value Imputation (median/mode)
2. **T2** — Duplicate Record Detection and Removal
3. **T3** — Data Type Conversion and Validation
4. **T4** — Outlier Detection and Handling (IQR method)
5. **T5** — Feature Normalization (Min-Max scaling)

## Datasets
- **DS-A:** UCI Adult Income Dataset — [UCI ML Repository](https://archive.ics.uci.edu/dataset/2/adult) (48,842 instances, 15 features)
- **DS-B:** Smartphone Specifications Dataset (1,023 instances, 12 features)

## How to Reproduce
1. Install Apache Airflow 2.9.3 (Python 3.12)
2. Place datasets in `data/` folder
3. Copy `dags/preprocessing_dag.py` to your Airflow dags folder
4. Trigger the DAG from the Airflow web UI
5. Results are saved to `logs/airflow_metrics.csv`

For n8n: import the workflow JSON and run with `NODE_FUNCTION_ALLOW_BUILTIN=fs`  
For Flowise: run `python3 flowise_pipeline.py` with the virtual environment active

## Citation
If you use this work, please cite:
```
M. S. Inksar and S. Shahzeen, "Low-Code AI Workflow Tools for ML Data Pipeline
Automation: A Comparative Study of n8n, Flowise, and Apache Airflow,"
IEEE Access, 2026. [Under Review]
```
