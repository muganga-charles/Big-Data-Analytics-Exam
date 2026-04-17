## NYC Taxi Demand Prediction
## DSC8201Big Data Analytics Exam (Easter 2026)

End-to-end PySpark pipeline answering: *"Can we predict taxi pickups per
zone for the next hour accurately enough to guide driver allocation for a
small, resource-constrained urban mobility startup?"*

---

## Project layout

```
ucu_bda_project/
├── notebook/
│   ├── NYC_Taxi_Demand_Pipeline.ipynb  
│   └── pipeline.py                     
├── src/
│   ├── generate_sample_data.py         
│   └── build_architecture_diagram.py    
├── architecture/
│   ├── architecture_diagram.png
│   └── architecture_diagram.svg
├── data/
│   ├── raw/                 
│   ├── processed/           
│   ├── stream_input/        
│   ├── stream_checkpoint/
│   └── stream_output/      
├── models/                  
├── outputs/
│   ├── figures/            
│   └── metrics/             
└── README.md
```

## Prerequisites

- Python 3.10+
- Java 11 or 17 (PySpark 3.5 requirement)
- `pip install pyspark==3.5.1 pandas numpy matplotlib jupytext jupyterlab`

Check Java:  `java -version`

## How to run

**Option A: use the real dataset (exam submission path)**

```bash
# 1. Drop the NYC Taxi file(s) into data/raw/
#    (CSV or Parquet; the notebook auto-detects)
rm -f data/raw/sample_taxi.csv                  ## remove the synthetic file
cp /path/to/yellow_tripdata_*.parquet data/raw/

# 2. Launch Jupyter and run all cells
jupyter lab notebook/NYC_Taxi_Demand_Pipeline.ipynb
#   Kernel  ->  Restart Kernel and Run All Cells
```

**Option B: validate the pipeline on synthetic data first**

```bash
python src/generate_sample_data.py --rows 200000
jupyter lab notebook/NYC_Taxi_Demand_Pipeline.ipynb
```

## Pipeline stages (mapped to the exam tasks)

| Section | Exam task | What it produces |
|---|---|---|
| §0–1 | Task 1 framing | Role, stakeholder, analytical question |
| §2–3 | Task 4.B + 4.A | Data-quality audit, clean Parquet (silver) |
| §4   | Task 4.C        | Hourly zone panel (gold) |
| §5   | Task 4.C        | Temporal + lag + rolling features |
| §6   | Task 4.E        | Chronological train / val / test split |
| §7   | Task 4.D + 4.E  | LR / RF / GBT in Pipeline API + CV |
| §8   | Task 4.F + 5    | Saved PipelineModel, batch inference demo |
| §9   | Task 3 (streaming) | Structured Streaming file-source simulation |
| §10  | Task 5          | 5 stakeholder-facing visualisations |
| §11  | Task 5          | Metrics JSON + CSV for the report |

## Spark configuration rationale (for a laptop)

| Knob | Value | Why |
|---|---|---|
| `master` | `local[*]` | Use all cores on a single machine |
| `spark.driver.memory` | `3g` | Safe on an 8 GB laptop |
| `spark.sql.shuffle.partitions` | `16` | Default 200 is wasteful locally |
| `spark.sql.adaptive.enabled` | `true` | AQE shrinks shuffle stages at runtime |
| `spark.sql.execution.arrow.pyspark.enabled` | `true` | Fast pandas interop |

## Deliverable checklist

- [x] PySpark codebase (notebook + .py)
- [x] Architecture diagram (PNG + SVG)
- [x] Run-summary metrics
- [x] Reproducible — single run, seed pinned (SEED = 42)
