"""Generate the end-to-end big-data architecture diagram (SVG + PNG)."""
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib.lines import Line2D

# ---- colour palette ------------------------------------------------------
C_SOURCE   = "#E8DAEF"   # sources
C_STORAGE  = "#D6EAF8"   # storage
C_PROCESS  = "#D1F2EB"   # processing
C_ML       = "#FCF3CF"   # ML
C_SERVE    = "#FADBD8"   # serving
C_MONITOR  = "#EAEDED"   # monitoring
EDGE       = "#1B2631"

fig, ax = plt.subplots(figsize=(14, 9))
ax.set_xlim(0, 14); ax.set_ylim(0, 9.5)
ax.axis("off")

def box(x, y, w, h, text, color, fontsize=9, bold=False):
    b = FancyBboxPatch((x, y), w, h,
                       boxstyle="round,pad=0.04,rounding_size=0.15",
                       linewidth=1.2, edgecolor=EDGE, facecolor=color)
    ax.add_patch(b)
    weight = "bold" if bold else "normal"
    ax.text(x + w/2, y + h/2, text, ha="center", va="center",
            fontsize=fontsize, fontweight=weight, wrap=True)

def arrow(x1, y1, x2, y2, style="-", color=EDGE, lw=1.4):
    a = FancyArrowPatch((x1, y1), (x2, y2),
                        arrowstyle="-|>", mutation_scale=14,
                        linestyle=style, color=color, linewidth=lw)
    ax.add_patch(a)

# ---------- title ----------
ax.text(7, 9.1, "NYC Taxi Demand-Prediction  —  On-Prem PySpark Architecture",
        ha="center", fontsize=14, fontweight="bold")
ax.text(7, 8.75, "Small Urban Mobility Startup  ·  single laptop  ·  offline-first",
        ha="center", fontsize=10, style="italic", color="#555")

# ---------- Layer 1: Sources ----------
ax.text(0.2, 8.05, "1 · Data Sources", fontsize=10, fontweight="bold")
box(0.2, 7.0, 3.0, 0.9, "Dispatch-system trip logs\n(CSV, streaming drop)", C_SOURCE, 9)
box(3.5, 7.0, 3.0, 0.9, "Historical TLC dumps\n(CSV / Parquet batches)", C_SOURCE, 9)

# ---------- Layer 2: Ingestion ----------
ax.text(0.2, 6.7, "2 · Ingestion", fontsize=10, fontweight="bold")
box(0.2, 5.7, 3.0, 0.8, "Folder watcher\n(Spark readStream, file source)", C_PROCESS, 9)
box(3.5, 5.7, 3.0, 0.8, "Batch loader\n(PySpark read.csv w/ schema)", C_PROCESS, 9)

arrow(1.7, 7.0, 1.7, 6.5, style="--")      # streaming (dashed)
arrow(5.0, 7.0, 5.0, 6.5)                  # batch (solid)

# ---------- Layer 3: Storage (Bronze / Silver / Gold) ----------
ax.text(0.2, 5.35, "3 · Storage (Medallion)", fontsize=10, fontweight="bold")
box(0.2, 4.3, 2.1, 0.9, "Bronze\nRaw CSV\n(data/raw/)", C_STORAGE, 9)
box(2.5, 4.3, 2.1, 0.9, "Silver\nCleaned Parquet\npartitioned by date", C_STORAGE, 9)
box(4.8, 4.3, 2.1, 0.9, "Gold\nHourly-zone panel\n(features + target)", C_STORAGE, 9)

arrow(1.7, 5.7, 1.25, 5.2, style="--")
arrow(5.0, 5.7, 3.55, 5.2)
arrow(2.3, 4.75, 2.5, 4.75)
arrow(4.6, 4.75, 4.8, 4.75)

# ---------- Layer 4: Processing engine (centred) ----------
ax.text(7.2, 8.05, "4 · Processing Engine", fontsize=10, fontweight="bold")
box(7.2, 6.3, 3.2, 1.6,
    "Apache Spark 3.5 (local[*])\nPySpark DataFrame API\n"
    "Structured Streaming\nAQE + Arrow enabled",
    C_PROCESS, 10, bold=True)

arrow(6.9, 5.9, 7.3, 6.3)          # from gold → spark
arrow(5.85, 5.2, 7.3, 6.3)

# ---------- Layer 5: ML ----------
ax.text(7.2, 5.85, "5 · ML Layer (Spark MLlib)", fontsize=10, fontweight="bold")
box(7.2, 4.8, 3.2, 0.9,
    "Pipeline: Indexer → OHE →\nAssembler → Scaler → GBT",
    C_ML, 9)
box(7.2, 3.75, 3.2, 0.9,
    "Model registry\n(local /models  PipelineModel)",
    C_ML, 9)
arrow(8.8, 6.3, 8.8, 5.7)
arrow(8.8, 4.8, 8.8, 4.65)

# ---------- Layer 6: Serving ----------
ax.text(10.7, 8.05, "6 · Serving / Outputs", fontsize=10, fontweight="bold")
box(10.7, 7.0, 3.1, 0.9,
    "Batch inference\n(cron  hourly Parquet)",
    C_SERVE, 9)
box(10.7, 5.85, 3.1, 0.9,
    "Streaming aggregate sink\n(hourly zone counts)",
    C_SERVE, 9)
box(10.7, 4.7, 3.1, 0.9,
    "Ops-Manager dashboard\n(CSV/Parquet → BI tool)",
    C_SERVE, 9)

arrow(10.4, 4.2, 10.7, 5.1)
arrow(10.4, 5.2, 10.7, 6.3, style="--")
arrow(10.4, 7.1, 10.7, 7.45)

# ---------- Layer 7: Monitoring (bottom band) ----------
ax.text(0.2, 3.6, "7 · Monitoring & Ops", fontsize=10, fontweight="bold")
box(0.2, 2.5, 6.8, 0.9,
    "Spark event logs  ·  data-quality audit (row counts, null %)  ·  metrics JSON  ·  model evaluation CSV",
    C_MONITOR, 9)
box(7.2, 2.5, 6.6, 0.9,
    "Retraining trigger: weekly cron  ·  MAE drift check  ·  manual model promotion",
    C_MONITOR, 9)
arrow(3.6, 3.75, 3.6, 3.4)
arrow(8.8, 3.75, 10.5, 3.4)

# ---------- Legend ----------
legend_handles = [
    FancyBboxPatch((0,0), 1, 1, facecolor=C_SOURCE, edgecolor=EDGE, label="Data sources"),
    FancyBboxPatch((0,0), 1, 1, facecolor=C_STORAGE, edgecolor=EDGE, label="Storage"),
    FancyBboxPatch((0,0), 1, 1, facecolor=C_PROCESS, edgecolor=EDGE, label="Processing"),
    FancyBboxPatch((0,0), 1, 1, facecolor=C_ML, edgecolor=EDGE, label="ML"),
    FancyBboxPatch((0,0), 1, 1, facecolor=C_SERVE, edgecolor=EDGE, label="Serving"),
    FancyBboxPatch((0,0), 1, 1, facecolor=C_MONITOR, edgecolor=EDGE, label="Monitoring"),
    Line2D([0],[0], color=EDGE, linestyle="-", linewidth=1.5, label="Batch flow"),
    Line2D([0],[0], color=EDGE, linestyle="--", linewidth=1.5, label="Streaming flow"),
]
ax.legend(handles=legend_handles, loc="lower center",
          bbox_to_anchor=(0.5, -0.02), ncol=8, fontsize=8.5, frameon=False)

# ---------- Footer ----------
ax.text(14, 0.3, "All components: open-source · on-premise  |  No cloud dependency",
        ha="right", fontsize=8.5, style="italic", color="#555")

plt.tight_layout()
plt.savefig("architecture/architecture_diagram.png", dpi=200, bbox_inches="tight")
plt.savefig("architecture/architecture_diagram.svg", bbox_inches="tight")
print("Saved architecture diagram (PNG + SVG)")
