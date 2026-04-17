"""
DSC8201 Big Data Analytics — Project Report (Easter 2026)
Generates a 6-page PDF using reportlab's Platypus framework.

Layout strategy
---------------
- A4, narrow margins (15 mm) to maximise content
- 9.5 pt body text, 11/13 pt headers, 8.5 pt tables
- Two-column for some sections to increase information density
- All embedded figures sized to fit alongside text where possible
- Strict adherence to the exam's 6-page maximum
"""
from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, Table, TableStyle,
    KeepTogether, KeepInFrame, NextPageTemplate, PageTemplate, Frame,
    BaseDocTemplate,
)

REPORT_DIR = Path("")
OUT_PDF = REPORT_DIR / "UCU_BDA_Report.pdf"

# ---------- styles ----------
ss = getSampleStyleSheet()
NAVY = colors.HexColor("#1B3A6E")
GOLD = colors.HexColor("#C9A227")
GREY = colors.HexColor("#555555")
LIGHT = colors.HexColor("#F2F2F2")

style_body = ParagraphStyle("body", parent=ss["BodyText"], fontName="Helvetica",
                            fontSize=9.0, leading=11.0, alignment=TA_JUSTIFY,
                            spaceBefore=0, spaceAfter=2)
style_body_compact = ParagraphStyle("bodyc", parent=style_body,
                                    fontSize=8.6, leading=10.3, spaceAfter=2)
style_h1 = ParagraphStyle("h1", parent=ss["Heading1"], fontName="Helvetica-Bold",
                          fontSize=12.0, leading=14, textColor=NAVY,
                          spaceBefore=5, spaceAfter=2, keepWithNext=1)
style_h2 = ParagraphStyle("h2", parent=ss["Heading2"], fontName="Helvetica-Bold",
                          fontSize=10.0, leading=11.5, textColor=NAVY,
                          spaceBefore=3, spaceAfter=1, keepWithNext=1)
style_h3 = ParagraphStyle("h3", parent=ss["Heading3"], fontName="Helvetica-Bold",
                          fontSize=9.2, leading=10.5, textColor=colors.black,
                          spaceBefore=2, spaceAfter=1, keepWithNext=1)
style_caption = ParagraphStyle("cap", parent=ss["BodyText"], fontName="Helvetica-Oblique",
                               fontSize=7.8, leading=9, alignment=TA_CENTER,
                               textColor=GREY, spaceBefore=1, spaceAfter=4)
style_cover_title = ParagraphStyle("cv_t", parent=ss["Title"], fontName="Helvetica-Bold",
                                   fontSize=20, leading=24, alignment=TA_CENTER,
                                   textColor=NAVY, spaceAfter=6)
style_cover_sub = ParagraphStyle("cv_s", parent=ss["Title"], fontName="Helvetica",
                                 fontSize=12.5, leading=16, alignment=TA_CENTER,
                                 textColor=GREY, spaceAfter=3)
style_cover_meta = ParagraphStyle("cv_m", parent=ss["BodyText"], fontName="Helvetica",
                                  fontSize=11, leading=14, alignment=TA_CENTER,
                                  spaceAfter=2)
style_cover_meta_l = ParagraphStyle("cv_ml", parent=style_cover_meta,
                                    alignment=TA_LEFT)
style_ref = ParagraphStyle("ref", parent=style_body_compact, fontSize=7.6,
                           leading=9, leftIndent=12, firstLineIndent=-12,
                           alignment=TA_LEFT, spaceAfter=1)


# ---------- helpers ----------
def P(text, style=style_body):
    return Paragraph(text, style)


def figure_block(path, caption, width=170 * mm, height=None):
    img = Image(str(path), width=width, height=height) if height \
          else Image(str(path), width=width)
    img.hAlign = "CENTER"
    return [img, P(caption, style_caption)]


def small_table(data, col_widths=None, header_bg=NAVY):
    t = Table(data, colWidths=col_widths, hAlign="LEFT")
    t.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, 0), header_bg),
        ("TEXTCOLOR",    (0, 0), (-1, 0), colors.white),
        ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",     (0, 0), (-1, -1), 8.0),
        ("LEADING",      (0, 0), (-1, -1), 9.5),
        ("ALIGN",        (0, 0), (-1, -1), "LEFT"),
        ("VALIGN",       (0, 0), (-1, -1), "TOP"),
        ("GRID",         (0, 0), (-1, -1), 0.4, colors.HexColor("#888")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
        ("TOPPADDING",    (0, 0), (-1, -1), 2.5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 4),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
    ]))
    return t


# ---------- document ----------
PAGE_W, PAGE_H = A4
MARGIN = 15 * mm
USABLE_W = PAGE_W - 2 * MARGIN

doc = SimpleDocTemplate(
    str(OUT_PDF), pagesize=A4,
    leftMargin=14 * mm, rightMargin=14 * mm,
    topMargin=12 * mm, bottomMargin=12 * mm,
    title="UCU DSC8201 Big Data Analytics Project Report",
    author="Group S25M19",
)

story = []

# =====================================================================
# COVER PAGE
# =====================================================================
story.append(Spacer(1, 18 * mm))
story.append(Image(str(REPORT_DIR / "ucu_badge.png"), width=42 * mm, height=42 * mm,
                   hAlign="CENTER"))
story.append(Spacer(1, 6 * mm))
story.append(P("UGANDA CHRISTIAN UNIVERSITY", style_cover_meta))
story.append(P("Faculty of Engineering, Design and Technology", style_cover_meta))
story.append(P("Department of Computing and Technology", style_cover_meta))
story.append(Spacer(1, 14 * mm))
story.append(P("Predicting Hourly Taxi Demand at the Pickup-Zone Level<br/>"
               "for a Resource-Constrained Urban-Mobility Startup",
               style_cover_title))
story.append(Spacer(1, 4 * mm))
story.append(P("A PySpark Demand-Forecasting Pipeline using NYC Yellow-Taxi "
               "Trip Records as a Methodological Proxy", style_cover_sub))
story.append(Spacer(1, 18 * mm))

cover_meta = [
    ["Course Code",    P("DSC8201 — Big Data Analytics", style_cover_meta_l)],
    ["Programme",      P("MSc Data Science (Year 1, Semester 2)", style_cover_meta_l)],
    ["Examination",    P("Easter 2026 — Project-Based Exam", style_cover_meta_l)],
    ["Dataset",        P("NYC TLC Yellow-Taxi Trip Records (2025)", style_cover_meta_l)],
    ["Group",          P("S25M19", style_cover_meta_l)],
    ["Members",        P("[YOUR NAME] — [YOUR REG NO]<br/>"
                         "Akongo Irene Comfort — S25M19/001", style_cover_meta_l)],
    ["Submission",     P("Easter Semester 2026", style_cover_meta_l)],
]
cover_tbl = Table(cover_meta, colWidths=[45 * mm, 110 * mm])
cover_tbl.setStyle(TableStyle([
    ("FONTNAME",  (0, 0), (0, -1), "Helvetica-Bold"),
    ("FONTNAME",  (1, 0), (1, -1), "Helvetica"),
    ("FONTSIZE",  (0, 0), (-1, -1), 10.5),
    ("LEADING",   (0, 0), (-1, -1), 13),
    ("TEXTCOLOR", (0, 0), (0, -1), NAVY),
    ("VALIGN",    (0, 0), (-1, -1), "TOP"),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ("TOPPADDING",    (0, 0), (-1, -1), 4),
    ("LINEBELOW", (0, 0), (-1, -1), 0.4, colors.HexColor("#CCCCCC")),
]))
cover_tbl.hAlign = "CENTER"
story.append(cover_tbl)

story.append(Spacer(1, 22 * mm))
story.append(P('"A Complete Education for A Complete Person"', style_cover_sub))
story.append(PageBreak())

# =====================================================================
# PAGE 2 — TASKS 1 & 2
# =====================================================================

# ---------- TASK 1 ----------
story.append(P("Task 1 · Problem Formulation and Stakeholder Identification", style_h1))

story.append(P("1.1 Business Objective", style_h2))
story.append(P(
    "The hypothetical organisation is a small urban-mobility startup operating in a developing-economy city "
    "(modelled on Kampala, Uganda) that aims to replicate the data-driven dispatch and pricing practices of "
    "global ride-hailing platforms. The startup has neither cloud computing budget nor a dedicated data-engineering team; "
    "all analytics must run on a single office laptop with intermittent internet. The Operations Manager loses "
    "revenue daily because drivers idle in low-demand zones while passengers wait elsewhere. "
    "<b>The business objective is to reduce average passenger wait-time and driver idle-time by producing a "
    "short-horizon, zone-level demand forecast that can be acted on hourly.</b> "
    "Because no comparable open dataset exists for the local market, the methodology is developed and validated on "
    "the NYC TLC Yellow-Taxi corpus, whose row schema (timestamped pickups by zone, with fare and distance) is "
    "structurally identical to the trip-log records the startup's dispatch system already collects. "
    "The trained pipeline is therefore directly transferable once equivalent local data is accumulated."
))

story.append(P("1.2 Analytical Question", style_h2))
story.append(P(
    "<b><i>For each pickup zone, can we predict the number of taxi pickups that will occur in the next one-hour window, "
    "with sufficient accuracy to materially improve driver allocation decisions over the current heuristic "
    "(\"deploy where they were last hour\")?</i></b> "
    "The task is formulated as a supervised regression problem in which each row of the analytical panel represents "
    "one (zone, hour) cell, the target is the integer pickup count for that cell, and the predictors are derived "
    "exclusively from columns present in the raw schema: pickup timestamp, pickup zone identifier, fare, distance, "
    "and trip duration. No external covariates (weather, events, holidays) are used, both to honour the exam's "
    "schema-only constraint and to match the data the startup itself can realistically obtain offline."
))

story.append(P("1.3 Expected Outcome", style_h2))
story.append(P(
    "The deliverable is a serialised <b>Spark MLlib PipelineModel</b> together with a parallel scikit-learn "
    "model artefact, a batch inference Parquet file containing the next-hour demand prediction for every "
    "active zone, and an interpretive dashboard for the Operations Manager. "
    "Success is measured against three benchmarks: (i) <b>RMSE</b> on a temporally held-out test window, "
    "as the primary loss-functional metric; (ii) <b>MAE</b>, which translates directly into the average dispatch "
    "error in number of cars; and (iii) the <b>relative gap to a persistence baseline</b> "
    "(<i>\"next hour = same as this hour\"</i>), which is the only forecast the startup currently uses. "
    "An ML model is considered worth deploying only if it beats persistence by a margin large enough to justify "
    "the additional operational complexity."
))

# ---------- TASK 2 ----------
story.append(P("Task 2 · Big-Data Architecture Design", style_h1))

story.append(P("2.1 Architecture Diagram (submitted as separate file)", style_h2))
story.append(P(
    "The full-stack on-premises architecture is shown in the file "
    "<i>architecture_diagram.png</i> packaged alongside this report. It depicts a seven-layer "
    "medallion design: Sources → Ingestion → Storage (Bronze/Silver/Gold) → Processing Engine → "
    "ML Layer → Serving → Monitoring. Solid arrows mark batch flows; dashed arrows mark streaming flows. "
    "All components are open-source and on-premise; no cloud dependency exists at any layer."
))

story.append(P("2.2 Layer Description and Justification", style_h2))
arch_data = [
    ["Layer", "Tool selected", "Justification (anchored in the startup context)"],
    ["Ingestion (batch)",  "PySpark CSV/Parquet reader",
     "Dispatch system writes daily trip-logs to a shared folder; no Kafka licence cost."],
    ["Ingestion (stream)", "Spark Structured Streaming (file source)",
     "Watches an arrival folder; equivalent UX to Kafka without the operational burden."],
    ["Storage — Bronze",   "Raw CSV / Parquet on local disk",
     "Cheapest possible landing zone; preserves the original record for audit."],
    ["Storage — Silver",   "Parquet partitioned by pickup_date",
     "5–10× smaller than CSV; predicate push-down dramatically reduces RAM pressure."],
    ["Storage — Gold",     "Hourly-zone analytical panel (Parquet)",
     "Pre-aggregated; downstream ML reads only the columns it needs."],
    ["Processing engine",  "Apache Spark 3.5 (local[*]) + AQE + Arrow",
     "DataFrame API scales 0 → cluster without rewrite; AQE shrinks shuffles automatically."],
    ["ML — distributed",   "Spark MLlib Pipeline (LR, DT, RF, GBT)",
     "Native Spark estimators run on the full dataset; serialisable to disk in one call."],
    ["ML — local sample",  "scikit-learn (SVR, MLPRegressor)",
     "Spark has no SVR/MLP regressor; both are trained on stratified samples (§4.1)."],
    ["Serving",            "Cron-driven batch + streaming Parquet sinks",
     "Hourly cron writes a forecast file consumed by a CSV-driven Ops dashboard."],
    ["Monitoring",         "Spark event logs + JSON metrics + manual MAE drift check",
     "No Datadog/Prometheus budget; simplicity beats sophistication on a laptop."],
]
arch_tbl = Table(arch_data, colWidths=[33 * mm, 47 * mm, 100 * mm])
arch_tbl.setStyle(TableStyle([
    ("BACKGROUND",   (0, 0), (-1, 0), NAVY),
    ("TEXTCOLOR",    (0, 0), (-1, 0), colors.white),
    ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
    ("FONTSIZE",     (0, 0), (-1, -1), 8.0),
    ("LEADING",      (0, 0), (-1, -1), 9.6),
    ("VALIGN",       (0, 0), (-1, -1), "TOP"),
    ("GRID",         (0, 0), (-1, -1), 0.4, colors.HexColor("#888")),
    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
    ("TOPPADDING",    (0, 0), (-1, -1), 2.5),
    ("LEFTPADDING",   (0, 0), (-1, -1), 3.5),
    ("RIGHTPADDING",  (0, 0), (-1, -1), 3.5),
]))
story.append(arch_tbl)

# =====================================================================
# PAGE 3 — TASKS 3 & 4
# =====================================================================
story.append(P("Task 3 · Data Processing Strategy", style_h1))

story.append(P("3.1 Batch Processing — Spark Job Implemented", style_h2))
story.append(P(
    "<b>Historical data:</b> ten months of NYC Yellow-Taxi records (Feb–Dec 2025), "
    "totalling 12.03 M raw rows and 11.15 M after cleaning (a 92.7% retention rate). "
    "<b>Computation:</b> a single PySpark batch job (i) reads all monthly Parquet files with an explicit schema; "
    "(ii) drops duplicates, non-positive fares and distances, inverted timestamps, and rogue dates outside [2009, 2030]; "
    "(iii) caps trip distance at the 99.5<super>th</super> percentile to remove outlier long-haul trips; "
    "(iv) writes a Silver Parquet partitioned by <i>pickup_date</i>; "
    "(v) aggregates to a (zone, hour) panel and engineers temporal features and lags. "
    "<b>Why batch is appropriate:</b> the model is retrained at most weekly, the laptop has no cluster to push "
    "compute against, and Parquet partitioning gives near-streaming retrieval speeds for downstream queries. "
    "Batch is the rational default; streaming is a complement, not a substitute."
))

story.append(P("3.2 Streaming Processing — Simulated File-Source Stream", style_h2))
story.append(P(
    "A Structured-Streaming query watches a folder (<i>data/stream_input/</i>) into which the dispatch system "
    "drops new trip-log fragments. The query applies a 2-hour watermark, groups arrivals into 1-hour "
    "tumbling windows by pickup zone, and writes hourly counts to a Parquet sink. Latency is sub-minute "
    "and the same query would scale to a Kafka source by changing one <code>readStream.format()</code> call. "
    "<b>Why streaming is appropriate:</b> it lets the Ops Manager react to within-hour anomalies (e.g. a "
    "concert ending) without waiting for the nightly batch job. <b>Why it is partly inappropriate:</b> "
    "the ML model itself is batch-trained, and intermittent internet means streaming inference would stall — "
    "so streaming is used for live aggregation and dashboarding, not for online prediction."
))

story.append(P("3.3 Trade-off Analysis — Batch vs. Streaming in our Context", style_h2))
to_data = [
    ["Dimension", "Batch (our context)", "Streaming (our context)"],
    ["Latency",  "Hourly to nightly (acceptable for driver allocation; planning, not panic).",
     "Sub-minute (essential for surge zones during shift handover and special events)."],
    ["Cost",     "Effectively zero — runs on the office laptop; no per-message fee.",
     "Zero infrastructure cost using the file-source pattern; some CPU contention with batch."],
    ["Accuracy", "Higher — full historical context, careful train/test discipline, complete features.",
     "Lower — shorter windows, no lag features available until enough history accumulates."],
]
story.append(small_table(to_data, col_widths=[26 * mm, 75 * mm, 79 * mm]))

story.append(P("Task 4 · End-to-End ML Pipeline in Spark", style_h1))

story.append(P("4.1 Reproducible Pipeline (overview)", style_h2))
story.append(P(
    "The pipeline is implemented as a single executable notebook organised into eleven sections — "
    "ingestion, quality audit, cleaning, target construction, feature engineering, temporal split, "
    "model training, persistence, batch inference, streaming simulation, and visualisation. Reproducibility "
    "is enforced by (a) an explicit Spark schema instead of <i>inferSchema</i>, (b) a fixed random seed (42), "
    "(c) Parquet outputs at every stage, and (d) a serialised <i>PipelineModel</i> for the winning Spark estimator "
    "and a pickled bundle for the sklearn estimator. <b>Feature engineering</b> creates hour, day-of-week, "
    "weekend, month, and day calendar features, plus the three temporal lags <i>(t–1, t–24, t–168)</i> and a "
    "trailing 24-hour mean — these capture diurnal, weekly, and short-trend dynamics respectively. "
    "<b>Six models</b> are trained and compared: Linear Regression, Decision Tree, Random Forest, GBT (all "
    "Spark MLlib, full data), plus SVR-RBF and MLPRegressor (both scikit-learn, trained on stratified samples "
    "of 30 K and 200 K rows respectively because SVR is O(n²) in memory and Spark has no native SVR/MLP regressor). "
    "Persistence (<i>ŷ = lag_1h</i>) is reported alongside as the operational baseline."
))

story.append(P("4.2 Offline Training vs. Online (Batch) Inference", style_h2))
story.append(P(
    "<b>Offline training</b> happens on the developer laptop, weekly: the full hourly panel "
    "(297 K rows) is rebuilt from Silver Parquet, the train / validation / test windows are recomputed "
    "chronologically (70/15/15 of the time-axis), all six models are fit, and the best ML model by "
    "validation RMSE is serialised. Compute is bounded by the laptop's RAM; with a 3 GB driver heap and "
    "AQE-coalesced shuffles, end-to-end training completes in roughly 25 minutes. <b>Online inference</b> "
    "in our deployment context is in fact <b>batch inference at hourly cadence</b>: a cron job loads the "
    "saved PipelineModel, reads the most recent (zone, hour) feature row from the Gold table, and writes a "
    "Parquet of next-hour predictions to the dashboard folder. True real-time per-trip inference is "
    "deliberately out of scope — the Ops Manager dispatches in hourly cycles, so sub-second latency would be "
    "wasted. The hand-off from training to inference is made safe by the fact that the same Spark Pipeline "
    "(StringIndexer → OneHotEncoder → VectorAssembler → StandardScaler → estimator) is invoked at both ends — "
    "any feature transformation that runs at training necessarily runs at inference, eliminating train-serve skew."
))

# =====================================================================
# PAGE 4 — TASK 5 INSIGHTS
# =====================================================================
story.append(P("Task 5 · Analysis and Insights", style_h1))

story.append(P("5.1 Modelling Choice — Justification", style_h2))
story.append(P(
    "Six candidates were trained spanning four algorithmic families: linear (Linear Regression), "
    "single-tree (Decision Tree), tree ensembles (Random Forest, GBT), kernel methods (SVR-RBF), and neural "
    "networks (MLPRegressor with two hidden layers of 64 and 32 units). This range was deliberate: a Masters-level "
    "comparison should cover families with fundamentally different inductive biases, not multiple variants of the "
    "same family. <b>Random Forest emerged as the winner on both validation and test sets</b>, narrowly ahead of MLP, "
    "and is the model promoted to deployment. The full validation and test leaderboards are reproduced below."
))

# Validation + test leaderboards side-by-side
val_data = [
    ["Model",              "Val RMSE", "Val MAE", "Val R²"],
    ["random_forest",      "13.82", "6.10", "0.970"],
    ["mlp_regressor",      "13.83", "6.64", "0.970"],
    ["gbt",                "14.21", "6.39", "0.968"],
    ["linear_regression",  "16.73", "7.98", "0.956"],
    ["decision_tree",      "17.19", "7.11", "0.953"],
    ["persistence_lag1h",  "22.35", "9.47", "0.921"],
    ["svr_rbf",            "42.73", "12.70", "0.712"],
]
test_data = [
    ["Model",                   "Test RMSE", "Test MAE", "Test R²"],
    ["random_forest",           "13.31", "5.72", "0.934"],
    ["mlp_regressor",           "13.75", "6.69", "0.929"],
    ["persistence_lag1h",       "14.35", "6.26", "0.923"],
    ["linear_regression",       "14.45", "6.95", "0.922"],
    ["gbt",                     "14.83", "6.34", "0.918"],
    ["decision_tree",           "15.90", "6.42", "0.905"],
    ["svr_rbf",                 "23.67", "12.68", "0.790"],
]
combo = Table([[
    small_table(val_data, col_widths=[34 * mm, 16 * mm, 14 * mm, 14 * mm]),
    small_table(test_data, col_widths=[34 * mm, 16 * mm, 14 * mm, 14 * mm]),
]], colWidths=[90 * mm, 90 * mm])
combo.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"),
                           ("LEFTPADDING", (0, 0), (-1, -1), 0),
                           ("RIGHTPADDING", (0, 0), (-1, -1), 0)]))
story.append(combo)
story.append(P("Table 1. Validation (left) and held-out test (right) leaderboards across the six "
               "candidate models plus the persistence baseline. Lower RMSE/MAE is better; higher R² is better.",
               style_caption))

story.append(P("5.2 Key Findings", style_h2))
story.append(P(
    "<b>(i) Random Forest beats persistence by 7.24% on test RMSE</b> (13.31 vs 14.35) and by 8.7% on MAE — "
    "a meaningful but not dominant margin, consistent with the literature on short-horizon urban-mobility "
    "forecasting. <b>(ii) Persistence beats Linear Regression, GBT, and Decision Tree on the test set</b>, "
    "which is a striking finding: the strongest signal in hourly taxi demand is the previous hour itself, "
    "and any model that does not exploit that signal richly enough will be outperformed by simply repeating "
    "the last observation. <b>(iii) MLP comes within 3.3% of Random Forest</b>, suggesting that on this "
    "feature set the relationship is largely additive in the lag features and tree-based ensembling captures "
    "almost everything a small neural net can. <b>(iv) SVR collapses</b> with RMSE 23.67, almost twice the "
    "winner — this is not a model quality issue but a data-volume issue: SVR's O(n²) memory complexity "
    "forced training on a 30 K stratified sample (0.6% of the available training rows), which is insufficient "
    "to learn the long-tailed zone distribution. This finding is directly attributable to the big-data "
    "context of the project and is discussed further in §6.3."
))

# Side-by-side: feature importance + pred-vs-actual
fi_img = Image(str(REPORT_DIR / "05_feature_importance.png"), width=88 * mm, height=55 * mm)
pa_img = Image(str(REPORT_DIR / "04_pred_vs_actual.png"), width=72 * mm, height=72 * mm)
fp = Table([[fi_img, pa_img]], colWidths=[92 * mm, 88 * mm])
fp.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 0),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 0)]))
story.append(fp)
story.append(P("Figure 1. Random-Forest feature importances (left): three lag features account for ~88% of "
               "the model's predictive signal. Predicted vs. actual on the test set (right): tight clustering "
               "along the diagonal up to ~300 pickups/hour; mild under-prediction at the extreme upper tail.",
               style_caption))

# =====================================================================
# PAGE 5 — INSIGHTS CONTINUED + REFLECTION START
# =====================================================================
story.append(P("5.3 Stakeholder Visualisations and Demand Patterns", style_h2))

# Three small figures in a row
hr_img = Image(str(REPORT_DIR / "01_demand_by_hour.png"), width=58 * mm, height=29 * mm)
dw_img = Image(str(REPORT_DIR / "02_demand_by_dow.png"), width=58 * mm, height=35 * mm)
tz_img = Image(str(REPORT_DIR / "03_top_zones.png"), width=63 * mm, height=32 * mm)
trip = Table([[hr_img, dw_img, tz_img]], colWidths=[60 * mm, 60 * mm, 65 * mm])
trip.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"),
                          ("LEFTPADDING", (0, 0), (-1, -1), 0),
                          ("RIGHTPADDING", (0, 0), (-1, -1), 0)]))
story.append(trip)
story.append(P("Figure 2. Demand by hour-of-day (left), by day-of-week (centre), and total demand by "
               "pickup zone — top 15 (right). Evening peak ≈ 5–10 pm; weekday/weekend split is mild "
               "(~30%); demand is heavily concentrated in zones 237, 161, 236, 132 (Manhattan core).",
               style_caption))

story.append(P("5.4 Business Insights and Recommendations for the Operations Manager", style_h2))
story.append(P(
    "Three actionable patterns emerge for the dispatch desk. "
    "<b>(R1) Pre-position drivers ahead of the evening ramp.</b> Mean per-zone demand more than doubles "
    "between 9am (~30 pickups/hour) and 6pm (~55 pickups/hour). The model's hourly forecast should drive a "
    "<i>15-minute-lead</i> dispatch order so cars arrive in high-demand zones before the surge, not after it. "
    "<b>(R2) Rebalance toward the Manhattan-core zones (237, 161, 236, 132).</b> "
    "These four zones alone absorb roughly 40% of total demand in the top-15 list. A dynamic "
    "<i>top-N</i> dispatch rule that allocates a minimum number of standby cars to each of these zones "
    "every hour would directly translate the model's predictions into operational policy. "
    "<b>(R3) Use the persistence-baseline gap as a real-time anomaly alarm.</b> When the model's prediction "
    "diverges sharply from <i>lag_1h</i> (e.g. > 30%), this signals a regime change — a concert, a weather event, "
    "a transport disruption — and the Ops Manager should be paged. The 7% average improvement is modest, but "
    "this <i>conditional</i> use of the model captures most of the genuine business value."
))

story.append(P("Task 6 · Critical Reflection", style_h1))

story.append(P("6.1 Reflection Questions", style_h2))

story.append(P("(a) Why feature engineering must be identical between training and inference.", style_h3))
story.append(P(
    "Every feature in our pipeline is constructed as a deterministic transformation of raw inputs — for example, "
    "<i>lag_24h</i> is computed via a windowed lag over <i>(zone, hour)</i> tuples, and <i>roll_mean_24h</i> is "
    "an order-dependent rolling mean. If inference computes these any differently than training did — e.g. "
    "indexing by <i>(zone, day)</i> instead of <i>(zone, hour)</i>, or with a different fillna policy — the "
    "model receives <b>numerically different inputs</b> from the ones it was fitted on. This is the canonical "
    "<b>train–serve skew</b> failure mode: model accuracy degrades silently while the code reports no error. "
    "Our pipeline guards against this in two ways: (i) all preprocessing is encapsulated in a single "
    "<i>PipelineModel</i> object that is serialised once and loaded identically at inference time; and (ii) the "
    "lag/rolling-feature SQL is centralised in one notebook section, so there is exactly one definition of "
    "what each feature means."
))

story.append(P("(b) How the pipeline would respond to schema changes in production.", style_h3))
story.append(P(
    "<b>Concrete example.</b> NYC TLC introduced a <i>cbd_congestion_fee</i> column in the 2025 Yellow-Taxi "
    "files to reflect new congestion-pricing rules; an analogous policy change in the local context might add a "
    "new payment type or rename <i>PULocationID</i> to <i>OriginZoneID</i>. <b>Stage of failure.</b> Failure "
    "occurs in the ingestion layer — Spark's <i>read.parquet</i> with our explicit schema would either drop the "
    "new column silently (best case) or fail on a type mismatch (worst case). If a categorical receives a new, "
    "unseen value, the StringIndexer raises an exception unless <i>handleInvalid</i> is set. <b>Mitigations.</b> "
    "(1) <i>handleInvalid=\"keep\"</i> is set on both StringIndexer and OneHotEncoder so new categories are "
    "routed to a reserved \"unseen\" bucket rather than crashing the job; the model degrades gracefully until "
    "the next retraining. (2) A schema-validation step (implemented as a defensive <i>select</i>-with-cast in "
    "the ingestion cell) compares incoming columns against an expected list and writes any deviations to a "
    "<i>data/anomalies/</i> Parquet for human review. (3) Weekly retraining on the latest Silver Parquet means "
    "any new categorical bucket is incorporated automatically once enough examples exist."
))

story.append(P("(c) Challenges of streaming in our context.", style_h3))
story.append(P(
    "Three concrete challenges. <b>(i) No event broker.</b> Without Kafka or Kinesis, our streaming source is a "
    "watched folder, which means latency is bounded by however quickly the dispatch system writes its log fragments — "
    "typically minutes, not seconds. <b>(ii) Lag features are not stream-friendly.</b> Computing <i>lag_168h</i> on a "
    "stream requires either materialising a 168-hour state store (memory-expensive on a laptop) or accepting that the "
    "first week of streaming output has only partial features. We chose the latter and document it. "
    "<b>(iii) Intermittent connectivity.</b> The startup's office connection drops several times a week; a streaming "
    "ML pipeline that requires constant uplink (e.g. a remote feature store) would be unavailable precisely when the "
    "Ops Manager needs it most. This is the structural reason we kept inference offline."
))

# =====================================================================
# PAGE 6 — REFLECTION CONTINUED, CONCLUSION, REFERENCES
# =====================================================================

story.append(P("6.2 Limitations", style_h2))
story.append(P(
    "<b>Data limitations.</b> The cleaning step removed 7.3% of raw rows (882 K out of 12.03 M), of which "
    "the majority were either invalid timestamps or non-positive fares — i.e. corrupt records, not biased "
    "samples. Two more subtle biases remain: (i) NYC TLC records cover only metered Yellow Taxis, "
    "underrepresenting outer-borough demand and ride-hail substitutes, and (ii) zone-level imbalance is "
    "extreme (the top four zones absorb ~40% of demand), which means model error is dominated by "
    "performance in those few cells. The dataset's representativeness for a developing-city startup is "
    "<b>methodological, not direct</b>: NYC's spatial-temporal demand structure (peaks, zone concentration, "
    "lag autocorrelation) generalises, but the absolute pickup volumes do not. "
    "<b>Algorithmic limitations.</b> Random Forest assumes that the joint distribution of features is "
    "stationary across the train/test split — Christmas and New Year's Eve fall in the test window and "
    "violate this assumption, which partly explains the val-to-test R² drop from 0.970 to 0.934. Overfitting "
    "risk is moderate (60 trees × depth 10 = high capacity), mitigated only by the chronological split; we "
    "did not run nested cross-validation due to the runtime budget. "
    "<b>Infrastructure limitations.</b> A single 8 GB laptop bounds the dataset at roughly 15 M rows, the "
    "tree depth at ~10, and SVR training at ~30 K rows. Anything larger triggers the JVM out-of-memory errors "
    "we encountered during development. Model complexity is therefore not chosen freely — it is dictated by "
    "what the heap will hold."
))

story.append(P("6.3 Ethical Considerations", style_h2))
story.append(P(
    "Three ethical risks deserve explicit treatment. "
    "<b>(E1) Driver surveillance and surge-pricing harm.</b> A demand-prediction model that informs dispatch "
    "can equally inform <i>algorithmic management</i> of drivers: forcing relocation, penalising idle time, "
    "or triggering surge pricing that hurts low-income passengers in poorly-served zones. Mitigation: the "
    "Operations Manager is explicitly named as the <b>only</b> consumer of the prediction; surge-pricing "
    "use is excluded by policy and any future extension would require a separate fairness audit. "
    "<b>(E2) Zone-level redlining.</b> Continually deploying drivers to high-demand zones starves "
    "low-demand zones of supply, creating a feedback loop in which underserved neighbourhoods become "
    "permanently underserved. Mitigation: enforce a minimum service floor per zone in dispatch policy, "
    "independent of model output. "
    "<b>(E3) Privacy of trip records.</b> Even though TLC data is aggregated by zone, individual drivers "
    "and high-frequency riders can be re-identified from pickup patterns. Mitigation: never join the "
    "trip-log table to any customer or driver identifier inside the analytical store; restrict raw access to "
    "named individuals."
))

story.append(P("6.4 Scalability Challenges", style_h2))
story.append(P(
    "Local PySpark execution scales linearly until it hits the laptop's RAM ceiling (~15 M trip rows or "
    "~500 K hourly-panel rows for our pipeline). The first bottleneck is the lag-feature window — "
    "<i>Window.partitionBy(\"PULocationID\")</i> shuffles the entire dataset across cores, and at the "
    "100 M-row scale this would saturate the disk. The second bottleneck is the GBT trainer, which holds "
    "intermediate boosting residuals in memory. <b>Technical scaling path:</b> migrate to a small YARN or "
    "Kubernetes Spark cluster (3–5 worker nodes, 16 GB each) — no code change required, only the "
    "<i>master</i> URL. <b>Architectural scaling path:</b> replace the watched folder with Kafka, replace "
    "Parquet-on-disk with Delta Lake or Iceberg for ACID guarantees, and introduce a feature store "
    "(e.g. Feast) so streaming inference can read materialised lag features in O(1)."
))

story.append(P("6.5 Real-World Deployment", style_h2))
story.append(P(
    "In production the pipeline runs as three cron jobs on the office laptop. <b>(i) Hourly batch:</b> "
    "ingest the previous hour's dispatch CSV, append to Bronze, promote to Silver, refresh the Gold panel, "
    "and write next-hour predictions to <i>data/predictions/</i> for the dashboard. "
    "<b>(ii) Weekly retraining:</b> rebuild the full hourly panel, re-fit all six models, promote the new "
    "champion only if it beats the current champion's MAE on the most recent two-week window by ≥3% (a "
    "shadow-deployment guard). <b>(iii) Daily monitoring:</b> compute MAE drift on the previous day's "
    "predictions; if drift exceeds 25%, page the Ops Manager. The whole stack is reproducible from the "
    "GitHub repository (<i>Group_S25M19_Dataset_Taxi_UCU_BDA_Exam_Easter2026.zip</i>), with a fixed seed, "
    "pinned package versions in <i>requirements.txt</i>, and a Run-All notebook that produces every "
    "artefact in this report from the raw Parquet inputs."
))

story.append(P("Conclusion", style_h1))
story.append(P(
    "This project delivered an end-to-end Spark + scikit-learn demand-prediction pipeline for a "
    "resource-constrained urban-mobility startup, validated on ten months of NYC Yellow-Taxi records. "
    "Six models spanning four algorithmic families were compared on a strictly chronological train/test "
    "split; Random Forest emerged as the deployment champion with a held-out RMSE of 13.31 pickups per "
    "zone-hour and an R<super>2</super> of 0.934, beating the operational persistence baseline by 7.2%. Three findings "
    "carry the most weight for the report's audience. First, in short-horizon urban-mobility forecasting "
    "the persistence baseline is genuinely competitive — a Masters-level evaluation must beat it explicitly, "
    "not assume any ML model wins by default. Second, kernel methods such as SVR do not survive the "
    "transition from textbook datasets to a multi-million-row big-data context without architectural "
    "compromises that destroy their accuracy. Third, on commodity laptop hardware Spark's distributed "
    "abstractions scale cleanly to the ~10 M-row regime when paired with explicit schemas, Parquet, and "
    "AQE. The pipeline is therefore both immediately deployable in its current local form and trivially "
    "portable to a YARN/Kubernetes cluster when the startup outgrows the office laptop — the "
    "single-machine constraint is honoured today without forfeiting the option to scale tomorrow."
))

story.append(P("Team Contribution", style_h1))
team_cell_style = ParagraphStyle("team", parent=style_body_compact,
                                 fontSize=8.4, leading=10, alignment=TA_LEFT,
                                 spaceBefore=0, spaceAfter=0)
team_data = [
    [P("<b>Member</b>", team_cell_style), P("<b>Tasks executed</b>", team_cell_style)],
    [P("[YOUR NAME] — [YOUR REG NO]", team_cell_style),
     P("Architecture design (Task 2); Spark ingestion, cleaning, and feature engineering "
       "(Task 4 §A–§C); model training and evaluation for Spark MLlib estimators "
       "(Task 4 §D–§E); report sections 1, 3, 4, 6.4, conclusion.", team_cell_style)],
    [P("Akongo Irene Comfort — S25M19/001", team_cell_style),
     P("Streaming-simulation implementation (Task 3); scikit-learn SVR and MLP integration; "
       "stakeholder visualisations and feature-importance analysis (Task 5); "
       "report sections 5, 6.1–6.3.", team_cell_style)],
]
team_tbl = Table(team_data, colWidths=[55 * mm, 127 * mm])
team_tbl.setStyle(TableStyle([
    ("BACKGROUND",   (0, 0), (-1, 0), NAVY),
    ("TEXTCOLOR",    (0, 0), (-1, 0), colors.white),
    ("FONTSIZE",     (0, 0), (-1, -1), 8.4),
    ("LEADING",      (0, 0), (-1, -1), 10),
    ("VALIGN",       (0, 0), (-1, -1), "TOP"),
    ("GRID",         (0, 0), (-1, -1), 0.4, colors.HexColor("#888")),
    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ("TOPPADDING",    (0, 0), (-1, -1), 3),
    ("LEFTPADDING",   (0, 0), (-1, -1), 4),
    ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
]))
story.append(team_tbl)

story.append(P("References", style_h1))
refs = [
    "[1] NYC Taxi & Limousine Commission (2025). <i>TLC Trip Record Data: Yellow Taxi Trip Records.</i> "
    "Available at: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page (accessed April 2026).",
    "[2] Karau, H., Konwinski, A., Wendell, P., &amp; Zaharia, M. (2015). <i>Learning Spark: Lightning-Fast "
    "Big Data Analysis.</i> O'Reilly Media.",
    "[3] Chambers, B., &amp; Zaharia, M. (2018). <i>Spark: The Definitive Guide.</i> O'Reilly Media. "
    "Chapter 24 (Advanced Analytics with MLlib) and Chapter 21 (Structured Streaming).",
    "[4] Apache Spark (2024). <i>Structured Streaming Programming Guide.</i> "
    "https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html.",
    "[5] Pedregosa, F. et al. (2011). Scikit-learn: Machine Learning in Python. "
    "<i>Journal of Machine Learning Research</i>, 12, 2825–2830.",
    "[6] Breiman, L. (2001). Random Forests. <i>Machine Learning</i>, 45(1), 5–32.",
    "[7] Friedman, J. H. (2001). Greedy Function Approximation: A Gradient Boosting Machine. "
    "<i>Annals of Statistics</i>, 29(5), 1189–1232.",
    "[8] Hyndman, R. J., &amp; Athanasopoulos, G. (2021). <i>Forecasting: Principles and Practice</i> "
    "(3rd ed.). OTexts. Chapter 5 (Benchmark methods, including the naïve / persistence baseline).",
    "[9] Sculley, D. et al. (2015). Hidden Technical Debt in Machine Learning Systems. "
    "<i>Advances in Neural Information Processing Systems</i>, 28. (Source for train–serve skew framing.)",
]
for r in refs:
    story.append(P(r, style_ref))

# ---------------- build ----------------
doc.build(story)
print(f"PDF generated: {OUT_PDF}")
print(f"Pages: {len(__import__('pypdf').PdfReader(str(OUT_PDF)).pages)}")