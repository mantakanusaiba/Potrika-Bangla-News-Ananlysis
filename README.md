# Bangla News Media Behaviour Analysis (Potrika)

Analysis code and supplementary result tables for *"Explainable and
Robustness-Checked Analysis of Bangla News Media Behaviour: Publication
Spikes, Cross-Newspaper Lead–Lag Structure, and Headline–Body Divergence."*

This repository accompanies the manuscript and supports the paper's Data
and Code Availability statement. It does not redistribute the underlying
Potrika newspaper archive.

## Contents

- **`potrika_analysis.ipynb`** — the complete analysis pipeline: ingestion
  and schema auditing, cleaning and deduplication, TF–IDF and LDA topic
  modeling (including the held-out evaluation grid), rolling publication-spike
  detection with same-weekday and event-calendar robustness checks,
  cross-newspaper lead–lag analysis with BH-FDR correction, and headline–body
  lexical divergence with suffix-normalization and headline-length checks.
  Code cells are included; execution outputs have been cleared.
- **`audit_potrika_rows.py`** — an independent per-file row-provenance
  utility for the 63 original Potrika source CSVs. It flags blank rows,
  duplicate headers, and schema-width anomalies, and can reproduce the
  Spark ingestion row count when run with `--spark`. This is the tool
  referenced in the manuscript for resolving the unresolved four-row
  discrepancy described below.
- **`results/leadlag_bh_summary.csv`** — per-series summary of the
  BH-FDR-adjusted lead–lag analysis (Table V in the manuscript): number of
  eligible pairs, pairs with best lag at zero, positive non-zero selected
  lags, and BH-confirmed positive leads, by series (Overall, National,
  International, Sports, Economy).
- **`results/economy_confirmed_pairs.csv`** — the three Economy source pairs
  with a positive non-zero best-lag correlation surviving BH-FDR correction
  across the full 412-test family (Table VI). `lag` is the signed lag from
  the pairwise correlation search; `gap_days` is its absolute value, matching
  the "Gap (d)" column as printed in the manuscript. The
  Somoyer Alo–Kaler Kontho pair is flagged in the manuscript as a
  high-sensitivity estimate due to Somoyer Alo's short archive coverage
  (359 active days) and should be treated with additional caution.
- **`figures/`** — the manuscript's figures, as used in the paper.

## Running the analysis

The notebook uses PySpark (tested with Apache Spark 4.0.4) and standard
Python data-science packages (pandas, numpy, pyarrow, matplotlib). It expects
the raw Potrika archive (63 source CSV files) in a local directory; the
archive itself is not included here (see below).

```bash
pip install pyspark==4.0.4 pandas numpy pyarrow matplotlib
jupyter notebook potrika_analysis.ipynb
```

Set the dataset directory as instructed in the notebook's setup cell before
running the full pipeline.

## Data availability

This repository does not include the Potrika newspaper archive. The dataset
is independently published by Ahmad et al. and is available under
CC BY 4.0 from Mendeley Data (version 4):
https://data.mendeley.com/datasets/v362rp78dc/4

## Known open issue: row-count discrepancy

Local ingestion of the 63 original source files recorded 664,884 rows, four
more than the 664,880 reported in the upstream publication. The original
source files and a per-file row manifest were not retained alongside this
repository, so the four extra rows cannot currently be attributed to
specific files or classified as repeated headers, blank records, or parser
differences. This is reported in the manuscript as an open provenance item,
not a claimed revision of the published corpus size.

To investigate, run `audit_potrika_rows.py` against the original 63 source
files:

```bash
python audit_potrika_rows.py --data-dir /path/to/RawDataset --spark
```

Review its per-file summary (`file_row_audit.csv`, `suspect_rows.csv`)
against the actual released files before drawing any conclusion. If a
specific cause is identified, please open an issue or pull request — this
would let the manuscript's provenance note be updated with a confirmed
explanation.

## Citation

If you use this code, please cite the manuscript (details to be added on
acceptance) and the original Potrika dataset:

> I. Ahmad, F. AlQurashi, and R. Mehmood, "Potrika: Raw and balanced
> newspaper datasets in the Bangla language with eight topics and five
> attributes," arXiv:2210.09389, 2022.

## License

Code in this repository is released under the MIT License (see `LICENSE`).
This license covers the analysis code only; it does not extend to the
Potrika dataset itself, which remains subject to its own CC BY 4.0 terms and
any applicable rights of the original newspaper publishers.
