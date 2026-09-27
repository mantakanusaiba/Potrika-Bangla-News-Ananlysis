#!/usr/bin/env python3
"""Reconcile Potrika local CSV ingestion by file. Requires the original RawDataset.

Examples:
    python audit_potrika_rows.py --data-dir '/path/to/RawDataset'
    python audit_potrika_rows.py --data-dir '/path/to/RawDataset' --spark

The CSV pass identifies trailing/embedded blank records, duplicated header records,
and schema-width anomalies; --spark repeats the notebook's precise Spark reader
settings and counts each source file. A four-row cause is NOT asserted merely
because there are four suspect records; compare with the actual release/manifest.
"""
import argparse
import csv
from collections import Counter
from pathlib import Path
import sys


def normalized(items):
    return tuple(''.join(ch.lower() for ch in x.strip() if ch.isalnum()) for x in items)


def count_csv(path, root, candidate_writer):
    rel = path.relative_to(root).as_posix()
    record_counts = Counter()
    field_widths = Counter()
    first_header = None
    index = 0
    with path.open('r', encoding='utf-8-sig', errors='replace', newline='') as fp:
        reader = csv.reader(fp)
        try:
            header = next(reader)
        except StopIteration:
            return dict(file=rel, parsed_data_rows=0, likely_data_rows=0,
                        blank_rows=0, repeated_headers=0, width_mismatches=0,
                        header_width=0, header='')
        first_header = normalized(header)
        for row in reader:
            index += 1
            record_counts['parsed_data_rows'] += 1
            if not row or all(not str(x).strip() for x in row):
                reason = 'blank_record'
            elif len(row) == len(header) and normalized(row) == first_header:
                reason = 'repeated_header'
            elif len(row) != len(header):
                reason = 'field_count_mismatch'
            else:
                reason = None
            if reason:
                record_counts[reason] += 1
                candidate_writer.writerow([rel, index, reader.line_num, reason,
                                           len(row), len(header), repr(row[:5])[:160]])
            if reason not in ('blank_record', 'repeated_header'):
                record_counts['likely_data_rows'] += 1
            field_widths[len(row)] += 1
    return dict(file=rel,
                parsed_data_rows=record_counts['parsed_data_rows'],
                likely_data_rows=record_counts['likely_data_rows'],
                blank_rows=record_counts['blank_record'],
                repeated_headers=record_counts['repeated_header'],
                width_mismatches=record_counts['field_count_mismatch'],
                header_width=len(header), header=' | '.join(header)[:350])


def spark_counts(paths):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    spark = (SparkSession.builder.master('local[*]')
             .appName('PotrikaPerFileIngestionAudit').getOrCreate())
    try:
        parsed = (spark.read.option('header', True)
                  .option('multiLine', True)
                  .option('quote', '"')
                  .option('escape', '"')
                  .option('mode', 'PERMISSIVE')
                  .option('encoding', 'UTF-8')
                  .csv([str(p) for p in paths])
                  .withColumn('source_file', F.input_file_name()))
        # Notebook counts raw rows after selecting/renaming; selection does not filter.
        rows = parsed.groupBy('source_file').count().collect()
        by_path = {str(Path(r['source_file'].replace('file://', '')).resolve()):
                   int(r['count']) for r in rows}
        return by_path
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', required=True, type=Path,
                        help='Extracted original Potrika RawDataset directory')
    parser.add_argument('--out-dir', default=Path('potrika_row_audit'), type=Path)
    parser.add_argument('--expected-published', type=int, default=664880)
    parser.add_argument('--expected-local', type=int, default=664884)
    parser.add_argument('--spark', action='store_true',
                        help='Reproduce notebook Spark reader (requires PySpark 4.0.4)')
    args = parser.parse_args()
    root = args.data_dir.resolve()
    paths = sorted(root.rglob('*.csv'))
    if not paths:
        parser.error('No CSV files found under ' + str(root))
    args.out_dir.mkdir(parents=True, exist_ok=True)
    csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
    suspects_path = args.out_dir / 'suspect_rows.csv'
    with suspects_path.open('w', encoding='utf-8-sig', newline='') as fp:
        writer = csv.writer(fp)
        writer.writerow(['file','data_record_index','end_physical_line','reason',
                         'actual_fields','expected_fields','row_preview'])
        results = [count_csv(path, root, writer) for path in paths]
    if args.spark:
        per_file = spark_counts(paths)
        for item, path in zip(results, paths):
            item['spark_rows'] = per_file.get(str(path.resolve()), 0)
            item['spark_minus_csv_data'] = (item['spark_rows'] -
                                            item['parsed_data_rows'] +
                                            item['blank_rows'])
    manifest = args.out_dir / 'file_row_audit.csv'
    with manifest.open('w', encoding='utf-8-sig', newline='') as fp:
        writer = csv.DictWriter(fp, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    sums = {key: sum(int(r.get(key, 0)) for r in results)
            for key in ('parsed_data_rows','likely_data_rows','blank_rows',
                        'repeated_headers','width_mismatches','spark_rows')}
    text = [f'Files audited: {len(paths)}',
            f'Published count to reconcile: {args.expected_published:,}',
            f'Prior local ingestion: {args.expected_local:,}',
            f'Prior discrepancy: {args.expected_local - args.expected_published:+,}',
            f"CSV records after one header per file (including blank): {sums['parsed_data_rows']:,}",
            f"Blank CSV records: {sums['blank_rows']:,}",
            f"Embedded duplicate header records: {sums['repeated_headers']:,}",
            f"Field-count mismatches: {sums['width_mismatches']:,}",
            f"CSV records excluding blank/header repeats: {sums['likely_data_rows']:,}"]
    if args.spark:
        text += [f"Spark-ingested rows: {sums['spark_rows']:,}",
                 f"Spark versus notebook's recorded 664,884: {sums['spark_rows'] - args.expected_local:+,}"]
    text += [f'Per-file results: {manifest}', f'Suspect records: {suspects_path}',
             'No attribution is established without checking suspect rows against the source release/manifest.']
    (args.out_dir / 'audit_summary.txt').write_text('\n'.join(text)+'\n', encoding='utf-8')
    print('\n'.join(text))


if __name__ == '__main__':
    main()
