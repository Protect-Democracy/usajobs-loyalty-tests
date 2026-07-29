#!/usr/bin/env python3
"""One-time backfill: derive the flat columns, then drop MatchedObjectDescriptor.

The raw descriptor was ~26 KB/row and 98.9% of the parquet bytes, which pushed
current_jobs_2026.parquet past 1 GB and started OOM-ing the CI runner during
save_jobs_to_parquet (runs 30380189935 and 30433491764). This rewrites each
current_jobs_*.parquet with the columns job_fields.derive_job_fields() produces
and without the blob.

Streams one row group at a time, so peak memory is a row group rather than the
whole file — the thing that was killing the runner in the first place.

The raw JSON is not lost: the sibling usajobs_historical project mirrors its
parquets (blob included) to r2://usajobs-data/data/.

Usage:
    python backfill_drop_descriptor.py --data-dir ../../data
    python backfill_drop_descriptor.py --data-dir ../../data --dry-run
"""
import argparse
import glob
import json
import os
import shutil
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from job_fields import DERIVED_COLUMNS, derive_job_fields

BLOB_COLUMN = "MatchedObjectDescriptor"
BOOL_COLUMNS = {"hasMonsterLink", "usesUsastaffing", "mentionsQuestionnaire"}

# Columns derive_job_fields() also produces that may already hold good values on
# rows whose blob is missing. For those rows we keep what's already there rather
# than blanking it.
PRESERVE_IF_NO_BLOB = ("serviceType", "payScale", "minimumGrade", "maximumGrade")


def _derived_array(values, column):
    if column in BOOL_COLUMNS:
        return pa.array(values, type=pa.bool_())
    return pa.array([None if v is None else str(v) for v in values], type=pa.string())


def convert_batch(table):
    """Return `table` with derived columns filled in and the blob dropped."""
    blobs = table.column(BLOB_COLUMN).to_pylist()
    rest = table.drop([BLOB_COLUMN])
    rows = rest.to_pylist()

    derived_rows = []
    for blob, row in zip(blobs, rows):
        if blob:
            try:
                mod = json.loads(blob)
            except (ValueError, TypeError):
                mod = {}
        else:
            mod = {}

        fields = derive_job_fields(mod, extra_text=str(row))

        if not blob:
            # No descriptor to derive from — don't clobber values the row
            # already carries, and fall back to grepping the row itself for
            # links, which is what the extractor used to do for these rows.
            for column in PRESERVE_IF_NO_BLOB:
                if row.get(column) is not None:
                    fields[column] = row[column]

        derived_rows.append(fields)

    out = rest
    for column in DERIVED_COLUMNS:
        array = _derived_array([d[column] for d in derived_rows], column)
        if column in out.column_names:
            out = out.set_column(out.schema.get_field_index(column), column, array)
        else:
            out = out.append_column(column, array)
    return out


def convert_file(path, dry_run=False, row_group_size=20000):
    source = pq.ParquetFile(path)
    if BLOB_COLUMN not in source.schema_arrow.names:
        print(f"  skip (no {BLOB_COLUMN} column): {os.path.basename(path)}")
        return None

    before_rows = source.metadata.num_rows
    before_bytes = os.path.getsize(path)
    tmp_path = path + ".backfill.tmp"

    writer = None
    written = 0
    try:
        for batch in source.iter_batches(batch_size=row_group_size):
            out = convert_batch(pa.Table.from_batches([batch]))
            if writer is None:
                writer = pq.ParquetWriter(tmp_path, out.schema, compression="snappy")
            writer.write_table(out)
            written += out.num_rows
            print(f"    {written:,}/{before_rows:,} rows", end="\r", flush=True)
    finally:
        if writer is not None:
            writer.close()

    if written != before_rows:
        os.remove(tmp_path)
        raise RuntimeError(
            f"row count changed for {path}: {before_rows} -> {written}. Refusing to replace."
        )

    after_bytes = os.path.getsize(tmp_path)
    name = os.path.basename(path)
    print(
        f"  {name}: {before_rows:,} rows, "
        f"{before_bytes / 1048576:.1f} MB -> {after_bytes / 1048576:.1f} MB "
        f"({100 * (1 - after_bytes / before_bytes):.1f}% smaller)"
    )

    if dry_run:
        os.remove(tmp_path)
    else:
        shutil.move(tmp_path, path)

    return before_bytes, after_bytes, before_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="../../data")
    parser.add_argument("--dry-run", action="store_true", help="Report sizes, don't replace files")
    args = parser.parse_args()

    paths = sorted(glob.glob(os.path.join(args.data_dir, "current_jobs_*.parquet")))
    paths += sorted(glob.glob(os.path.join(args.data_dir, "historical_jobs_*.parquet")))
    if not paths:
        print(f"No parquet files found in {args.data_dir}")
        return 1

    total_before = total_after = 0
    for path in paths:
        result = convert_file(path, dry_run=args.dry_run)
        if result:
            before, after, _ = result
            total_before += before
            total_after += after

    if total_before:
        print(
            f"\nTotal: {total_before / 1048576:.1f} MB -> {total_after / 1048576:.1f} MB "
            f"({100 * (1 - total_after / total_before):.1f}% smaller)"
        )
    if args.dry_run:
        print("Dry run — no files were replaced.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
