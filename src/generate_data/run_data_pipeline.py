#!/usr/bin/env python3
"""
Current data update script for USAJobs data pipeline

This script:
1. Collects current jobs
2. Provides summary of what was updated

Usage:
    python update/update_current.py
"""

import os
import sys
import json
import pandas as pd
import glob
import subprocess
from datetime import datetime, timedelta

# Global variable to store initial counts for diagnostics
initial_counts = {}

def run_command(command, description, stream_output=False):
    """Run a shell command and return success status and output"""
    print(f"🔄 {description}...")
    try:
        if stream_output:
            # Stream output in real-time for commands with progress bars
            result = subprocess.run(command, shell=True, check=True)
            print(f"✅ {description} completed")
            return True, ""  # No captured output when streaming
        else:
            # Capture output for parsing
            result = subprocess.run(command, shell=True, check=True,
                                  capture_output=True, text=True)
            print(f"✅ {description} completed")
            return True, result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        if not stream_output and hasattr(e, 'stderr'):
            print(f"Error output: {e.stderr}")
        return False, e.stderr if hasattr(e, 'stderr') else ""

def parse_collection_output(output):
    """Parse output from data collection scripts to extract stats"""
    import re

    stats = {
        'new_jobs': 0,
        'total_jobs': 0,
        'failed_dates': [],
        'errors': [],
        'jobs_per_file': {}  # New field to track jobs added per file
    }

    # Look for patterns in the output
    if 'new jobs total' in output:
        # Current API pattern: "Added 3 new jobs total"
        match = re.search(r'Added (\d+) new jobs total', output)
        if match:
            stats['new_jobs'] = int(match.group(1))

    if 'jobs saved' in output:
        # Historical API pattern: "123 jobs saved"
        match = re.search(r'(\d+) jobs saved', output)
        if match:
            stats['new_jobs'] = int(match.group(1))

    # Look for per-file patterns
    # Pattern 1: "Saved 123 jobs to /path/to/file.parquet"
    file_matches = re.findall(r'Saved (\d+) jobs to (.+\.parquet)', output)
    for count, filepath in file_matches:
        filename = os.path.basename(filepath)
        stats['jobs_per_file'][filename] = int(count)

    # Pattern 2: "current_jobs_2025.parquet: 123 jobs" (from final summary)
    summary_matches = re.findall(r'((?:current_jobs_\d+\.parquet): ([\d,]+) jobs', output)
    for filename, count in summary_matches:
        # This is total count, not new jobs, so skip if we already have data
        if filename not in stats['jobs_per_file']:
            # Remove commas from number
            stats['jobs_per_file'][filename] = int(count.replace(',', ''))

    # Look for error patterns
    if 'CRITICAL DATA ISSUE' in output or 'failed' in output.lower():
        # Extract failed dates if present
        failed_matches = re.findall(r'Failed.*?(\d{4}-\d{2}-\d{2})', output)
        stats['failed_dates'].extend(failed_matches)

        # Extract general errors
        error_lines = [line.strip() for line in output.split('\n')
                      if 'error' in line.lower() or 'failed' in line.lower()]
        stats['errors'].extend(error_lines[:3])  # Limit to first 3 errors

    return stats

def record_initial_file_sizes():
    """Record initial file sizes before data collection"""
    print("📏 Recording initial file sizes...")

    data_files = glob.glob('../../data/current_jobs_*.parquet')
    initial_sizes = {}

    for file in data_files:
        try:
            initial_size = os.path.getsize(file)
            initial_sizes[file] = initial_size
            print(f"📝 {file}: {initial_size:,} bytes")
        except Exception as e:
            print(f"⚠️  Could not read {file}: {e}")
            initial_sizes[file] = 0  # Assume new file

    return initial_sizes

def record_initial_job_counts():
    """Record initial job counts before data collection"""
    global initial_counts
    print("📊 Recording initial job counts...")

    data_files = glob.glob('../../data/current_jobs_*.parquet')

    for file in data_files:
        try:
            df = pd.read_parquet(file)
            count = len(df)
            initial_counts[file] = count
            print(f"📝 {os.path.basename(file)}: {count:,} jobs")
        except Exception as e:
            print(f"⚠️  Could not read {file}: {e}")
            initial_counts[file] = 0  # Assume new file or error

    return initial_counts

def save_initial_snapshot(file_path):
    """Save a snapshot of job IDs before collection for comparison"""
    try:
        df = pd.read_parquet(file_path)
        if 'usajobs_control_number' in df.columns:
            return set(df['usajobs_control_number'].dropna().astype(str))
        elif 'usajobsControlNumber' in df.columns:
            return set(df['usajobsControlNumber'].dropna().astype(str))
        else:
            return set()
    except:
        return set()

def diagnose_shrinkage(file_path, initial_count):
    """Diagnose why a file shrunk by comparing job IDs"""
    print(f"\n📋 Diagnosing {os.path.basename(file_path)}:")

    try:
        # Load current data
        current_df = pd.read_parquet(file_path)
        current_count = len(current_df)

        print(f"   Initial jobs: {initial_count:,}")
        print(f"   Current jobs: {current_count:,}")
        job_difference = current_count - initial_count
        if job_difference > 0:
            print(f"   Jobs added: {job_difference:,}")
        elif job_difference < 0:
            print(f"   Jobs lost: {-job_difference:,}")
        else:
            print(f"   Jobs unchanged")

        # Try to load the previous version from git to compare
        temp_file = file_path + '.temp'
        # Use git show with proper path handling
        git_path = os.path.relpath(file_path, start='..')  # Convert to relative path from repo root
        result = subprocess.run(['git', 'show', f'HEAD:{git_path}'],
                              capture_output=True, cwd='..')

        if result.returncode == 0 and result.stdout:
            # Write binary data properly
            with open(temp_file, 'wb') as f:
                f.write(result.stdout)
            old_df = pd.read_parquet(temp_file)
            os.remove(temp_file)

            # Get control numbers
            if 'usajobs_control_number' in old_df.columns:
                old_ids = set(old_df['usajobs_control_number'].dropna().astype(str))
                current_ids = set(current_df['usajobs_control_number'].dropna().astype(str))
            elif 'usajobsControlNumber' in old_df.columns:
                old_ids = set(old_df['usajobsControlNumber'].dropna().astype(str))
                current_ids = set(current_df['usajobsControlNumber'].dropna().astype(str))
            else:
                print("   ⚠️  No control number column found for comparison")
                return

            # Find missing jobs
            missing_ids = old_ids - current_ids
            new_ids = current_ids - old_ids

            print(f"   Jobs removed: {len(missing_ids):,}")
            print(f"   Jobs added: {len(new_ids):,}")

            if missing_ids and len(missing_ids) <= 10:
                print("\n   Examples of removed jobs:")
                sample_missing = list(missing_ids)[:10]

                # Get details of missing jobs
                if 'usajobs_control_number' in old_df.columns:
                    missing_jobs = old_df[old_df['usajobs_control_number'].isin(sample_missing)]
                else:
                    missing_jobs = old_df[old_df['usajobsControlNumber'].isin(sample_missing)]

                for _, job in missing_jobs.iterrows():
                    control_num = job.get('usajobs_control_number', job.get('usajobsControlNumber'))
                    title = job.get('positionTitle', 'Unknown')
                    agency = job.get('hiringAgencyName', 'Unknown')
                    open_date = job.get('positionOpenDate', 'Unknown')
                    print(f"   - {control_num}: {title} ({agency}) - opened {open_date}")
            elif missing_ids:
                print(f"\n   Too many removed jobs to list ({len(missing_ids):,} total)")
                print("   First 5 control numbers:", list(missing_ids)[:5])

        else:
            print("   ⚠️  Could not load previous version from git for detailed comparison")

    except Exception as e:
        print(f"   ❌ Error during diagnosis: {e}")

def calculate_job_additions(initial_counts):
    """Calculate how many jobs were added to each file"""
    print("📊 Calculating job additions...")

    data_files = glob.glob('../../data/current_jobs_*.parquet')
    job_additions = {}

    for file in data_files:
        try:
            df = pd.read_parquet(file)
            current_count = len(df)
            initial_count = initial_counts.get(file, 0)
            added = current_count - initial_count

            filename = os.path.basename(file)
            job_additions[filename] = added

            if added > 0:
                print(f"✅ {filename}: {added:,} jobs added (was {initial_count:,}, now {current_count:,})")
            else:
                print(f"ℹ️  {filename}: no new jobs (still {current_count:,})")

        except Exception as e:
            print(f"⚠️  Could not read {file}: {e}")
            job_additions[os.path.basename(file)] = 0

    return job_additions

def check_file_sizes_vs_initial(initial_sizes):
    """Check that data files haven't lost any jobs"""
    print("🔍 Checking data integrity (ensuring no job loss)...")

    data_files = glob.glob('../../data/current_jobs_*.parquet')
    size_checks = []
    files_changed = False
    shrunken_files = []
    suspicious_files = []

    for file in data_files:
        try:
            current_size = os.path.getsize(file)
            initial_size = initial_sizes.get(file, 0)

            # Also check job counts
            current_df = pd.read_parquet(file)
            current_count = len(current_df)
            initial_count = initial_counts.get(file, 0)

            # Only check job counts - file size doesn't matter
            count_decreased = current_count < initial_count
            jobs_change = current_count - initial_count
            bytes_change = current_size - initial_size

            if count_decreased:
                print(f"❌ {file} LOST JOBS: {initial_count:,} → {current_count:,} jobs ({jobs_change:,}), {initial_size:,} → {current_size:,} bytes")
                size_checks.append(False)
                shrunken_files.append(file)
            elif jobs_change > 0:
                print(f"✅ {file}: {initial_count:,} → {current_count:,} jobs (+{jobs_change:,}), {initial_size:,} → {current_size:,} bytes ({bytes_change:+,})")
                size_checks.append(True)
                files_changed = True
            else:
                print(f"✅ {file}: {current_count:,} jobs (unchanged), {current_size:,} bytes")
                size_checks.append(True)

        except Exception as e:
            print(f"❌ Could not check {file}: {e}")
            size_checks.append(False)

    # If files shrunk, provide detailed diagnostics
    if shrunken_files:
        print("\n🔍 DIAGNOSTIC INFORMATION FOR FILES WITH DATA LOSS:")
        for file in shrunken_files:
            diagnose_shrinkage(file, initial_counts.get(file, 0))

    if not all(size_checks):
        print("\n" + "🚨" * 40)
        print("🚨 CRITICAL: DATA LOSS DETECTED! ABORTING ALL OPERATIONS! 🚨")
        print("🚨" * 40)
        print("\n⚠️  Some data files lost jobs! This should NEVER happen!")
        print("⚠️  Refusing to commit or push changes to prevent data loss.")
        print("\n📋 Next steps:")
        print("  1. Check the diagnostic information above")
        print("  2. Restore data from git history if needed")
        print("  3. Fix the root cause before running again")
        print("  4. Consider running: git checkout -- ../../data/*.parquet")
        return False, False

    return True, files_changed

def main():
    print("🚀 USAJobs Data Pipeline - Current Data Update")
    print("=" * 50)

    # Step 1: Record initial file sizes and job counts
    initial_sizes = record_initial_file_sizes()
    initial_counts = record_initial_job_counts()

    # Initialize collection statistics
    total_new_jobs = 0
    all_failed_dates = []
    collection_errors = []
    all_jobs_per_file = {}  # Track jobs added per file

    # Step 2: Collect current jobs (always do this to get latest active postings)
    current_cmd = "python ./collect_current_data.py --data-dir ../../data"
    success, output = run_command(current_cmd, "Collecting current jobs", stream_output=True)

    if success:
        # When streaming, we need to calculate stats differently
        print(f"   📊 Current data collection completed")
    else:
        print("❌ Current data collection failed.")
        collection_errors.append("Current data collection failed")

    # Step 4: Check data file integrity
    print("\\n🔍 Checking data file integrity...")
    files_ok, files_changed = check_file_sizes_vs_initial(initial_sizes)
    if not files_ok:
        print("⚠️  Data file checks failed.")
        return

    if not files_changed:
        print("ℹ️  No data files changed. Skipping summary.")
        print("\\n🎉 Update completed - no changes detected!")
        return

    # Step 5: Calculate actual job additions
    job_additions = calculate_job_additions(initial_counts)


    # Step 6: Summary
    print("\\n" + "=" * 50)
    print("📊 UPDATE SUMMARY")
    print("=" * 50)

    try:
        # Show jobs added per file (using actual calculated additions)
        if job_additions:
            print("\\n📊 Jobs added per file:")
            for filename in sorted(job_additions.keys()):
                count = job_additions[filename]
                if count > 0:
                    print(f"   • {filename}: {count:,} jobs added")
                else:
                    print(f"   • {filename}: 0 jobs added")

    except Exception as e:
        print(f"❌ Could not read summary data: {e}")

    print("\\n🎉 Update completed successfully!")
    print("\\nNext steps:")
    print("   • Review any error logs in logs/ directory")

if __name__ == "__main__":
    # Ensure we're in the right directory
    if not os.path.exists('./collect_current_data.py'):
        print("❌ Please run this script from the src/generate_data/ directory")
        sys.exit(1)

    main()
