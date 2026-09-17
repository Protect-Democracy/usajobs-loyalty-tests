# USAJobs Essay Site

Static website, generated daily, that tracks the use of essay questions within job applications.

**Dashboard (updated daily)**: https://usajobsloyaltytests.netlify.app/

The system:
- Daily scrapes questionnaires from USAStaffing and Monster Government
- Identifies jobs asking "How would you help advance the President's Executive Orders and policy priorities in this role?"
- Shows trends by agency, location, grade level, and time
- Updates automatically via GitHub Actions

This site uses data from the [USAJobs API](https://developer.usajobs.gov/) but **is not an official USAJobs project**.

## Setup

1. **Install Git LFS (Large File Storage):**
   This project uses Git LFS for parquet files. Install it before cloning:
   ```bash
   # macOS
   brew install git-lfs
   
   # Ubuntu/Debian
   apt-get install git-lfs
   
   # Windows
   choco install git-lfs
   ```
   
   After cloning the repository:
   ```bash
   git lfs install
   git lfs pull
   ```

2. **Create virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Create .env file (only needed for current jobs collection):**
   ```bash
   # .env
   USAJOBS_API_TOKEN=your_api_token_here  # Get from https://developer.usajobs.gov/
   ```
   
   **Note:** The API key is only required for collecting current jobs. Historical data collection does not require authentication.

## Usage

### Run Pipeline

**Workflow for data updates:**

```bash
# Collect current jobs
cd src/generate_data
python ./run_data_pipeline.py
```

**Workflow for questionnaire site updates:**

```bash
# Collect current jobs
cd src/generate_site
python ./run_questionnaire_pipeline.py
```

**Historical data collection (if needed):**
- Single year: [src/generate_data/run_single.sh](./src/generate_data/run_single.sh)
- Multiple years: [src/generate_data/run_parallel.sh](./src/generate_data/run_parallel.sh)

```bash
# Single year:
src/generate_data/run_single.sh range 2024-01-01 2024-12-31

# Multiple years:
src/generate_data/run_parallel.sh 2020 2021 2022
```

### Data Storage

- **Parquet Files**: Storage format
  - `historical_jobs_YEAR.parquet`: Historical job announcements by year
  - `current_jobs_YEAR.parquet`: Current job postings by year
- **Logs**: Stored in `logs/` directory with aggressive data gap detection

## Known Limitations

**Some agency-branded questionnaire portals block automated scraping and must be
backfilled manually — this cannot be fixed in the daily GitHub Actions pipeline.**

A few agencies host their own USAStaffing-style questionnaire portal instead of
using `apply.usastaffing.gov` or Monster Government directly (e.g. the FAA at
`jobs.faa.gov`). The daily pipeline can *discover* that a link exists for these
postings just fine, but the portal itself blocks a launched/headless browser at
the network level — confirmed on a dev laptop, a residential network, and
GitHub Actions' own runners alike, while a normal hand-driven browser loads the
same page instantly. There's no human in the loop on a scheduled CI run to get
past that, so these postings will always show as `no_questionnaire_link_found`
(or, once discovered but not yet scraped, sit indefinitely unscraped) until
someone runs the backfill below by hand.

To backfill these:

```bash
# 1. Start a debug Chrome and load the target portal once by hand, to establish
#    a normal, non-automated session:
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
    --remote-debugging-port=9222 --user-data-dir="$HOME/chrome-debug"
# (visit e.g. https://jobs.faa.gov in that window)

# 2. Run the backfill — it attaches to that Chrome over the DevTools Protocol
#    instead of launching its own browser:
cd src/generate_site
python3 backfill_blocked_portal_questionnaires.py
```

See that script's module docstring for details, and
`questionnaire_utils.py`'s `_AGENCY_QUESTIONNAIRE_URL_PATTERNS` / `BLOCKED_DOMAINS`
in the backfill script to add a newly-identified blocked portal.

## Contributing

See [docs/CONTRIBUTING.md](./docs/CONTRIBUTING.md).

## License

Licensed under the [LGPL 3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html); see [LICENSE.txt](./LICENSE.txt) for details.
