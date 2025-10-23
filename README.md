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
python update/update_current.py      # Update data
```

**Historical data collection (if needed):**
- Single year: [scripts/run_single.sh](./scripts/run_single.sh)
- Multiple years: [scripts/run_parallel.sh](./scripts/run_parallel.sh)

```bash
# Single year:
scripts/run_single.sh range 2024-01-01 2024-12-31

# Multiple years:
scripts/run_parallel.sh 2020 2021 2022
```

### Data Storage

- **Parquet Files**: Storage format
  - `historical_jobs_YEAR.parquet`: Historical job announcements by year
  - `current_jobs_YEAR.parquet`: Current job postings by year
- **Logs**: Stored in `logs/` directory with aggressive data gap detection

## Contributing

See [docs/CONTRIBUTING.md](./docs/CONTRIBUTING.md).

## License

Licensed under the [LGPL 3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html); see [LICENSE.txt](./LICENSE.txt) for details.
