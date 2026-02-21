#!/bin/bash
set -e

# Run the full questionnaire pipeline locally and push to main
# Uses caffeinate to keep the machine awake during long scraping

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

echo "=== Local Questionnaire Pipeline ==="
echo "Started at: $(date)"
echo ""

# Use the abigailhaddad-2 gh account for PR/workflow operations
gh auth switch --user abigailhaddad-2 2>/dev/null || true
echo "gh CLI account: $(gh auth status 2>&1 | grep 'Active account' -B1 | head -1 | xargs)"

# Ensure we're on data-fixes branch with latest
git checkout data-fixes
git pull origin data-fixes || true

# Activate venv if it exists
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    echo "Using venv Python: $(which python)"
else
    echo "No .venv found, using system Python"
fi

# Step 1: Run questionnaire extraction + scraping (the slow part)
echo ""
echo "=== Step 1: Extract and scrape questionnaires ==="
cd src/generate_site
python run_questionnaire_pipeline.py
echo "Pipeline finished at: $(date)"

# Step 2: Run tests
echo ""
echo "=== Step 2: Run questionnaire tests ==="
python test_questionnaire_artifacts.py
TEST_EXIT=$?

if [ $TEST_EXIT -ne 0 ]; then
    echo "❌ Tests failed! Not pushing."
    exit 1
fi

# Step 3: Commit everything
echo ""
echo "=== Step 3: Commit and push ==="
cd "$REPO_ROOT"

if [ -n "$(git status --porcelain)" ]; then
    git add .
    git commit -m "Update questionnaire analysis - $(date +'%Y-%m-%d')

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
    git push origin data-fixes
    echo "✅ Pushed to data-fixes"
else
    echo "No changes to commit"
fi

# Step 4: Merge PR #230 to main
echo ""
echo "=== Step 4: Merge to main ==="
gh pr merge 230 --merge
echo "✅ Merged to main"

# Step 5: Trigger the daily questionnaire analysis workflow (deploys to Netlify)
echo ""
echo "=== Step 5: Trigger daily questionnaire analysis workflow ==="
gh workflow run daily-questionnaire-analysis.yml
echo "✅ Workflow triggered — check https://github.com/Protect-Democracy/usajobs-loyalty-tests/actions"

echo ""
echo "=== Done at: $(date) ==="
