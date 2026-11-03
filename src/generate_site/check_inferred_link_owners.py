#!/usr/bin/env python3
"""Record which posting each inferred USAStaffing questionnaire link belongs to.

questionnaire_links.csv has links the scraper guessed (inferred_from_announcement or
inferred_from_posting_html) rather than found in the posting. A guess from the
announcement number can land on an earlier posting's questionnaire that reused the
number: on 2026-11-02, 881742600 was credited with 12891436, which belongs to
858413000. Each USAStaffing questionnaire's public JSON names its own posting
(controlNumber), so this script looks that up for every inferred USAStaffing link
and caches it in questionnaire_rechecks/inferred_link_owners.csv (qid,
owner_control_number, checked_date).

When an inferred link's questionnaire names a different posting, the link is
corrected in questionnaire_rechecks/inferred_link_corrections.csv, which
generate_website_json.py applies:
  replaced      USAStaffing's data names a questionnaire of this posting's own
                (usastaffing_ids.csv, any version in git history with
                --git-history): the link is replaced by that one. Its text is fetched
                now and saved to inferred_link_corrections/, and its flag comes from it.
  kept_shared   no questionnaire of its own is known and the owner has the same
                announcement number: one announcement split across postings
                sharing a questionnaire, so the link stays.
  dropped       no questionnaire of its own is known and the owner's announcement
                number differs (often a re-announcement, e.g. -019R vs -019): the
                link is dropped.

Owners come from questionnaire_rechecks/usastaffing_ids.csv where it has the ID,
otherwise from the JSON, two requests at a time (USAStaffing blocks fast clients;
see questionnaire_rechecks/README.md). Only IDs not yet cached are looked up, so a
daily run checks just the new ones. A questionnaire that no longer exists is cached
with owner_control_number "missing". Questionnaires on other portals (jobs.faa.gov,
Monster) don't name a posting and aren't checked.

Usage: python3 check_inferred_link_owners.py [--limit N] [--git-history]
"""
import argparse
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from questionnaire_utils import (extract_questionnaire_id, fetch_usastaffing_questionnaire,
                                 fetch_usastaffing_questionnaire_json)
from refresh_open_questionnaires import flags

HERE = Path(__file__).parent
LINKS = HERE / 'questionnaire_links.csv'
IDS = HERE / 'questionnaire_rechecks' / 'usastaffing_ids.csv'
OWNERS = HERE / 'questionnaire_rechecks' / 'inferred_link_owners.csv'
COLUMNS = ['qid', 'owner_control_number', 'checked_date']
CORRECTIONS = HERE / 'questionnaire_rechecks' / 'inferred_link_corrections.csv'
CORRECTION_TEXTS = HERE / 'questionnaire_rechecks' / 'inferred_link_corrections'
CORRECTION_COLUMNS = ['usajobs_control_number', 'announcement_number', 'questionnaire_url', 'owner_control_number',
                      'owner_announcement_number', 'action', 'own_questionnaire_url', 'own_has_loyalty_q',
                      'own_has_new_wording', 'checked_date']
USASTAFFING_URL = 'https://apply.usastaffing.gov/ViewQuestionnaire/{}'


def inferred_usastaffing_links(links):
    """Inferred links to USAStaffing questionnaires, with a qid column."""
    inferred = (links['inferred_from_announcement'].astype(str) == 'True') | (
        links['inferred_from_posting_html'].astype(str) == 'True')
    out = links[inferred & links['questionnaire_url'].str.contains('usastaffing.gov', na=False)].copy()
    out['qid'] = out['questionnaire_url'].map(lambda u: extract_questionnaire_id(u)[1])
    return out


def load_owners(path=OWNERS):
    if path.exists():
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    return pd.DataFrame(columns=COLUMNS)


def lookup(qid):
    raw, invalid = fetch_usastaffing_questionnaire_json(USASTAFFING_URL.format(qid))
    if raw is not None:
        return qid, str(json.loads(raw).get('controlNumber') or '')
    return qid, 'missing' if invalid else None  # None: couldn't fetch; tried again next run


def id_table(git_history=False):
    """qid -> control_number pairs from usastaffing_ids.csv; with git_history, from
    every committed version too (rows closed more than 90 days are dropped from it)."""
    tables = [pd.read_csv(IDS, dtype=str, keep_default_na=False)]
    if git_history:
        rel = IDS.relative_to(HERE.parent.parent)
        for commit in subprocess.run(['git', 'log', '--format=%H', '--', str(rel)], capture_output=True, text=True,
                                     check=True, cwd=HERE.parent.parent).stdout.split():
            from io import StringIO
            old = subprocess.run(['git', 'show', f'{commit}:{rel}'], capture_output=True, text=True, cwd=HERE.parent.parent)
            if old.returncode == 0:
                tables.append(pd.read_csv(StringIO(old.stdout), dtype=str, keep_default_na=False))
    return pd.concat(tables)[['qid', 'control_number']].drop_duplicates()


def correct_mismatches(links, owners, today, git_history=False):
    """Adds a row to inferred_link_corrections.csv for each inferred link whose
    questionnaire names a different posting and isn't there yet (module docstring)."""
    done = pd.read_csv(CORRECTIONS, dtype=str, keep_default_na=False) if CORRECTIONS.exists() else \
        pd.DataFrame(columns=CORRECTION_COLUMNS)
    owner_of = dict(zip(owners['qid'], owners['owner_control_number']))
    inferred = inferred_usastaffing_links(links)
    inferred['owner'] = inferred['qid'].map(owner_of)
    mismatched = inferred[inferred['owner'].notna() & ~inferred['owner'].isin(['', 'missing'])
                          & (inferred['owner'] != inferred['usajobs_control_number'])]
    seen = set(zip(done['usajobs_control_number'], done['questionnaire_url']))
    mismatched = mismatched[[k not in seen for k in zip(mismatched['usajobs_control_number'],
                                                         mismatched['questionnaire_url'])]]
    if mismatched.empty:
        print('No new inferred links naming a different posting')
        return
    pairs = pd.concat([id_table(git_history),
                       owners.rename(columns={'owner_control_number': 'control_number'})[['qid', 'control_number']]])
    own_qids = pairs.drop_duplicates().groupby('control_number')['qid'].apply(sorted)
    CORRECTION_TEXTS.mkdir(parents=True, exist_ok=True)
    rows = []
    for _, link in mismatched.iterrows():
        cn = link['usajobs_control_number']
        raw, _ = fetch_usastaffing_questionnaire_json(link['questionnaire_url'])
        owner_ann = str(json.loads(raw).get('announcementNumber') or '') if raw else ''
        row = {'usajobs_control_number': cn, 'announcement_number': link['announcement_number'],
               'questionnaire_url': link['questionnaire_url'], 'owner_control_number': link['owner'],
               'owner_announcement_number': owner_ann, 'checked_date': today}
        own = [q for q in own_qids.get(cn, []) if q != link['qid']]
        texts = {}
        for q in own:
            text, _ = fetch_usastaffing_questionnaire(USASTAFFING_URL.format(q))
            if text is not None:
                texts[q] = text
        if texts:
            for q, text in texts.items():
                (CORRECTION_TEXTS / f'usastaffing_{q}.txt').write_text(text, encoding='utf-8')
                f = flags(text)
                rows.append({**row, 'action': 'replaced', 'own_questionnaire_url': USASTAFFING_URL.format(q),
                             'own_has_loyalty_q': str(f['has_loyalty_q']),
                             'own_has_new_wording': str(f['has_new_wording'])})
        elif own:
            print(f'  {cn}: its own questionnaire {own} could not be fetched; tried again next run')
        else:
            same = owner_ann.strip() == link['announcement_number'].strip()
            rows.append({**row, 'action': 'kept_shared' if same else 'dropped'})
    out = pd.concat([done, pd.DataFrame(rows, columns=CORRECTION_COLUMNS)], ignore_index=True)
    out.to_csv(CORRECTIONS, index=False)
    new = pd.DataFrame(rows, columns=CORRECTION_COLUMNS)
    print(f'{len(mismatched):,} inferred links name a different posting: '
          f"{new['action'].value_counts().to_dict()}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--limit', type=int, default=None, help='look up at most this many IDs')
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--git-history', action='store_true',
                        help="also look for postings' own questionnaires in usastaffing_ids.csv's git history")
    args = parser.parse_args()

    links = pd.read_csv(LINKS, dtype=str, keep_default_na=False)
    wanted = set(inferred_usastaffing_links(links)['qid'])
    owners = load_owners()
    today = datetime.now(timezone.utc).date().isoformat()

    table = pd.read_csv(IDS, dtype=str, keep_default_na=False)
    from_table = table[table['qid'].isin(wanted - set(owners['qid']))]
    new_rows = [{'qid': q, 'owner_control_number': c, 'checked_date': today}
                for q, c in zip(from_table['qid'], from_table['control_number'])]
    todo = sorted(wanted - set(owners['qid']) - set(from_table['qid']))
    if args.limit is not None:
        todo = todo[:args.limit]
    print(f'{len(wanted):,} inferred USAStaffing questionnaires; {len(owners):,} cached, '
          f'{len(new_rows):,} from usastaffing_ids.csv, {len(todo):,} to look up', flush=True)

    failed = 0

    def save():
        all_rows = pd.concat([owners, pd.DataFrame(new_rows, columns=COLUMNS)], ignore_index=True)
        all_rows.drop_duplicates('qid', keep='last').sort_values('qid').to_csv(OWNERS, index=False)

    with ThreadPoolExecutor(args.workers) as pool:
        for i, (qid, owner) in enumerate(pool.map(lookup, todo), 1):
            if owner is None:
                failed += 1
            else:
                new_rows.append({'qid': qid, 'owner_control_number': owner, 'checked_date': today})
            if i % 500 == 0:
                save()
                print(f'  {i:,}/{len(todo):,} looked up, {failed:,} failed', flush=True)
    save()
    print(f'Done: {len(todo) - failed:,} looked up, {failed:,} failed (retried next run)', flush=True)
    correct_mismatches(links, load_owners(), today, args.git_history)


if __name__ == '__main__':
    main()
