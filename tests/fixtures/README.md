# Fixtures

Post sequences from the events of Aug 31 – Sep 8, 2026 (CLAUDE.md §11):

- `khasab_vlcc.json` — Saudi VLCC Sidr + Liberian-flagged Senegal Prosperity
  struck by projectiles ~17nm off Khasab exiting the Strait of Hormuz (Aug 31).
- `kharg_strikes.json` — US strikes on Iranian tankers: M/T Downy off Kharg
  Island, M/T Stark 1 near Jask, M/T Kylo (Noxen) in the Gulf of Oman (Sep 5).
- `jazan_refinery.json` — Houthi drone/missile attack on Aramco's Jazan
  refinery and Abha distribution centre (Sep 8).
- `irgc_warning.json` — IRGC Navy warning to tanker crews near Kuwaiti and
  Bahraini ports to evacuate (Sep 8).
- `noise.json` — analysis / recap / engagement posts that must score < 50.

Each event sequence includes: the first OSINT post, a wire-service repost
hours later (expected DUPLICATE), an official confirmation (UPDATE), a denial
(UPDATE), and unrelated analysis (suppressed).

**Provenance:** the underlying facts (vessels, locations, times, actors) are
real, taken from contemporary reporting. The post *wording* is reconstructed
in each account's style — the original tweets were not retrievable verbatim
when these were written. Swap in real post text if you have it; the tests only
assert threshold side, category, and dedup label.

`expect` fields per post: `surfaced` (bool), `dedup` (NEW_EVENT | UPDATE |
DUPLICATE | null), `category_in` (acceptable categories), `max_score`
(optional, for noise posts).
