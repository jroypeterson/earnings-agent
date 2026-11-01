"""Board #661: a revenue actual for a different PERIOD than its estimate is not a beat or a miss.

The vendors give an actual and an estimate with no period attached. Smith & Nephew reported its first
half on 2026-08-04: actual $3,128.8M against a quarterly estimate of $1,640M, posted as a +90.8% beat.
argenx's 2026-07-23 row has the same shape (1.73x). A half-year reporter has no Q2 figure to give, so
nothing upstream can repair the pair; the comparison has to be refused where it is made.

The test is a ratio near a whole number of quarters (2, 3 or 4 -- or 1/2, 1/3, 1/4 the other way) on a
company whose quarterly revenue estimate is at least $250M. The size gate is what makes it safe: a
small biotech can really double its estimate (Cytokinetics' launch, 2026-05-05: $19.4M vs $8.7M), a
quarter worth $250M+ does not. Measured 2026-11-01 over the 677 reported rows with an estimate that
large: SNN, ARGX and UCB (Belgian UCB's row was another company's print, #712) and nothing else. The
nearest real surprise, Sarepta's 1.51x on 2026-05-06, stays a beat.

Every surface that prints a revenue surprise imports this one function: the #earnings card, the
daily summary, season progress, and earnings_review's review card.
"""
from __future__ import annotations

MIN_REV_ESTIMATE = 250e6   # USD; below it a doubling can be a real quarter
TOLERANCE = 0.15           # ARGX's 1.73x is 13.5% from 2x; Sarepta's real 1.51x is 24.5% away
_PERIODS = ((2, "half-year"), (3, "nine-month"), (4, "full-year"))


def rev_period_mismatch(actual: float | None, estimate: float | None) -> str | None:
    """Why this revenue actual and estimate are not the same period, or None when they can be compared."""
    if actual is None or estimate is None or actual <= 0 or estimate < MIN_REV_ESTIMATE:
        return None
    ratio = actual / estimate
    for k, label in _PERIODS:
        if abs(ratio / k - 1) <= TOLERANCE:
            return f"actual is {ratio:.2f}x the estimate, the size of a {label} figure against a quarterly one"
        if abs(ratio * k - 1) <= TOLERANCE:
            return f"actual is {ratio:.2f}x the estimate, the size of a quarterly figure against a {label} one"
    return None
