"""Board #298, parts 1 and 3: the Biopharma results subgroup and the marker legend.

Part 2 (the guidance-vs-consensus square) is deliberately not covered here — it
is not built. See PLAN_298.md.

Every test here encodes a failure found in REVIEW, not in testing:
  * a sector check placed before the Large Pharma check silently relocates LLY;
  * a legend rendered last is the first thing squeezed off a crowded card;
  * a hardcoded tier label drifts from config.TIER_2_LABEL (it already had);
  * a capped card drops rows that callers then mark reported=1 forever;
  * an all-or-nothing subgroup check starves Tier 1 while Tier 2 renders.
"""

import re
from datetime import date

import config
from notifications import (
    _RESULT_MARKER_SLOTS,
    _SUBCATEGORY_ORDER,
    _results_subcategory,
    _results_tier_label,
    build_results_legend_text,
    build_results_fallback_text,
    build_results_slack_blocks,
    ResultRow,
    SLACK_MAX_BLOCKS,
)


def _row(ticker="X", position="", sector="", subsector="", tier=2):
    return ResultRow(
        ticker=ticker,
        company_name=f"{ticker} Inc",
        event_date="2026-08-26",
        event_hour=None,
        eps_actual=None,
        eps_estimate=None,
        rev_actual=None,
        rev_estimate=None,
        tier=tier,
        sector=sector,
        subsector=subsector,
        position=position,
    )


class TestBiopharmaSubgroup:
    def test_biopharma_no_longer_falls_to_other(self):
        """The actual ask: a Biopharma name outside Large Pharma had no home."""
        row = _row("XOMA", sector="Biopharma", subsector="Biotech")
        assert _results_subcategory(row) == "Biopharma"

    def test_large_pharma_does_not_migrate(self):
        """LLY is sector Biopharma / subsector Large Pharma.

        #298 asked only that biopharma stop falling into Other. Moving Large
        Pharma names into the new bucket would be an unrequested change, and
        is what happens if the sector check is placed first.
        """
        row = _row("LLY", sector="Biopharma", subsector="Large Pharma")
        assert _results_subcategory(row) == "Large Pharma"

    def test_position_still_outranks_the_new_sector_bucket(self):
        row = _row("LLY", position="Portfolio", sector="Biopharma", subsector="Biotech")
        assert _results_subcategory(row) == "Portfolio"

    def test_biopharma_is_ordered_between_large_pharma_and_other(self):
        assert "Biopharma" in _SUBCATEGORY_ORDER
        assert _SUBCATEGORY_ORDER.index("Large Pharma") < _SUBCATEGORY_ORDER.index("Biopharma")
        assert _SUBCATEGORY_ORDER.index("Biopharma") < _SUBCATEGORY_ORDER.index("Other")

    def test_every_subcategory_result_has_a_slot_in_the_order(self):
        """A label with no slot in _SUBCATEGORY_ORDER renders nowhere."""
        rows = [
            _row(sector="Biopharma", subsector="Biotech"),
            _row(sector="Biopharma", subsector="Large Pharma"),
            _row(sector="Healthcare Services"),
            _row(sector="MedTech"),
            _row(sector="Tech"),
            _row(position="Researching", sector="Consumer"),
        ]
        for r in rows:
            assert _results_subcategory(r) in _SUBCATEGORY_ORDER


class TestTierLabelDoesNotDrift:
    def test_tier2_label_matches_config(self):
        """It previously said 'HC Services + MedTech' while config had already
        added Core Biopharma — so the card could print a Biopharma subgroup
        under a header claiming Biopharma was absent."""
        assert _results_tier_label(2) == config.TIER_2_LABEL

    def test_tier1_and_fallback_match_config(self):
        assert _results_tier_label(1) == config.TIER_1_LABEL
        assert _results_tier_label(99) == config.TIER_3_LABEL

    def test_tier2_label_mentions_biopharma(self):
        assert "Biopharma" in _results_tier_label(2)


class TestLegend:
    def test_legend_names_every_marker_slot_in_order(self):
        text = build_results_legend_text()
        labels = [label for label, _ in _RESULT_MARKER_SLOTS]
        assert all(label in text for label in labels)
        positions = [text.index(label) for label in labels]
        assert positions == sorted(positions), "legend order must match render order"

    def test_legend_covers_all_three_squares(self):
        text = build_results_legend_text()
        assert "EPS" in text and "Revenue" in text and "Stock" in text

    def test_legend_survives_the_slack_block_cap(self):
        """The regression that motivated placing it in the first context block.

        On a crowded card the builder spends its whole block budget on
        results; a legend appended at the END would be the first thing
        squeezed out, exactly when the card is busiest.
        """
        sectors = [
            ("Biopharma", "Biotech"),
            ("Biopharma", "Large Pharma"),
            ("Healthcare Services", ""),
            ("MedTech", ""),
            ("Tech", ""),
        ]
        rows = []
        for i in range(3000):
            sector, subsector = sectors[i % len(sectors)]
            rows.append(
                _row(f"T{i:04d}", sector=sector, subsector=subsector, tier=(i % 3) + 1)
            )
        blocks = build_results_slack_blocks(rows, date(2026, 8, 26))
        # NEVER exceed the cap. `>=` would have accepted an oversized payload
        # (measured: one big subgroup produced 69 blocks vs Slack's hard 50)
        # as proof the cap worked.
        assert len(blocks) <= SLACK_MAX_BLOCKS, f"payload exceeds cap: {len(blocks)}"
        flat = repr(blocks)
        assert "Squares, left to right" in flat

    def test_legend_present_on_a_small_card_too(self):
        blocks = build_results_slack_blocks(
            [_row("UNH", sector="Healthcare Services")], date(2026, 8, 26)
        )
        assert "Squares, left to right" in repr(blocks)


class TestNoResultIsSilentlyDropped:
    """Both callers mark EVERY input row reported=1 after a successful post,
    so a row the card omits is never rendered again. Truncation therefore has
    to be visible, and the payload must never exceed Slack's hard limit."""

    def _big(self, n=3000):
        sectors = [
            ("Biopharma", "Biotech"), ("Biopharma", "Large Pharma"),
            ("Healthcare Services", ""), ("MedTech", ""), ("Tech", ""),
        ]
        rows = []
        for i in range(n):
            sector, subsector = sectors[i % len(sectors)]
            rows.append(_row(f"T{i:04d}", sector=sector, subsector=subsector,
                             tier=(i % 3) + 1))
        return rows

    @staticmethod
    def _texts(blocks):
        """Every rendered text string in the payload."""
        out = []
        for b in blocks:
            if "text" in b and isinstance(b["text"], dict):
                out.append(b["text"].get("text", ""))
            for el in b.get("elements", []) or []:
                out.append(el.get("text", ""))
        return out

    def _assert_all_accounted_for(self, rows, blocks):
        """The real invariant: every input ticker is either rendered, named in
        the overflow list, or covered by an explicit '+N more not shown' count.

        Checking block COUNT alone is what made the first version of these
        tests vacuous — they passed against the pre-fix implementation too.
        """
        flat = "\n".join(self._texts(blocks))
        tickers = {r.ticker for r in rows}
        missing = {t for t in tickers if t not in flat}
        if not missing:
            return
        m = re.search(r"\+(\d+) more not shown", flat)
        assert m, f"{len(missing)} ticker(s) vanished with no count stated"
        assert int(m.group(1)) == len(missing), (
            f"count says {m.group(1)} unnamed but {len(missing)} are actually missing"
        )

    def _assert_slack_limits(self, blocks):
        assert len(blocks) <= SLACK_MAX_BLOCKS, f"block count {len(blocks)}"
        for t in self._texts(blocks):
            assert len(t) <= 3000, f"section is {len(t)} chars, Slack rejects >3000"

    def test_payload_never_exceeds_slack_limits(self):
        rows = self._big()
        blocks = build_results_slack_blocks(rows, date(2026, 8, 26))
        self._assert_slack_limits(blocks)
        self._assert_all_accounted_for(rows, blocks)

    def test_single_huge_subgroup_does_not_blow_the_cap(self):
        """One 2,000-row subgroup produced 69 blocks (vs Slack's hard 50)
        because the old code appended all chunks before testing the cap — and
        then the compact overflow line measured 14,064 chars."""
        rows = [
            _row(f"B{i:04d}", sector="Biopharma", subsector="Biotech", tier=1)
            for i in range(2000)
        ]
        blocks = build_results_slack_blocks(rows, date(2026, 8, 26))
        self._assert_slack_limits(blocks)
        self._assert_all_accounted_for(rows, blocks)

    def test_overflow_actually_engages(self):
        """Guards against these tests going vacuous again: this input MUST
        reach the overflow path, or the assertions below prove nothing."""
        rows = [
            _row(f"B{i:04d}", sector="Biopharma", subsector="Biotech", tier=1)
            for i in range(2000)
        ]
        blocks = build_results_slack_blocks(rows, date(2026, 8, 26))
        flat = "\n".join(self._texts(blocks))
        assert "Also reported" in flat or "not shown" in flat, (
            "overflow never engaged — this fixture no longer tests anything"
        )

    def test_skipped_tiers_do_not_eat_the_overflow_reserve(self):
        """Codex round-2 High, verbatim: 1,108 Tier-1 rows plus one Tier-2 and
        one Tier-3 row produced 48 blocks ending in two dividers, with neither
        the later tickers nor any overflow notice surviving the final slice."""
        rows = [
            _row(f"B{i:04d}", sector="Biopharma", subsector="Biotech", tier=1)
            for i in range(1108)
        ]
        rows.append(_row("YYYY", sector="Tech", tier=2))
        rows.append(_row("ZZZZ", sector="Tech", tier=3))
        blocks = build_results_slack_blocks(rows, date(2026, 8, 26))
        self._assert_slack_limits(blocks)
        self._assert_all_accounted_for(rows, blocks)
        flat = "\n".join(self._texts(blocks))
        assert "YYYY" in flat and "ZZZZ" in flat, "later tiers vanished entirely"

    def test_no_trailing_divider_or_orphan_tier_header(self):
        rows = [
            _row(f"B{i:04d}", sector="Biopharma", subsector="Biotech", tier=1)
            for i in range(1108)
        ]
        rows.append(_row("ZZZZ", sector="Tech", tier=3))
        blocks = build_results_slack_blocks(rows, date(2026, 8, 26))
        assert blocks[-1].get("type") != "divider", "card ends on a dangling divider"

    def test_oversized_tier1_subgroup_still_fills_the_card(self):
        """Codex round-3 Medium: with 1,345 Tier-1 rows plus one Tier-2 row the
        all-or-nothing subgroup check discarded Tier 1 wholesale — the card
        collapsed to 7 blocks, Tier 2 rendered in full, and 41 blocks sat
        unused. A partial render must fill the budget in priority order.
        """
        rows = [
            _row(f"B{i:04d}", sector="Biopharma", subsector="Biotech", tier=1)
            for i in range(1345)
        ]
        rows.append(_row("ZZZZ", sector="Tech", tier=2))
        blocks = build_results_slack_blocks(rows, date(2026, 8, 26))
        self._assert_slack_limits(blocks)
        self._assert_all_accounted_for(rows, blocks)
        flat = "\n".join(self._texts(blocks))
        assert "Tier 1" in flat, "the highest-priority tier vanished entirely"
        rendered_t1 = sum(1 for r in rows if r.tier == 1 and f"`{r.ticker}`" in flat)
        assert rendered_t1 > 100, (
            f"only {rendered_t1} Tier-1 rows rendered in detail — the budget "
            f"was not filled in priority order"
        )
        assert len(blocks) >= SLACK_MAX_BLOCKS - 4, (
            f"only {len(blocks)} blocks used of {SLACK_MAX_BLOCKS}; budget left idle"
        )

    def test_small_card_has_no_overflow_notice(self):
        blocks = build_results_slack_blocks(
            [_row("UNH", sector="Healthcare Services")], date(2026, 8, 26)
        )
        assert "Also reported" not in repr(blocks)
        assert "not shown" not in repr(blocks)

    def test_production_shaped_batch_renders_everything(self):
        """~270 tracked names across tiers/subgroups — the real workload."""
        sectors = [
            ("Biopharma", "Biotech"), ("Biopharma", "Large Pharma"),
            ("Healthcare Services", ""), ("MedTech", ""), ("Tech", ""),
        ]
        rows = []
        for i in range(270):
            sector, subsector = sectors[i % len(sectors)]
            rows.append(_row(f"P{i:03d}", sector=sector, subsector=subsector,
                             tier=(i % 3) + 1))
        blocks = build_results_slack_blocks(rows, date(2026, 8, 26))
        self._assert_slack_limits(blocks)
        flat = "\n".join(self._texts(blocks))
        assert all(r.ticker in flat for r in rows), "a production-size card lost a name"
        assert "not shown" not in flat, "production-size card should not truncate"


class TestSectionCharLimitLayers:
    """Two layers keep a section under Slack's 3000-char limit: the chunker
    splits an oversized line, and _overflow_blocks emits short lines in the
    first place.

    End-to-end tests cannot distinguish them — either layer alone keeps the
    card valid — so each is exercised directly here. The overflow tests below
    still observe POST-chunker output (there is no pre-chunker seam to
    inspect); what they pin is the observable consequence of the short-line
    behaviour, namely that no ticker is severed mid-name.
    """

    def test_chunker_splits_an_oversized_single_line(self):
        from notifications import _chunk_result_section

        blocks = _chunk_result_section("header", ["x" * 10_000])
        assert len(blocks) > 1, "an oversized line was not split at all"
        for b in blocks:
            assert len(b["text"]["text"]) <= 3000

    def test_chunker_preserves_content_when_splitting(self):
        from notifications import _chunk_result_section

        payload = "abcdefghij" * 800  # 8,000 chars
        blocks = _chunk_result_section("header", [payload])
        joined = "".join(
            b["text"]["text"].replace("header", "").replace("\n", "") for b in blocks
        )
        assert joined == payload, "splitting an oversized line lost or reordered content"

    def test_overflow_blocks_emit_short_lines(self):
        from notifications import _overflow_blocks

        rows = [_row(f"T{i:04d}", sector="Tech") for i in range(2000)]
        blocks = _overflow_blocks(rows, budget=40)
        assert blocks
        for b in blocks:
            assert len(b["text"]["text"]) <= 3000

    def test_overflow_never_splits_a_ticker_across_blocks(self):
        """Why _overflow_blocks emits short lines even though the chunker can
        split an oversized one: the chunker's split is a blind character cut,
        so a single giant comma-joined line gets severed MID-TICKER and the
        card shows a name that does not exist.
        """
        from notifications import _overflow_blocks

        rows = [_row(f"TICK{i:04d}", sector="Tech") for i in range(2000)]
        blocks = _overflow_blocks(rows, budget=200)
        per_block = [b["text"]["text"] for b in blocks]
        for r in rows:
            assert any(r.ticker in text for text in per_block), (
                f"{r.ticker} was severed across a block boundary"
            )

    def test_overflow_states_a_count_when_it_must_truncate(self):
        from notifications import _overflow_blocks

        rows = [_row(f"T{i:04d}", sector="Tech") for i in range(2000)]
        blocks = _overflow_blocks(rows, budget=1)
        flat = "\n".join(b["text"]["text"] for b in blocks)
        assert "not shown" in flat, "truncated silently instead of stating a count"


class TestFallbackTextBeatCount:
    def test_missing_eps_is_not_counted_as_a_beat(self):
        """Ingestion admits a row when EITHER actual is present, so a
        revenue-only row is normal. It rendered ⬜ in the card but was counted
        as an EPS beat in the fallback."""
        r = _row("UNH", sector="Healthcare Services")
        r.rev_actual, r.rev_estimate = 100.0, 90.0  # revenue only
        text = build_results_fallback_text([r], date(2026, 8, 26))
        assert "0 beat" in text
        assert "1 n/a" in text

    def test_real_beat_and_miss_still_counted(self):
        beat = _row("A", sector="MedTech")
        beat.eps_actual, beat.eps_estimate = 2.0, 1.0
        miss = _row("B", sector="MedTech")
        miss.eps_actual, miss.eps_estimate = 1.0, 2.0
        text = build_results_fallback_text([beat, miss], date(2026, 8, 26))
        assert "1 beat" in text and "1 miss" in text
        assert "n/a" not in text


class TestMarkerSingleSourceOfTruth:
    def test_rendered_line_uses_the_slot_order(self):
        from notifications import _format_results_line, _render_result_markers

        r = _row("UNH", sector="Healthcare Services")
        r.eps_actual, r.eps_estimate = 2.0, 1.0   # beat  -> green
        r.rev_actual, r.rev_estimate = 1.0, 2.0   # miss  -> red
        markers = _render_result_markers(r)
        assert markers.split() == ["\U0001F7E9", "\U0001F7E5", "⚠️"]
        assert _format_results_line(r).startswith(markers)

    def test_slot_count_matches_rendered_marker_count(self):
        from notifications import _render_result_markers

        r = _row("UNH", sector="MedTech")
        assert len(_render_result_markers(r).split()) == len(_RESULT_MARKER_SLOTS)


if __name__ == "__main__":
    import sys

    failures = 0
    for cls in [
        TestBiopharmaSubgroup, TestTierLabelDoesNotDrift,
        TestLegend, TestMarkerSingleSourceOfTruth,
        TestNoResultIsSilentlyDropped, TestFallbackTextBeatCount,
    ]:
        inst = cls()
        for name in sorted(dir(inst)):
            if not name.startswith("test_"):
                continue
            try:
                getattr(inst, name)()
                print(f"  PASS  {cls.__name__}.{name}")
            except AssertionError as exc:
                failures += 1
                print(f"  FAIL  {cls.__name__}.{name}: {exc}")
    print(f"\n{'FAILED' if failures else 'ALL PASSED'} ({failures} failure(s))")
    sys.exit(1 if failures else 0)
