# Consensus Metrics Preview (SA Wave 1 §9A) — v1 build spec (Fable, 2026-07-12)

Composed from `earnings_agent`. Target: StreetAccount §9A pre-earnings brief, one per upcoming covered reporter. Ship before Q2 season 2026-07-14.

## v1 scope
**(A) ships on FREE/cached data (yfinance + SQLite + coverage exports):** header (company, event_date, BMO/AMC badge, last price ± day change); mean consensus Rev+EPS (DB events table, 0 API calls); QTD price since prior quarter-end (2026-06-30) for TICKER/SPY/sector-ETF; options-implied move (ATM straddle, §2); last-4-quarter post-earnings moves (DB reported events → `fetch_post_earnings_move`, backfill via `yf get_earnings_dates`); EPS beat rate up to 20q (yfinance `get_earnings_dates` Surprise%>0); conf-call line from `events.call_datetime_utc`.
**(B) include-if-FMP-Starter-serves-it else "n/a (FMP endpoint unavailable)":** estimate count+low/high range (`/api/v3/analyst-estimates/{t}?period=quarter`); revenue beat rate (`/api/v3/historical/earning_calendar/{t}`).
**(C) deferred v2:** sector-specific sub-metric tree (no fleet consensus source — data problem not code); IV-based expected move; estimate-revision trend (DB already snapshots estimates daily → v2 diff).

## Options-implied move (ATM straddle mid ÷ spot on first expiry containing the reaction)
- reaction_date: bmo→event_date; amc/dmh/blank→next weekday after event_date (weekend-skip only).
- expiry = min(e in yf.Ticker(t).options if e >= reaction_date); none → n/a.
- spot S = last close (same 5d history call as header; not intraday fast_info).
- strike K = argmin|K−S| present in BOTH calls & puts; |K−S|/S>0.05 → n/a.
- leg mid: bid>0 & ask>=bid → (bid+ask)/2; elif lastPrice>0 → lastPrice + note "stale quote"; else n/a.
- implied_move_pct = (call_mid+put_mid)/S*100.
- sanity: 0.5 ≤ pct ≤ 40 else n/a "implausible straddle". Annotate: wide market if (ask−bid)/mid>0.6; "(includes N extra days of time value)" if expiry−reaction>7d.
- render with provenance: `Options imply ~3.9% move (Jul 17 exp straddle)`.

## Module plan — new `consensus_preview.py`
Dataclasses: ImpliedMove(pct,expiry,method,notes); EstimateRange(count,low,high); BeatRate(beats,total,source); PreviewRow(all fields + na_notes list). SECTOR_ETF map {Healthcare Services:XLV, MedTech:IHI, Large Pharma:XLV, Biotech:XBI, Fintech:XLF}; unknown→no ETF row+note.
Functions: select_upcoming_reporters(conn,coverage,*,days_ahead=3,max_tier=1,ticker=None); fetch_price_snapshot(ticker)->(last_close,1d%); fetch_quarter_performance(tickers,since)->dict (one batched yf.download incl SPY+ETFs); compute_implied_move(ticker,event_date,event_hour); fetch_recent_post_moves(conn,ticker,n=4); fetch_eps_beat_history(ticker,max_q=20); fetch_fmp_estimate_range(ticker,event_date,api_key); fetch_fmp_rev_beat_history(ticker,api_key,max_q=20); build_preview_blocks(rows,as_of); build_preview_fallback(rows,as_of); write_preview_export(rows,out_path,window).

## main.py (additive; run() untouched)
`--consensus-preview` flag + `--preview-ticker` + `--preview-days` (default 3). `run_consensus_preview(dry_run,days_ahead,ticker)`:
1. load_coverage → select_upcoming_reporters (Tier 1 only default).
2. dedup via kv_store `consensus_preview_posted:TICKER:EVENT_DATE`.
3. assemble PreviewRows (per-ticker try/except → field-level n/a, never drop row).
4. FMP only if FMP_API_KEY set AND kv_store `fmp_preview_endpoints_ok`=="true".
5. write_preview_export → exports/consensus_preview.json (even dry-run).
6. non-dry: post_slack(SLACK_WEBHOOK_STREET_ACCOUNT,...) THEN mark kv_store (post-then-mark).
7. dry: print fallback+block JSON, no Slack, no kv marks.
config.py: add SLACK_WEBHOOK_STREET_ACCOUNT (secret name exists in sa-monitor). Missing webhook at post → raise.

## Output: exports/consensus_preview.json mirrors upcoming_events.json contract
schema_version=1, source="earnings-agent", generated_at, window{start,end}, counts{previews}, previews[]{ticker,company_name,tier,event_date,event_hour,call_datetime_utc,last_price,day_change_pct,consensus{eps{mean,count,low,high},revenue{...}},qtd{since,ticker_pct,spx_pct,sector_etf,sector_etf_pct},implied_move{pct,expiry,method,notes},post_earnings_moves[],beat_rates{eps{beats,total,source},revenue},na_fields[{field,reason}]}. Gated fields null + machine-readable na_fields entry.

## FMP probe (one shot, 2 calls, JPM): 
1. `/api/v3/analyst-estimates/JPM?period=quarter&limit=4` — PASS=200+numberAnalystsEstimatedEps>0+quarterly-spaced dates.
2. `/api/v3/historical/earning_calendar/JPM?limit=24` — PASS=200+non-null eps/epsEstimated AND revenue/revenueEstimated.
Record kv_store `fmp_preview_endpoints_ok`. Gate features independently if desired.

## Verify tonight (free, no Slack spam): dry-run single Tier-1 reporter; implied-move BMO vs AMC expiry selection; thin-options degradation; full-window dry-run count + dedup-not-marked-in-dryrun; one live post then no-op re-run; FMP probe (2 calls) then gate flip.
