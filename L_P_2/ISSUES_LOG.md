# Project2 Issues Log
## Quality Audit Report | Version 1.0.0
**Audit Date:** 2026-02-01 | **Auditor:** Claude Code

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| CRITICAL | 4 | **ALL FIXED** |
| HIGH | 5 | NEEDS FIX |
| MEDIUM | 6 | RECOMMENDED |
| LOW | 3 | ADVISORY |

---

## CRITICAL ISSUES (Must Fix Before Public Release)

### ISS-001: P2-FED Date Range Misrepresentation
**Severity:** CRITICAL | **Project:** P2-FED | **Status:** FIXED

**Issue:** Reports claim "FY2024 (Oct 2023 - Sep 2024)" but actual data spans 2004-2027 (23 years).

**Evidence:**
- Data Dictionary states: "Data Coverage: FY2024 (Oct 2023 - Sep 2024)"
- Actual `kpis.json` shows spending for years 2004, 2005, 2006... through 2027
- FY2024 spend: $11.89B | Total all years: $67.14B

**Impact:** Public viewer could verify via USAspending.gov API and challenge the claim.

**Resolution Applied:**
- Updated PDF headers to "Multi-Year Contract Analysis (FY2004-2027)"
- Updated traceability panel parameters
- Updated Data Dictionary with explanatory note
- PDFs regenerated 2026-02-01

---

### ISS-002: P2-AIR Simulated Data Not Disclosed
**Severity:** CRITICAL | **Project:** P2-AIR | **Status:** FIXED

**Issue:** Flight data is SIMULATED but reports claim "BTS TranStats" as source without disclosure.

**Evidence:**
- Checkpoint states: "BTS TranStats | ✓ SIMULATED | 99,975"
- PDF reports state: "Data Sources: BTS TranStats + NOAA CDO API"
- No disclaimer that flight data is synthetic/simulated

**Impact:** Public viewer could attempt to verify flight records via transtats.bts.gov and find data doesn't match.

**Resolution Applied:**
- Added prominent disclaimer to all 3 P2-AIR PDFs
- Founder Summary: TL;DR now states "synthetic, BTS format"
- Executive Summary: Red warning in traceability panel
- Methodology: Note box explaining SYNTHETIC vs REAL data sources
- Data Dictionary updated with IMPORTANT NOTICE
- All footers updated to clarify "Synthetic flights (BTS format)"
- PDFs regenerated 2026-02-01

---

### ISS-003: P2-AIR Weather Impact Metrics Inconsistent
**Severity:** CRITICAL | **Project:** P2-AIR | **Status:** FIXED

**Issue:** Checkpoint claims "+11.6 min weather delay" but actual KPI shows +0.32 min.

**Evidence:**
- `Project2_CHECKPOINT.md`: "Weather Impact | +11.6 min delay"
- `kpis.json` weather_attribution: "weather_delay_delta": 0.32055443631736225

**Discrepancy:** 36x difference (11.6 vs 0.32)

**Impact:** This is a major inconsistency that would immediately undermine credibility.

**Resolution Applied:**
- Checkpoint updated to "+0.32 min avg delay"
- PDF TL;DR updated to show "+0.3 minute" (rounded)
- Language changed from "increases delays by X minutes" to "correlates with +X minute additional delay"
- Fixed 2026-02-01

---

### ISS-004: P2-AIR On-Time Rate Inconsistent
**Severity:** CRITICAL | **Project:** P2-AIR | **Status:** FIXED

**Issue:** Checkpoint and KPI data show different on-time rates.

**Evidence:**
- `Project2_CHECKPOINT.md`: "On-Time Rate | 73.9%"
- `kpis.json` reliability: "on_time_rate": 0.677629 = 67.8%

**Discrepancy:** 6.1 percentage point difference

**Resolution Applied:**
- Checkpoint updated to "67.8%" (matches actual KPI data)
- PDFs already pull from kpis.json so display correct value
- Fixed 2026-02-01

---

## HIGH PRIORITY ISSUES

### ISS-005: P2-SEC "Fortune 500" Claim Unverifiable
**Severity:** HIGH | **Project:** P2-SEC

**Issue:** Founder Summary claims "39 Fortune 500 companies" without verification.

**Evidence:**
- PDF text: "39 Fortune 500 companies"
- Company cohort includes mix of Fortune 500 and Fortune 1000

**Impact:** Claim could be challenged if any company in cohort isn't actually Fortune 500.

**Solution:**
1. Verify each company against current Fortune 500 list
2. Change wording to "39 large-cap public companies" or "Fortune 1000"
3. Or document the exact Fortune rank for each company

---

### ISS-006: P2-SEC Restatement Rate Not Highlighted
**Severity:** HIGH | **Project:** P2-SEC

**Issue:** Pipeline detected 83,541 restatements (17.4%) but this significant finding is not prominently displayed.

**Evidence:**
- `pipeline_metrics.json`: "83,541 potential restatements detected (17.4%)"
- Reports say "Quality Gates (All Passing)" without context

**Impact:** Sophisticated viewer could see restatement rate and question data quality.

**Solution:**
1. Add context explaining restatements are normal in XBRL (amendments, corrections)
2. Include restatement rate in Executive Summary as a data characteristic
3. Note that gate still passes with 82.6% non-restatement rate

---

### ISS-007: P2-SEC NFLX Fiscal Year Anomaly
**Severity:** HIGH | **Project:** P2-SEC

**Issue:** Netflix shows fiscal_year: 2015 in company_metrics while others show 2025-2026.

**Evidence:**
- `kpis.json` NFLX: "fiscal_year": 2015
- Other companies: fiscal_year 2025 or 2026

**Impact:** Indicates data extraction error or edge case not handled.

**Solution:**
1. Investigate NFLX XBRL data structure
2. Fix fiscal year detection logic
3. Re-run KPI calculation for NFLX

---

### ISS-008: P2-FED Contains Infinity Values
**Severity:** HIGH | **Project:** P2-FED

**Issue:** KPI JSON contains JavaScript Infinity values which may cause rendering issues.

**Evidence:**
- `kpis.json`: "max_qoq_increase": Infinity
- Multiple `qoq_changes` entries with Infinity

**Impact:** JSON parsers may fail; displays may show "Infinity" in reports.

**Solution:**
1. Handle division by zero in QoQ calculation
2. Replace Infinity with null or "N/A"
3. Add guard clause for zero denominators

---

### ISS-009: P2-FED Zero Award Amount Records
**Severity:** HIGH | **Project:** P2-FED

**Issue:** Data contains awards with $0.00 amount.

**Evidence:**
- `kpis.json` summary: "min_award": 0.0

**Impact:** Zero-dollar awards may indicate data quality issues or specific award types that should be filtered.

**Solution:**
1. Investigate zero-amount awards (may be modifications, not new awards)
2. Either filter or document why they exist
3. Consider excluding from spend calculations

---

## MEDIUM PRIORITY ISSUES

### ISS-010: P2-AIR Weather Record Ratio
**Severity:** MEDIUM | **Project:** P2-AIR

**Issue:** Only 1,536 weather records for 99,975 flights (1:65 ratio).

**Evidence:**
- `pipeline_metrics.json`: weather_records: 1,536, flight_records: 99,975
- 1,536 = 10 airports × ~154 days (reasonable)

**Context:** This is actually correct (daily weather × airports) but should be documented.

**Solution:**
1. Add explanation in Methodology: "Weather data is daily per airport, not per flight"
2. Document the join logic clearly

---

### ISS-011: P2-AIR Carrier Delay Uniformity
**Severity:** MEDIUM | **Project:** P2-AIR

**Issue:** All 8 carriers show nearly identical cancel_rate (0.02 = 2.0%) suggesting synthetic data pattern.

**Evidence:**
```
F9: 0.02, DL: 0.02, AS: 0.02, UA: 0.02, B6: 0.02, WN: 0.02, AA: 0.02, NK: 0.02
```

**Impact:** Real BTS data would show more variance between carriers.

**Solution:**
1. Add variance to synthetic data generation
2. Or add disclaimer about data simulation methodology

---

### ISS-012: P2-SEC Cohort Benchmark Min/Max Unrealistic
**Severity:** MEDIUM | **Project:** P2-SEC

**Issue:** Benchmark statistics only include 4 companies (partial cohort).

**Evidence:**
- Cohort is 39 companies
- Benchmark min revenue: $43.6B (but smaller companies exist in cohort)
- Only 4 companies included in benchmark calculation

**Solution:**
1. Include all 39 companies in benchmark calculation
2. Or document why partial cohort was used

---

### ISS-013: P2-FED Future Fiscal Years
**Severity:** MEDIUM | **Project:** P2-FED

**Issue:** Data contains FY2025, 2026, 2027 records (future periods as of data extraction).

**Evidence:**
- FY2025: $1.08B across 308 awards
- FY2026: $21.8M across 5 awards
- FY2027: $4.3M across 2 awards

**Context:** These are likely multi-year contracts with future obligation dates.

**Solution:**
1. Document that multi-year contracts have future-dated obligations
2. Consider filtering to current/past FY only for simpler narrative

---

### ISS-014: P2-AIR Join Coverage Below 90%
**Severity:** MEDIUM | **Project:** P2-AIR

**Issue:** Weather join coverage is 89.9%, below the 90% typical threshold.

**Evidence:**
- `pipeline_metrics.json`: "join_coverage" score: 0.8993
- ~10,000 flights without weather match

**Impact:** Some flights may have missing weather attribution.

**Solution:**
1. Investigate which flights lack weather data (date range issues?)
2. Document missing coverage in methodology
3. Consider raising threshold or improving join logic

---

### ISS-015: Email Address Hardcoded
**Severity:** MEDIUM | **All Projects**

**Issue:** Personal email hardcoded in all PDF generators.

**Evidence:**
- EMAIL = "MboyaJeffers9@gmail.com" in all 3 generators

**Impact:** Professional portfolio may want professional email.

**Solution:**
1. Consider using professional domain email
2. Or remove email from public-facing documents
3. Centralize constant to single config file

---

## LOW PRIORITY ISSUES

### ISS-016: P2-SEC API Rate Limit Claim
**Severity:** LOW | **Project:** P2-SEC

**Issue:** Claims "10 req/sec with User-Agent header" but SEC actually allows 10/second without explicit documentation.

**Evidence:**
- Methodology PDF: "Rate limit: 10 req/sec"
- SEC doesn't officially document this limit

**Solution:**
1. Soften language: "Implemented conservative rate limiting (~10 req/sec)"

---

### ISS-017: Inconsistent Decimal Precision
**Severity:** LOW | **All Projects**

**Issue:** Quality scores show varying decimal precision across reports.

**Evidence:**
- Some show 96.7%, others 96.61%, others 0.9667

**Solution:**
1. Standardize to 1 decimal place for percentages
2. Apply consistent formatting across all reports

---

### ISS-018: P2-FED 2005 Zero Spend Anomaly
**Severity:** LOW | **Project:** P2-FED

**Issue:** FY2005 shows $0.00 spend but 3 awards.

**Evidence:**
- `kpis.json`: "2005": 0.0 (spend), "2005": 3 (awards)

**Impact:** Minor data anomaly, possibly awards with null amounts.

**Solution:**
1. Investigate 3 FY2005 awards
2. Document or exclude as appropriate

---

## Resolution Tracking

| Issue ID | Assigned | Status | Fixed Date | Verified |
|----------|----------|--------|------------|----------|
| ISS-001 | Claude | **FIXED** | 2026-02-01 | PDFs regenerated |
| ISS-002 | Claude | **FIXED** | 2026-02-01 | PDFs regenerated |
| ISS-003 | Claude | **FIXED** | 2026-02-01 | Checkpoint updated |
| ISS-004 | Claude | **FIXED** | 2026-02-01 | Checkpoint updated |
| ISS-005 | - | OPEN | - | - |
| ISS-006 | - | OPEN | - | - |
| ISS-007 | - | OPEN | - | - |
| ISS-008 | - | OPEN | - | - |
| ISS-009 | - | OPEN | - | - |
| ISS-010 | - | OPEN | - | - |
| ISS-011 | - | OPEN | - | - |
| ISS-012 | - | OPEN | - | - |
| ISS-013 | - | OPEN | - | - |
| ISS-014 | - | OPEN | - | - |
| ISS-015 | - | OPEN | - | - |
| ISS-016 | - | OPEN | - | - |
| ISS-017 | - | OPEN | - | - |
| ISS-018 | - | OPEN | - | - |

---

## Recommended Priority Order

1. **ISS-002** - Simulated data disclosure (legal/ethical)
2. **ISS-003** - Weather impact 36x discrepancy (credibility)
3. **ISS-004** - On-time rate mismatch (credibility)
4. **ISS-001** - Date range misrepresentation (accuracy)
5. **ISS-005** - Fortune 500 claim (verifiability)

---

*Issues Log maintained as part of CMS Enterprise Quality System*
*Last Updated: 2026-02-01*
