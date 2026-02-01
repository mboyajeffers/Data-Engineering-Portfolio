# Project2 Issues Log
## Tracking: Bugs, Issues, Fixes

---

## Issue Template

```
### ISSUE-XXX: [Title]
**Status:** OPEN | IN PROGRESS | RESOLVED | WONTFIX
**Severity:** CRITICAL | HIGH | MEDIUM | LOW
**Project:** P2-FED | P2-SEC | P2-AIR | ALL
**Created:** YYYY-MM-DD
**Resolved:** YYYY-MM-DD (if applicable)

**Description:**
[Detailed description]

**Root Cause:**
[Analysis]

**Fix:**
[Solution implemented]

**Verification:**
[How it was verified]
```

---

## Open Issues

*No open issues*

---

## In Progress

*No issues in progress*

---

## Resolved Issues

### ISSUE-001: USAspending.gov API Rate Limiting
**Status:** RESOLVED
**Severity:** MEDIUM
**Project:** P2-FED
**Created:** 2026-02-01
**Resolved:** 2026-02-01

**Description:**
USAspending.gov API aggressively rate limits after ~250 consecutive requests (25,000 records). Server returns 500 errors and closes connections without response.

**Root Cause:**
API has undocumented rate limiting that triggers after sustained load. Fair use policy is more restrictive than documented.

**Fix:**
- Increased RATE_LIMIT_DELAY from 0.1s to 0.25s between calls
- Added exponential backoff on errors
- Implemented graceful degradation (stop after 10 consecutive errors)
- Pipeline saves partial data on interruption

**Verification:**
- Successfully ingested 24,800 records before rate limit
- All quality gates pass on partial data
- KPIs and PDFs generate correctly
- Sufficient data for portfolio demonstration

**Workaround for Full Dataset:**
For production use with 3M+ records:
1. Use bulk download API instead of search API
2. Implement multi-day incremental pulls with daily quotas
3. Cache responses to avoid re-fetching

---

### ISSUE-002: JSON Serialization with Tuple Keys
**Status:** RESOLVED
**Severity:** LOW
**Project:** P2-FED
**Created:** 2026-02-01
**Resolved:** 2026-02-01

**Description:**
KPI calculation created dictionary keys as tuples (e.g., `(2024, 1)` for fiscal_year/quarter) which cannot be serialized to JSON.

**Root Cause:**
Pandas groupby with multiple columns returns MultiIndex with tuple keys. Python json module cannot serialize tuples as dictionary keys.

**Fix:**
Converted tuple keys to string format:
```python
# Before:
trends['by_quarter'] = quarterly.to_dict()  # {(2024, 1): 1000}

# After:
trends['by_quarter'] = {f"{k[0]}_Q{k[1]}": v for k, v in quarterly.to_dict().items()}  # {"2024_Q1": 1000}
```

**Verification:**
- kpis.json saves without errors
- PDF generator reads data correctly

---

### ISSUE-003: NumPy Types Not JSON Serializable
**Status:** RESOLVED
**Severity:** LOW
**Project:** P2-FED
**Created:** 2026-02-01
**Resolved:** 2026-02-01

**Description:**
Pipeline metrics contained numpy.bool_ and numpy.float64 types that Python's json module cannot serialize.

**Root Cause:**
Quality gate calculations return numpy types instead of Python native types.

**Fix:**
Explicit type casting when building metrics dictionary:
```python
metrics_dict = {
    'passed': bool(g.passed),    # numpy.bool_ → bool
    'score': float(g.score),     # numpy.float64 → float
    ...
}
```

**Verification:**
- pipeline_metrics.json saves correctly
- PDF generator loads metrics without type errors

---

## Issue Statistics

| Status | Count |
|--------|-------|
| Open | 0 |
| In Progress | 0 |
| Resolved | 3 |
| Won't Fix | 0 |
| **Total** | **3** |

---

## By Severity

| Severity | Open | Resolved |
|----------|------|----------|
| Critical | 0 | 0 |
| High | 0 | 0 |
| Medium | 0 | 1 |
| Low | 0 | 2 |

---

## By Project

| Project | Open | Resolved |
|---------|------|----------|
| P2-FED | 0 | 3 |
| P2-SEC | 0 | 0 |
| P2-AIR | 0 | 0 |
| ALL | 0 | 0 |

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.1.0 | 2026-02-01 | Added 3 resolved issues from P2-FED |
| 1.0.0 | 2026-02-01 | Initial issues log created |

---

*Log maintained by Claude Code*
