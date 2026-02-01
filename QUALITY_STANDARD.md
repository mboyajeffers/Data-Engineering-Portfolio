# Proof Package Quality Standard
## Enterprise-Grade Report Requirements
**Version:** 1.0.0 | **Effective:** 2026-02-01

---

## Core Directive

> **All reports must withstand public scrutiny as if millions of dollars were on the line.**
>
> Reports are NOT ready for generation until they have passed the scrutiny they would face in professional settings. The author must be trusted.

---

## Mandatory Pre-Release Audit

Every report MUST pass the following audit before PDF generation:

### 1. Data Verification
- [ ] All numeric claims match actual source data (cross-check KPIs vs checkpoint vs PDFs)
- [ ] Date ranges are accurately stated
- [ ] Record counts match between pipeline metrics and reports
- [ ] Percentages calculated correctly (no rounding errors that change meaning)

### 2. Source Attribution
- [ ] All data sources clearly identified
- [ ] **SIMULATED/SYNTHETIC data explicitly disclosed** with prominent notice
- [ ] API endpoints documented and verifiable
- [ ] No claims of accessing data that wasn't actually fetched

### 3. Verifiability Test
For each major claim, ask: "Can a skeptical viewer verify this?"
- [ ] If YES: Ensure the verification would succeed
- [ ] If NO (simulated data): Add clear disclaimer

### 4. Professional Language
- [ ] No unsubstantiated superlatives ("best", "industry-leading")
- [ ] Claims qualified appropriately ("Fortune 500" only if verified)
- [ ] Correlation vs causation clearly distinguished
- [ ] Limitations acknowledged

### 5. Consistency Check
- [ ] All instances of same metric show same value
- [ ] Checkpoint matches KPIs matches PDFs
- [ ] No conflicting statements across documents

---

## Severity Levels

| Level | Definition | Action |
|-------|------------|--------|
| **CRITICAL** | Could destroy credibility if challenged | BLOCK release until fixed |
| **HIGH** | Significant risk of professional embarrassment | Fix before public release |
| **MEDIUM** | Could be questioned, has reasonable explanation | Recommended fix |
| **LOW** | Minor inconsistency, unlikely to be noticed | Advisory |

---

## Audit Process

1. **Generate Reports** - Create all PDFs from pipeline data
2. **Cross-Reference** - Compare claims against source data
3. **Verifiability Test** - Attempt to verify each major claim
4. **Log Issues** - Document all discrepancies in ISSUES_LOG.md
5. **Fix Critical/High** - Resolve before release
6. **Regenerate** - Create final PDFs after fixes
7. **Sign-Off** - Document audit completion in checkpoint

---

## Red Flags (Auto-Fail)

The following automatically fail the audit:

1. **Undisclosed simulated data** - Claiming real source for synthetic data
2. **Metric mismatch > 5%** - KPI value differs from report by > 5%
3. **Unverifiable claims** - Assertions that can be disproven
4. **Missing attribution** - Data without source
5. **Infinity/NaN in outputs** - Unhandled edge cases in calculations

---

## Sign-Off Template

```
QUALITY AUDIT SIGN-OFF
Report: [Project Name]
Version: [X.Y.Z]
Audit Date: [YYYY-MM-DD]

[ ] Data verification complete
[ ] Source attribution verified
[ ] Verifiability test passed
[ ] Language reviewed
[ ] Consistency check passed
[ ] All CRITICAL issues resolved
[ ] All HIGH issues resolved

Issues Remaining: [N] MEDIUM, [N] LOW
Recommendation: [APPROVED / BLOCKED]
```

---

## Applies To

This standard applies to ALL reports in:
- `/Career/Proof_Package/` (Portfolio demonstrations)
- `/Career/LinkedIn_Package/` (Public-facing materials)
- Any report intended for external audiences

---

## Rationale

Trust is built through accuracy and transparency. A single verifiable error can undermine an entire portfolio. Better to:
- Clearly label simulated data than hide it
- Use conservative claims than overstate
- Document limitations than ignore them
- Fix issues before release than explain them after

---

*Quality is not optional. Credibility is not recoverable.*

*Last Updated: 2026-02-01*
