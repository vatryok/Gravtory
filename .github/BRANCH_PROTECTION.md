# Branch Protection Rules

Recommended GitHub branch protection settings for the `main` branch.

> **Note**: This file is documentation only. Branch protection rules must be
> configured by a repository admin in **GitHub Settings > Branches > Branch
> protection rules**, or via the GitHub CLI command shown below.

## Settings for `main`

### Required Status Checks
- **lint** — Ruff check + format + mypy
- **unit** — Unit tests (all Python versions, all OS)
- **integration** — Integration tests (all backends)
- **e2e** — End-to-end tests
- **security** — pip-audit + bandit
- **container-scan** — Trivy image vulnerability scan
- **coverage** — 95% coverage gate

### Rules
- **Require pull request before merging**: Yes
- **Required approvals**: 1 (minimum)
- **Dismiss stale reviews on new push**: Yes
- **Require status checks to pass**: Yes (all checks above)
- **Require branches to be up to date**: Yes
- **Restrict pushes to `main`**: Only via PR merge
- **Do not allow force pushes**: Yes
- **Do not allow deletions**: Yes

### Recommended Merge Strategy
- **Squash and merge** for feature branches (clean history)
- **Merge commit** for release branches (preserve history)

## Setup via GitHub CLI

```bash
gh api repos/{owner}/gravtory/branches/main/protection \
  --method PUT \
  --field required_status_checks='{"strict":true,"contexts":["lint","unit","integration","e2e","security","container-scan","coverage"]}' \
  --field enforce_admins=true \
  --field required_pull_request_reviews='{"required_approving_review_count":1,"dismiss_stale_reviews":true}' \
  --field restrictions=null
```
