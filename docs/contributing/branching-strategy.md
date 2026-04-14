# Git Branching Strategy

Gravtory uses a **trunk-based development** model with short-lived feature
branches.

## Branches

| Branch | Purpose | Lifetime |
|--------|---------|----------|
| `main` | Production-ready code. Protected. | Permanent |
| `feature/<name>` | New features | Days (max 1 week) |
| `fix/<name>` | Bug fixes | Days |
| `release/<version>` | Release stabilisation | Until tag is cut |
| `hotfix/<name>` | Urgent production fixes | Hours |

## Workflow

```
main ─────●─────●─────●─────●─────●─── (always deployable)
           \   /       \   /
            ─●─         ─●─
         feature/x    fix/y
```

1. **Create branch** from `main`:
   ```bash
   git checkout -b feature/dynamic-workflows main
   ```

2. **Develop** with small, focused commits. Run `pre-commit` hooks locally.

3. **Open PR** targeting `main`. All CI checks must pass:
   - lint (ruff + mypy)
   - unit tests (Python 3.10–3.13 × 3 OSes)
   - integration tests (5 backends)
   - e2e tests
   - security (pip-audit + bandit)
   - coverage ≥ 95%

4. **Review**: Minimum 1 approval. Stale reviews are dismissed on new pushes.

5. **Merge**: Squash-and-merge for features/fixes. Merge-commit for releases.

6. **Delete** the branch after merge.

## Release Process

1. Create `release/X.Y.Z` from `main`.
2. Only bug fixes allowed on the release branch (cherry-picked to `main`).
3. Tag `vX.Y.Z` when ready → triggers `release.yml` CI → PyPI publish.
4. Merge release branch back to `main` (merge commit).

## Hotfixes

1. Create `hotfix/<name>` from the latest release tag.
2. Fix, test, PR → merge to `main`.
3. Tag a patch release if needed.

## Naming Conventions

- `feature/` — new functionality
- `fix/` — bug fixes
- `refactor/` — code restructuring (no behavior change)
- `docs/` — documentation only
- `test/` — test additions/fixes only
- `chore/` — tooling, CI, dependency updates

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(backends): add dynamic workflow persistence
fix(cli): correct version display in --version flag
docs(api): document batch operations
test(integration): add circuit breaker contract tests
```
