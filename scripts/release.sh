#!/usr/bin/env bash
# ============================================================================
# Gravtory Release Packaging Script
# Creates a clean, organized release folder ready for distribution.
#
# Usage:
#   ./scripts/release.sh                 # Build release for current version
#   ./scripts/release.sh --tag v0.2.0    # Build release for a specific tag
#   ./scripts/release.sh --skip-build    # Skip build, just package existing dist/
#   ./scripts/release.sh --dry-run       # Show what would happen without doing it
#
# Output:
#   release/gravtory-X.Y.Z/
#     gravtory-X.Y.Z.tar.gz             Source distribution
#     gravtory-X.Y.Z-py3-none-any.whl   Wheel
#     CHECKSUMS.sha256                   SHA-256 checksums
#     RELEASE_NOTES.md                   Auto-generated notes
#     install.sh                         One-command installer
#     docs/                              Built documentation site
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Colours & UI ──────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'
NC='\033[0m'

_step=0; _total=5
step()  { _step=$((_step+1)); echo -e "\n${CYAN}${BOLD}[$_step/$_total]${NC} ${BOLD}$*${NC}"; }
log()   { echo -e "  ${BLUE}...${NC} $*"; }
ok()    { echo -e "  ${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "  ${YELLOW}[WARN]${NC} $*"; }
fail()  { echo -e "\n  ${RED}[FAIL]${NC} $*\n"; exit 1; }
hr()    { echo -e "${DIM}$(printf '%.0s─' {1..60})${NC}"; }

# ── Parse arguments ───────────────────────────────────────────────
TAG=""
SKIP_BUILD=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)        TAG="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --dry-run)    DRY_RUN=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--tag vX.Y.Z] [--skip-build] [--dry-run]"
            echo ""
            echo "  --tag vX.Y.Z   Override version (otherwise read from pyproject.toml)"
            echo "  --skip-build   Skip the full build; package existing dist/ artifacts"
            echo "  --dry-run      Preview the release without creating files"
            exit 0
            ;;
        *) fail "Unknown argument: $1 (try --help)" ;;
    esac
done

# ── Setup ─────────────────────────────────────────────────────────
cd "$PROJECT_DIR"

# Detect version — prefer __about__.py, fall back to pyproject.toml
VERSION=""
if [ -n "$TAG" ]; then
    VERSION="${TAG#v}"
elif [ -f "src/gravtory/__about__.py" ]; then
    VERSION=$(grep '__version__' src/gravtory/__about__.py | head -1 | sed 's/.*"\(.*\)".*/\1/' 2>/dev/null || true)
fi
if [ -z "$VERSION" ]; then
    VERSION=$(grep '^version' pyproject.toml | head -1 | sed 's/.*"\(.*\)".*/\1/' 2>/dev/null || true)
fi
if [ -z "$VERSION" ]; then
    # Last resort: ask hatchling
    VERSION=$(python3 -c "
import tomllib, pathlib
d = tomllib.loads(pathlib.Path('pyproject.toml').read_text())
print(d.get('project',{}).get('version','0.0.0'))
" 2>/dev/null || echo "0.0.0")
fi

RELEASE_NAME="gravtory-${VERSION}"
RELEASE_DIR="$PROJECT_DIR/release/$RELEASE_NAME"

echo ""
echo -e "${BOLD}Gravtory Release${NC}  ${DIM}$(date '+%Y-%m-%d %H:%M:%S')${NC}"
hr
echo -e "  Version:   ${BOLD}$VERSION${NC}"
echo -e "  Output:    ${DIM}$RELEASE_DIR/${NC}"
$DRY_RUN && echo -e "  Mode:      ${YELLOW}DRY RUN${NC}"
hr

# ── Step 1: Build ─────────────────────────────────────────────────
step "Running full build pipeline"
if $SKIP_BUILD; then
    warn "Skipped (--skip-build)"
    if [ ! -d "$PROJECT_DIR/dist" ] || [ -z "$(ls "$PROJECT_DIR/dist/"*.whl 2>/dev/null)" ]; then
        fail "No artifacts in dist/. Run without --skip-build first."
    fi
    ok "Using existing dist/ artifacts"
elif $DRY_RUN; then
    log "Would run: bash $SCRIPT_DIR/build.sh --full"
    ok "Dry run — skipped"
else
    bash "$SCRIPT_DIR/build.sh" --full
    ok "Build pipeline passed"
fi

# ── Step 2: Create release directory ──────────────────────────────
step "Assembling release directory"
if ! $DRY_RUN; then
    rm -rf "$RELEASE_DIR"
    mkdir -p "$RELEASE_DIR"

    # Copy artifacts — try exact name first, then glob
    cp "$PROJECT_DIR/dist/$RELEASE_NAME"*.tar.gz "$RELEASE_DIR/" 2>/dev/null \
        || cp "$PROJECT_DIR/dist/"*.tar.gz "$RELEASE_DIR/" 2>/dev/null || true
    cp "$PROJECT_DIR/dist/"*.whl "$RELEASE_DIR/" 2>/dev/null || true

    # Verify at least one artifact made it
    ARTIFACT_COUNT=$(find "$RELEASE_DIR" -maxdepth 1 \( -name '*.tar.gz' -o -name '*.whl' \) | wc -l)
    if [ "$ARTIFACT_COUNT" -eq 0 ]; then
        fail "No build artifacts found. Run build first."
    fi
    ok "$ARTIFACT_COUNT artifact(s) copied"
else
    log "Would create $RELEASE_DIR/ with dist/ artifacts"
    ok "Dry run — skipped"
fi

# ── Step 3: Checksums ─────────────────────────────────────────────
step "Generating SHA-256 checksums"
if ! $DRY_RUN; then
    cd "$RELEASE_DIR"
    if command -v sha256sum &>/dev/null; then
        sha256sum *.tar.gz *.whl > CHECKSUMS.sha256 2>/dev/null
    elif command -v shasum &>/dev/null; then
        shasum -a 256 *.tar.gz *.whl > CHECKSUMS.sha256 2>/dev/null
    else
        warn "Neither sha256sum nor shasum found — skipping checksums"
    fi
    cd "$PROJECT_DIR"
    ok "CHECKSUMS.sha256 written"
else
    log "Would generate CHECKSUMS.sha256"
    ok "Dry run — skipped"
fi

# ── Step 4: Release notes + install helper ────────────────────────
step "Generating release notes"
if ! $DRY_RUN; then
    WHL_NAME=$(ls -1 "$RELEASE_DIR"/*.whl 2>/dev/null | head -1 | xargs basename 2>/dev/null || echo "$RELEASE_NAME-py3-none-any.whl")

    cat > "$RELEASE_DIR/RELEASE_NOTES.md" << EOF
# Gravtory $VERSION

Released: $(date +%Y-%m-%d)

## Install from PyPI

\`\`\`bash
pip install gravtory
# with a backend:
pip install gravtory[postgres]
pip install gravtory[all]
\`\`\`

## Install from Wheel

\`\`\`bash
python3 -m venv .venv && source .venv/bin/activate
pip install $WHL_NAME
\`\`\`

Or use the included installer:

\`\`\`bash
bash install.sh
\`\`\`

## Checksums

See \`CHECKSUMS.sha256\` for SHA-256 verification.

## Links

- Repository: https://github.com/vatryok/gravtory
- Changelog:  https://github.com/vatryok/gravtory/blob/main/CHANGELOG.md
- Issues:     https://github.com/vatryok/gravtory/issues
- PyPI:       https://pypi.org/project/gravtory/
EOF
    ok "RELEASE_NOTES.md generated"

    # Installer script
    cat > "$RELEASE_DIR/install.sh" << 'INSTALL_EOF'
#!/usr/bin/env bash
# Gravtory installer — creates a venv and installs the wheel.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

G='\033[0;32m'; R='\033[0;31m'; B='\033[1m'; N='\033[0m'
ok()   { echo -e "  ${G}[OK]${N} $*"; }
fail() { echo -e "  ${R}[FAIL]${N} $*"; exit 1; }

WHL=$(ls -1 *.whl 2>/dev/null | head -1)
[ -z "$WHL" ] && fail "No .whl found in $(pwd)"

echo -e "\n${B}Gravtory Installer${N}\n"

# Find python
PYTHON=""
for cmd in python3 python; do
    command -v "$cmd" &>/dev/null && PYTHON="$cmd" && break
done
[ -z "$PYTHON" ] && fail "Python not found"
ok "Using $($PYTHON --version 2>&1)"

# Ensure venv
if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "  ... Creating virtual environment at .venv/"
    $PYTHON -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip --quiet
    ok "Venv created"
else
    ok "Using active venv: $VIRTUAL_ENV"
fi

echo "  ... Installing $WHL"
pip install "$WHL" --quiet
ok "Installed"

python -c "import gravtory; print('  gravtory ' + gravtory.__version__)"
echo ""
[ -z "${VIRTUAL_ENV:-}" ] || echo "  Activate later with: source $(pwd)/.venv/bin/activate"
echo ""
INSTALL_EOF
    chmod +x "$RELEASE_DIR/install.sh"
    ok "install.sh helper generated"
else
    log "Would generate RELEASE_NOTES.md + install.sh"
    ok "Dry run — skipped"
fi

# ── Step 5: Documentation ────────────────────────────────────────
step "Building documentation"
if $DRY_RUN; then
    log "Would run mkdocs build"
    ok "Dry run — skipped"
elif command -v mkdocs &>/dev/null; then
    mkdocs build --site-dir "$RELEASE_DIR/docs" --quiet 2>/dev/null \
        && ok "Documentation built" \
        || warn "mkdocs build had warnings (non-fatal)"
else
    warn "mkdocs not installed — skipping docs (pip install mkdocs to enable)"
fi

# ── Summary ───────────────────────────────────────────────────────
echo ""
hr
echo -e "  ${GREEN}${BOLD}RELEASE READY: $RELEASE_NAME${NC}"
hr
if ! $DRY_RUN; then
    echo ""
    echo "  Contents:"
    ls -1 "$RELEASE_DIR/" | sed 's/^/    /'
    echo ""
    echo -e "  Location: ${DIM}$RELEASE_DIR/${NC}"

    # Show total size
    TOTAL_SIZE=$(du -sh "$RELEASE_DIR" 2>/dev/null | awk '{print $1}')
    echo -e "  Size:     ${DIM}$TOTAL_SIZE${NC}"
fi
echo ""
