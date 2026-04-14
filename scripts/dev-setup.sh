#!/usr/bin/env bash
# ============================================================================
# Gravtory Developer Setup Script
# One-command setup for new contributors.
#
# Usage:
#   ./scripts/dev-setup.sh             # Full setup
#   ./scripts/dev-setup.sh --no-test   # Skip smoke test
#   ./scripts/dev-setup.sh --reset     # Delete .venv and start fresh
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
MIN_PY_MAJOR=3
MIN_PY_MINOR=10

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
SKIP_TEST=false
RESET=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-test)  SKIP_TEST=true; shift ;;
        --reset)    RESET=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--no-test] [--reset]"
            echo ""
            echo "  --no-test   Skip the smoke test step"
            echo "  --reset     Delete existing .venv and start fresh"
            exit 0
            ;;
        *) fail "Unknown argument: $1 (try --help)" ;;
    esac
done

cd "$PROJECT_DIR"

echo ""
echo -e "${BOLD}Gravtory Developer Setup${NC}  ${DIM}$(date '+%Y-%m-%d %H:%M:%S')${NC}"
hr

# ── Step 1: Python ────────────────────────────────────────────────
step "Checking Python"
PYTHON=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        PYTHON="$cmd"
        PY_VERSION=$($PYTHON --version 2>&1 | awk '{print $2}')
        PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
        PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)
        if [ "$PY_MAJOR" -ge $MIN_PY_MAJOR ] && [ "$PY_MINOR" -ge $MIN_PY_MINOR ]; then
            break
        fi
        PYTHON=""
    fi
done
[ -z "$PYTHON" ] && fail "Python ${MIN_PY_MAJOR}.${MIN_PY_MINOR}+ required. Found: ${PY_VERSION:-none}"
ok "Python $PY_VERSION ($PYTHON)"

# ── Step 2: Virtual environment ───────────────────────────────────
step "Virtual environment"

if $RESET && [ -d "$VENV_DIR" ]; then
    log "Removing old .venv (--reset)..."
    rm -rf "$VENV_DIR"
fi

if [ -d "$VENV_DIR" ]; then
    # Validate existing venv: python + pip + correct path in activate
    venv_ok=true
    if ! [ -x "$VENV_DIR/bin/python" ] || ! "$VENV_DIR/bin/python" -c "import sys" 2>/dev/null; then
        venv_ok=false
    fi
    if $venv_ok && ! "$VENV_DIR/bin/python" -m pip --version &>/dev/null; then
        venv_ok=false
    fi
    if $venv_ok && [ -f "$VENV_DIR/bin/activate" ]; then
        if ! grep -q "$VENV_DIR" "$VENV_DIR/bin/activate" 2>/dev/null; then
            venv_ok=false
        fi
    fi

    if $venv_ok; then
        log "Reusing existing .venv/"
        # shellcheck disable=SC1091
        source "$VENV_DIR/bin/activate"
        ok "Venv active: $VENV_DIR"
    else
        warn "Stale/broken .venv detected — recreating..."
        rm -rf "$VENV_DIR"
        log "Creating virtual environment..."
        $PYTHON -m venv "$VENV_DIR"
        # shellcheck disable=SC1091
        source "$VENV_DIR/bin/activate"
        pip install --upgrade pip --quiet 2>/dev/null
        ok "Venv created: $VENV_DIR"
    fi
else
    log "Creating virtual environment at .venv/ ..."
    $PYTHON -m venv "$VENV_DIR"
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
    pip install --upgrade pip --quiet 2>/dev/null
    ok "Venv created: $VENV_DIR"
fi

# ── Step 3: Dependencies ─────────────────────────────────────────
step "Installing dependencies"
log "pip install -e '.[dev,all]' (this may take a minute)..."
pip install -e ".[dev,all]" --quiet 2>&1 | tail -3
ok "All dependencies installed"

# ── Step 4: Pre-commit hooks ─────────────────────────────────────
step "Pre-commit hooks"
if command -v pre-commit &>/dev/null; then
    pre-commit install --install-hooks --quiet 2>/dev/null
    ok "Pre-commit hooks installed"
elif pip show pre-commit &>/dev/null 2>&1; then
    python -m pre_commit install --install-hooks --quiet 2>/dev/null
    ok "Pre-commit hooks installed (via pip)"
else
    warn "pre-commit not available — skipping (install with: pip install pre-commit)"
fi

# ── Step 5: Verify ────────────────────────────────────────────────
step "Verifying installation"
log "Import check..."
python -c "import gravtory; print('  gravtory ' + gravtory.__version__)" \
    && ok "Import check passed" \
    || fail "Cannot import gravtory — installation broken"

if ! $SKIP_TEST; then
    log "Running quick smoke test (unit tests, first failure stops)..."
    if pytest tests/unit/ -q --timeout=30 -x --benchmark-disable --tb=line 2>&1 | tail -3; then
        ok "Smoke test passed"
    else
        warn "Some tests failed — check output above (non-blocking)"
    fi
else
    warn "Smoke test skipped (--no-test)"
fi

# ── Done ──────────────────────────────────────────────────────────
echo ""
hr
echo -e "  ${GREEN}${BOLD}DEVELOPER SETUP COMPLETE${NC}"
hr
echo ""
echo -e "  ${BOLD}Activate your environment:${NC}"
echo "    source .venv/bin/activate"
echo ""
echo -e "  ${BOLD}Quick reference:${NC}"
echo "    pytest tests/unit/ -q            Run unit tests"
echo "    ruff check src/ tests/           Lint"
echo "    ruff format src/ tests/          Auto-format"
echo "    mypy src/gravtory/               Type check"
echo "    ./scripts/build.sh               Full build pipeline"
echo "    ./scripts/build.sh --help        Build options"
echo "    ./scripts/release.sh             Package a release"
echo ""
