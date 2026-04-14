#!/usr/bin/env bash
# ============================================================================
# Gravtory Build Script
# Builds, tests, and packages the project for distribution.
#
# Usage:
#   ./scripts/build.sh              # Full build (lint + test + package)
#   ./scripts/build.sh --quick      # Package only (skip lint/test)
#   ./scripts/build.sh --test       # Run tests only
#   ./scripts/build.sh --lint       # Run lint + typecheck only
#   ./scripts/build.sh --clean      # Clean all build artifacts
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="$PROJECT_DIR/dist"
BUILD_DIR="$PROJECT_DIR/build"
VENV_DIR="$PROJECT_DIR/.venv"
MIN_PY_MAJOR=3
MIN_PY_MINOR=10

# ── Colours & UI ──────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'
NC='\033[0m'

_step=0; _total=0
step()  { _step=$((_step+1)); echo -e "\n${CYAN}${BOLD}[$_step/$_total]${NC} ${BOLD}$*${NC}"; }
log()   { echo -e "  ${BLUE}...${NC} $*"; }
ok()    { echo -e "  ${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "  ${YELLOW}[WARN]${NC} $*"; }
fail()  { echo -e "\n  ${RED}[FAIL]${NC} $*\n"; exit 1; }
hr()    { echo -e "${DIM}$(printf '%.0s─' {1..60})${NC}"; }

# ── Python detection ──────────────────────────────────────────────
find_python() {
    local candidates=(python3 python)
    for cmd in "${candidates[@]}"; do
        if command -v "$cmd" &>/dev/null; then
            PYTHON="$cmd"
            PY_VERSION=$($PYTHON --version 2>&1 | awk '{print $2}')
            PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
            PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)
            if [ "$PY_MAJOR" -ge $MIN_PY_MAJOR ] && [ "$PY_MINOR" -ge $MIN_PY_MINOR ]; then
                return 0
            fi
        fi
    done
    fail "Python ${MIN_PY_MAJOR}.${MIN_PY_MINOR}+ required. Found: ${PY_VERSION:-none}"
}

# ── Virtual environment (handles PEP 668 / stale venvs) ──────────
ensure_venv() {
    # Already inside a working venv whose python resolves
    if [ -n "${VIRTUAL_ENV:-}" ] && command -v python &>/dev/null; then
        ok "Using active venv: $VIRTUAL_ENV"
        return
    fi

    # Validate existing .venv — check python, pip, AND correct path in activate
    if [ -d "$VENV_DIR" ]; then
        local venv_ok=true
        # Check python binary
        if ! [ -x "$VENV_DIR/bin/python" ] || ! "$VENV_DIR/bin/python" -c "import sys" 2>/dev/null; then
            venv_ok=false
        fi
        # Check pip works
        if $venv_ok && ! "$VENV_DIR/bin/python" -m pip --version &>/dev/null; then
            venv_ok=false
        fi
        # Check activate script points to correct path (catches cross-machine clones)
        if $venv_ok && [ -f "$VENV_DIR/bin/activate" ]; then
            if ! grep -q "$VENV_DIR" "$VENV_DIR/bin/activate" 2>/dev/null; then
                venv_ok=false
            fi
        fi

        if $venv_ok; then
            log "Activating existing virtual environment..."
            # shellcheck disable=SC1091
            source "$VENV_DIR/bin/activate"
            ok "Venv active: $VENV_DIR"
            return
        else
            warn "Stale/broken .venv detected — recreating..."
            rm -rf "$VENV_DIR"
        fi
    fi

    log "Creating virtual environment at .venv/ ..."
    $PYTHON -m venv "$VENV_DIR"
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
    pip install --upgrade pip --quiet 2>/dev/null
    ok "Venv created: $VENV_DIR"
}

# ── Dependency installation (with cache awareness) ────────────────
install_deps() {
    log "Installing project with dev + all extras..."
    pip install -e ".[dev,all]" --quiet 2>&1 | tail -3
    ok "Dependencies installed"
}

# ── Ensure build tooling available ────────────────────────────────
ensure_build_tools() {
    if ! python -m build --version &>/dev/null 2>&1; then
        log "Installing build tooling (build, twine)..."
        pip install build twine --quiet
    fi
}

# ── Tasks ─────────────────────────────────────────────────────────
do_clean() {
    step "Cleaning build artifacts"
    rm -rf "$DIST_DIR" "$BUILD_DIR" "$PROJECT_DIR"/src/*.egg-info "$PROJECT_DIR"/*.egg-info
    rm -rf "$PROJECT_DIR/.mypy_cache" "$PROJECT_DIR/.ruff_cache"
    rm -rf "$PROJECT_DIR/.pytest_cache" "$PROJECT_DIR/htmlcov"
    rm -f  "$PROJECT_DIR/.coverage"
    find "$PROJECT_DIR" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
    ok "Clean complete"
}

do_lint() {
    step "Linting & type-checking"

    log "ruff check src/ tests/"
    ruff check src/ tests/ && ok "Ruff lint passed" || fail "Ruff lint failed"

    log "ruff format --check src/ tests/"
    ruff format --check src/ tests/ && ok "Format check passed" || fail "Format check failed"

    log "mypy src/gravtory/"
    mypy src/gravtory/ --ignore-missing-imports && ok "Type check passed" || fail "Type check failed"
}

do_test() {
    step "Running unit tests"
    pytest tests/unit/ -q --timeout=60 --benchmark-disable --tb=short \
        && ok "Unit tests passed" \
        || fail "Unit tests failed"
}

do_package() {
    step "Building sdist + wheel"
    ensure_build_tools
    rm -rf "$DIST_DIR" "$BUILD_DIR"
    python -m build --outdir "$DIST_DIR" 2>&1 | grep -E "^(Successfully|Building)" || true
    ok "Package built"
    ls -lh "$DIST_DIR"/*.tar.gz "$DIST_DIR"/*.whl 2>/dev/null | sed 's/^/    /'

    step "Verifying package"
    pip install "$DIST_DIR"/*.whl --quiet --force-reinstall
    python -c "import gravtory; print('  gravtory ' + gravtory.__version__)"
    pip install -e ".[dev,all]" --quiet
    ok "Package verification passed"
}

# ── Main ──────────────────────────────────────────────────────────
main() {
    cd "$PROJECT_DIR"
    echo ""
    echo -e "${BOLD}Gravtory Build${NC}  ${DIM}$(date '+%Y-%m-%d %H:%M:%S')${NC}"
    hr

    find_python
    ok "Python $PY_VERSION"

    case "${1:---full}" in
        --clean)
            _total=1
            do_clean
            ;;
        --lint)
            _total=1
            ensure_venv; install_deps
            do_lint
            ;;
        --test)
            _total=1
            ensure_venv; install_deps
            do_test
            ;;
        --quick)
            _total=2
            ensure_venv; install_deps
            do_package
            ;;
        --full|full|"")
            _total=5
            ensure_venv; install_deps
            do_clean
            do_lint
            do_test
            do_package
            echo ""
            hr
            echo -e "  ${GREEN}${BOLD}BUILD SUCCESSFUL${NC}"
            echo -e "  ${DIM}Artifacts: $DIST_DIR/${NC}"
            hr
            ;;
        -h|--help)
            echo "Usage: $0 [--clean|--lint|--test|--quick|--full]"
            echo ""
            echo "  --clean   Remove build artifacts"
            echo "  --lint    Run linter + type checker"
            echo "  --test    Run unit tests"
            echo "  --quick   Package only (skip lint/test)"
            echo "  --full    Full pipeline: clean + lint + test + package (default)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1 (try --help)"
            exit 1
            ;;
    esac
    echo ""
}

main "$@"
