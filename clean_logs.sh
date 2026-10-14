#!/bin/bash
# ---------------------------------------------------------------------------
# clean_logs.sh -- remove SLURM/dask job logs and temporary files.
#
# What it removes (top level of this workflow directory ONLY, it never
# recurses into condaEnv/, venv/, coffea/ or any other subdirectory):
#
#   Log files:
#     slurm-<jobid>.out/.err         one per dask worker (job_extra_directives
#                                    in plot_slurm.py)
#     <name>_<jobid>.out/.err        scheduler / diagnostic jobs, e.g.
#                                    cluster_<id>, diag_<id>, diag2_<id>,
#                                    min_<id>, test_imports_<id>,
#                                    xrd_noproxy_<id>, xrootd_test_<id>, ...
#     install*.log, run.log          environment setup / run logs
#
#   Temporary files:
#     dask-scratch-space/            dask worker spill directories (worker-*)
#                                    and lock files (worker-*.dirlock,
#                                    global.lock, purge.lock)
#     __pycache__/                   compiled python bytecode
#
# What is NEVER removed:
#     output.root, *.py, *.sh, files.txt (.v2/.bkp), *.bkp, README.md,
#     requirements.txt, condaEnv/, venv/, coffea/ and everything else that
#     does not match the patterns above.
#
# Safety:
#   * If you still have a SLURM job running, dask-scratch-space is left
#     untouched (deleting live worker directories can break a running
#     cluster). Use --force to override.
#   * With --days N, only files STRICTLY OLDER than N days are removed,
#     so very recent logs are kept. Default is 0 (no age filter).
#
# Usage:
#   ./clean_logs.sh                 # remove all matching logs + temp files
#   ./clean_logs.sh --dry-run       # only show what would be removed
#   ./clean_logs.sh --days 7        # only remove files older than 7 days
#   ./clean_logs.sh --skip-dask     # leave dask-scratch-space alone
#   ./clean_logs.sh --skip-pycache  # leave __pycache__ alone
#
# Options:
#   --dry-run       show what would be removed, remove nothing
#   --days N        only remove files strictly older than N days (default 0)
#   --skip-dask     do not clean dask-scratch-space
#   --skip-pycache  do not remove __pycache__
#   --force         clean dask-scratch-space even if a SLURM job is running
#   -h, --help      show this help
# ---------------------------------------------------------------------------
set -euo pipefail

BASE="$(cd "$(dirname "$0")" && pwd)"
DASK_DIR="$BASE/dask-scratch-space"
PYCACHE_DIR="$BASE/__pycache__"

DRY_RUN=0
DAYS=0
SKIP_DASK=0
SKIP_PYCACHE=0
FORCE=0

usage() {
    sed -n '2,/^# --- END HELP ---$/{ /^# --- END HELP ---$/d; p }' "$0" | sed 's/^# \{0,1\}//'
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run)     DRY_RUN=1 ;;
        --days)
            [ $# -ge 2 ] || die "--days needs a number"
            DAYS="$2"
            shift
            ;;
        --days=*)      DAYS="${1#--days=}" ;;
        --skip-dask)   SKIP_DASK=1 ;;
        --skip-pycache) SKIP_PYCACHE=1 ;;
        --force)       FORCE=1 ;;
        -h|--help)     usage; exit 0 ;;
        *)             die "unknown option: $1 (see --help)" ;;
    esac
    shift
done

case "$DAYS" in
    ''|*[!0-9]*) die "--days expects a non-negative integer, got: $DAYS" ;;
esac

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Human-readable byte count (numfmt if available, awk fallback).
human() {
    local b="$1"
    if command -v numfmt >/dev/null 2>&1; then
        numfmt --to=iec --suffix=B "$b"
    else
        awk -v b="$b" 'BEGIN {
            split("B KiB MiB GiB TiB", u, " ")
            i = 1
            while (b >= 1024 && i < 5) { b /= 1024; i++ }
            if (i == 1) printf "%d %s", b, u[i]
            else        printf "%.1f %s", b, u[i]
        }'
    fi
}

# Sum the sizes (in bytes) of the files listed one-per-line on stdin.
bytes_of_list() {
    xargs -d '\n' -r stat -c '%s' 2>/dev/null | awk '{ s += $1 } END { printf "%d", s + 0 }'
}

# True if this user still has an active SLURM job (running or pending).
have_running_jobs() {
    command -v squeue >/dev/null 2>&1 || return 1
    local out
    out="$(squeue -u "${USER:-$(id -un)}" --noheader 2>/dev/null || true)"
    [ -n "$out" ] || return 1
    # squeue columns: JOBID PARTITION NAME USER ST TIME NODES NODELIST
    printf '%s\n' "$out" | awk '{ print $5 }' | grep -qE '^(R|PD|BO|SO|PF|CA)$'
}

# Age filter shared by every find below. Empty when DAYS == 0.
AGE_ARGS=()
if [ "$DAYS" -gt 0 ]; then
    # -mtime +N-1 matches files strictly older than N days
    AGE_ARGS=(-mtime "+$((DAYS - 1))")
fi

# ---------------------------------------------------------------------------
# 1. Job log files (top level of BASE only)
# ---------------------------------------------------------------------------
LOG_FILES="$(find "$BASE" -maxdepth 1 -type f \
    \( -name 'slurm-[0-9]*.out'  -o -name 'slurm-[0-9]*.err' \
       -o -name '*_[0-9][0-9].out' -o -name '*_[0-9][0-9].err' \
       -o -name 'install*.log'    -o -name 'run.log' \
    \) ${AGE_ARGS[@]+"${AGE_ARGS[@]}"} 2>/dev/null || true)"

# ---------------------------------------------------------------------------
# 2. dask scratch space
# ---------------------------------------------------------------------------
DASK_ITEMS=""
DASK_SKIPPED=""
if [ -d "$DASK_DIR" ] && [ "$SKIP_DASK" -eq 0 ]; then
    if have_running_jobs && [ "$FORCE" -eq 0 ]; then
        DASK_SKIPPED="yes"
    else
        DASK_ITEMS="$(find "$DASK_DIR" -mindepth 1 \
            \( -name 'worker-*' -o -name '*.dirlock' \
               -o -name 'global.lock' -o -name 'purge.lock' \
            \) ${AGE_ARGS[@]+"${AGE_ARGS[@]}"} 2>/dev/null || true)"
    fi
fi

# ---------------------------------------------------------------------------
# 3. __pycache__
# ---------------------------------------------------------------------------
PYCACHE_SIZE=0
if [ "$SKIP_PYCACHE" -eq 0 ] && [ -d "$PYCACHE_DIR" ]; then
    PYCACHE_SIZE="$(du -sb "$PYCACHE_DIR" 2>/dev/null | awk '{ print $1 }' || echo 0)"
fi

# ---------------------------------------------------------------------------
# Report / remove
# ---------------------------------------------------------------------------
log_n=0; log_bytes=0
dask_n=0; dask_bytes=0
if [ -n "$LOG_FILES" ]; then
    log_n="$(printf '%s\n' "$LOG_FILES" | grep -c .)"
    log_bytes="$(printf '%s\n' "$LOG_FILES" | bytes_of_list)"
fi
if [ -n "$DASK_ITEMS" ]; then
    dask_n="$(printf '%s\n' "$DASK_ITEMS" | grep -c .)"
    dask_bytes="$(printf '%s\n' "$DASK_ITEMS" | bytes_of_list)"
fi
total_n=$((log_n + dask_n))
total_bytes=$((log_bytes + dask_bytes + PYCACHE_SIZE))

if [ "$DRY_RUN" -eq 1 ]; then
    echo "[dry-run] Would remove $total_n file(s)/dir(s), freeing $(human "$total_bytes"):"
    if [ "$log_n" -gt 0 ]; then
        echo "[dry-run]   $log_n log file(s) ($(human "$log_bytes")):"
        printf '%s\n' "$LOG_FILES" | sed 's/^/[dry-run]     /'
    fi
    if [ "$dask_n" -gt 0 ]; then
        echo "[dry-run]   $dask_n dask-scratch item(s) ($(human "$dask_bytes")):"
        printf '%s\n' "$DASK_ITEMS" | sed 's/^/[dry-run]     /'
    fi
    if [ "$PYCACHE_SIZE" -gt 0 ]; then
        echo "[dry-run]   __pycache__/ ($(human "$PYCACHE_SIZE"))"
    fi
    if [ -n "$DASK_SKIPPED" ]; then
        echo "[dry-run]   dask-scratch-space SKIPPED (a SLURM job is running)"
    fi
    exit 0
fi

if [ "$log_n" -gt 0 ]; then
    printf '%s\n' "$LOG_FILES" | xargs -d '\n' -r rm -f
    echo "Removed $log_n log file(s), freed $(human "$log_bytes")."
fi
if [ "$dask_n" -gt 0 ]; then
    # worker-* entries are directories -> need -rf; lock files are plain files.
    printf '%s\n' "$DASK_ITEMS" | xargs -d '\n' -r rm -rf
    echo "Removed $dask_n dask-scratch-space item(s), freed $(human "$dask_bytes")."
fi
PYCACHE_FREED=0
if [ "$PYCACHE_SIZE" -gt 0 ]; then
    rm -rf "$PYCACHE_DIR" 2>/dev/null || true
    # NFS home: a .pyc that some other node/process still has open is
    # "silly-renamed" to .nfs* and cannot be removed until that process
    # exits. Retry briefly, then report instead of failing.
    attempt=1
    while [ -d "$PYCACHE_DIR" ] && [ "$attempt" -le 5 ]; do
        sleep 1
        rm -rf "$PYCACHE_DIR" 2>/dev/null || true
        attempt=$((attempt + 1))
    done
    if [ -d "$PYCACHE_DIR" ]; then
        echo "Note: __pycache__ left behind (an open .nfs* file on NFS still holds it; it empties out by itself once the holding process exits)."
    else
        PYCACHE_FREED=$PYCACHE_SIZE
        echo "Removed __pycache__/ ($(human "$PYCACHE_SIZE"))."
    fi
fi
if [ -n "$DASK_SKIPPED" ]; then
    echo "dask-scratch-space left untouched (a SLURM job is still running; re-run later or use --force)."
fi

echo "Done: $total_n item(s) removed, $(human "$((log_bytes + dask_bytes + PYCACHE_FREED))") freed."
# --- END HELP ---
