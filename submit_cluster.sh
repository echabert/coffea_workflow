#!/bin/bash
# Submit the dask scheduler + client to a SLURM COMPUTE node.
#
# Why a compute node? The dask scheduler must be reachable by the workers. The
# login node's address is NOT reachable from the compute fabric (workers on the
# compute nodes time out connecting to the login-node scheduler). Running the
# scheduler on a compute node puts it on the same fabric as the workers, so they
# can connect. plot_slurm.py then submits the dask workers as nested SLURM jobs.
#
# Usage:
#   sbatch submit_cluster.sh              # default (10 workers)
#   sbatch submit_cluster.sh -- 20        # request 20 workers
# The optional argument is the number of dask workers (SLURM jobs). Default: 10.
#SBATCH --partition=vo.scigne.fr
#SBATCH --job-name=coffea_cluster
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=02:00:00
#SBATCH --output=/home-pbs/echabert/MCP/coffea_workflow/cluster_%j.out
#SBATCH --error=/home-pbs/echabert/MCP/coffea_workflow/cluster_%j.err

export X509_USER_PROXY=/home-pbs/echabert/proxy/x509up
cd /home-pbs/echabert/MCP/coffea_workflow || true

# ---------------------------------------------------------------------------
# Isolate this job from the submitting shell's environment.
#
# If a CMS environment (cmsenv / scram / setup.sh) was sourced in the login
# shell before calling `sbatch`, that shell exports a PYTHONPATH that includes
# CVMFS Python-2 areas such as:
#     /cvmfs/cms.cern.ch/slc6_amd64_gcc530/external/py2-numpy/.../site-packages
# That PYTHONPATH is inherited by this job. On a compute node (which does NOT
# have the login node's ~/.local numpy that normally masks it), `import numpy`
# then resolves to that Python-2 numpy and dies at the very first line with:
#     ImportError: dynamic module does not define module export function
#                 (PyInit_multiarray)
# Clearing it (and hiding the user-site) forces numpy/awkward/coffea to always
# load from the self-contained condaEnv, no matter how the job is submitted.
# ---------------------------------------------------------------------------
unset PYTHONPATH
unset PYTHONHOME
export PYTHONNOUSERSITE=1

# Number of dask workers, passed as a job argument.
#
# IMPORTANT: SLURM passes the `--` separator STRAIGHT THROUGH to the job
# script. So `sbatch submit_cluster.sh -- 20` arrives inside the job as
#   $1 = "--"     $2 = "20"
# A naive `JOBS=${1:-10}` therefore captures "--" instead of "20", and
# `plot_slurm.py --jobs "--"` dies with:
#   error: argument --jobs: expected one argument
# (argparse treats the trailing "--" as "end of options", leaving --jobs empty).
# So we must skip the leading "--" and take the first real argument.
#
# Supported invocation forms (all give the same result):
#   sbatch submit_cluster.sh            -> JOBS=10 (default)
#   sbatch submit_cluster.sh -- 20      -> JOBS=20
#   sbatch submit_cluster.sh 20         -> JOBS=20
#   sbatch submit_cluster.sh --         -> JOBS=10 (default)
JOBS=""
for arg in "$@"; do
    case "$arg" in
        --) : ;;                 # skip the sbatch option-separator token
        *)  JOBS="$arg"; break ;; # first real argument wins
    esac
done
# Fall back to the default when no argument was given.
JOBS=${JOBS:-10}
# Belt and suspenders: the value must be a positive integer, else use the default.
case "$JOBS" in
    ''|*[!0-9]*) JOBS=10 ;;
esac

/home-pbs/echabert/MCP/coffea_workflow/condaEnv/bin/python3.12 -u /home-pbs/echabert/MCP/coffea_workflow/plot_slurm.py --jobs "$JOBS"
