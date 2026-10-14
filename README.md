Python environment
-----------------
The `condaEnv/` directory is a self-contained conda environment (Python 3.12)
that contains ALL the dependencies (coffea, dask, dask-jobqueue, uproot, ...).
It is self-contained on purpose so that the same environment works on the login
node AND on every SLURM compute node (compute nodes do not share the login
node's `~/.local` user site).

To (re)build it from scratch run:
    ./install_env.sh
It installs the pinned `requirements.txt` into `condaEnv/lib/python3.12/site-packages`.

Usage
-----
1. Refresh the grid proxy (needed so compute nodes can read the dCache):
       ./copy_proxy.sh
   (see "Helper scripts" below; run it before submitting, and again
    whenever the proxy expires)

2. Submit the SLURM jobs:
       sbatch submit_cluster.sh
   `submit_cluster.sh` runs `plot_slurm.py` on a SLURM COMPUTE node, where it
   starts the dask scheduler and submits the dask workers as nested SLURM jobs.
   Logs:   cluster_<jobid>.out/.err   (scheduler + client)
           slurm-<jobid>.out/.err     (one per dask worker)
   Result: output.root (in this directory).

3. (Optional) To use this project's python in your login-node shell:
       source setup.sh
   (this puts `condaEnv/bin` on the PATH and aliases `python` to it)

WARNING: do not run `python plot_slurm.py` directly on the login node.
The dask workers are SLURM jobs on compute nodes, and the compute network
cannot reach the login node (193.48.86.55). If the scheduler starts on the
login node, every worker fails with
       OSError: Timed out trying to connect to tcp://193.48.86.55:<port> after 30 s
       RuntimeError: Nanny failed to start
(one slurm-<jobid>.err per worker) and the run hangs with 0 registered workers.
The scheduler must run on a compute node, which is what
`sbatch submit_cluster.sh` does.

Helper scripts
--------------
copy_proxy.sh
    Refresh the grid-proxy copy used by the workflow. It checks that your
    proxy (default `/tmp/x509up_u<uid>`) exists and is still valid for at
    least 2 hours (`voms-proxy-info`), then copies it to
    `/home-pbs/<user>/proxy/x509up` -- the file that `submit_cluster.sh`
    and `plot_slurm.py` point `X509_USER_PROXY` at.
        ./copy_proxy.sh
    Exits non-zero with a message if the proxy is missing or expired.

find_dcache.py
    Recursively list the files of a dCache directory (default host
    sbgdcache.in2p3.fr) and build the `files.txt` file list consumed by
    `load_file()` in `plot_slurm.py`. A path given WITHOUT a `root://`
    prefix is automatically prefixed with `root://sbgdcache.in2p3.fr//`.
        ./condaEnv/bin/python3.12 find_dcache.py \
            /cms/phedex//store/user/... --human --out files.txt
    Useful options: `--pattern REGEX` (keep only full paths matching it),
    `--out FILE` (one root:// URL per line, exactly the files.txt format),
    `--limit N`, `--workers N` (parallel directory listings), `--human`,
    `--proxy FILE`. No proxy is needed from the login node (the IPHC
    dCache trusts the login-node IPs); from a SLURM compute node pass
    `--proxy /home-pbs/<user>/proxy/x509up` or export X509_USER_PROXY.

clean_logs.sh
    Remove the job logs and temporary files this workflow leaves behind:
    `slurm-<jobid>.out/.err` (one per dask worker),
    `<name>_<jobid>.out/.err` (cluster_<id>, diag_<id>, ...),
    `install*.log`, `run.log`, the contents of `dask-scratch-space/`
    and `__pycache__/`. It only works at the top level of this directory,
    never touches project files, `output.root`, `files.txt*` or the
    environments, and it refuses to clean `dask-scratch-space` while a
    SLURM job of yours is still running (a live dask cluster).
        ./clean_logs.sh --dry-run    # preview what would be removed
        ./clean_logs.sh              # remove all matching files
        ./clean_logs.sh --days 7     # only files older than 7 days
    Options: `--dry-run`, `--days N`, `--skip-dask`, `--skip-pycache`,
    `--force`, `-h/--help`.

tests/
    One-off test and diagnostic scripts (test.py, test_imports.py/.sh,
    diag.sh, diag2.py/.sh) plus old backups (files.txt.bkp/.v2,
    plot_slurm.py.bkp). Not part of the normal workflow.

