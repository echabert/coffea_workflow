#!/bin/bash
# Install the full requirements into the condaEnv's OWN site-packages so the
# environment is self-contained and works on any SLURM compute node (independent
# of the user home / user-site which differs across nodes).
#
# PYTHONNOUSERSITE=1 is crucial: it hides /home/echabert/.local/... from this
# python so that pip does NOT treat the packages as "already satisfied" and
# instead installs them into the condaEnv's own site-packages.
set -u
PY=/home-pbs/echabert/MCP/coffea_workflow/condaEnv/bin/python3.12
REQ=/home-pbs/echabert/MCP/coffea_workflow/requirements.txt
export PIP_USER=no
export PIP_DISABLE_PIP_VERSION_CHECK=1
export PYTHONNOUSERSITE=1
echo "Using python: $($PY -c 'import sys; print(sys.executable)')"
echo "site-packages target: $($PY -c 'import sys,os; print(os.path.join(sys.prefix,\"lib\",\"python3.12\",\"site-packages\"))')"
echo "Installing requirements into condaEnv site-packages ..."
"$PY" -m pip install --no-cache-dir --no-input -r "$REQ"
echo "PIP_EXIT=$?"
echo "=== post-install verification (must point inside condaEnv) ==="
"$PY" -c "import distributed, coffea, uproot, awkward, dask, dask_jobqueue, hist, numpy; print('imports OK'); print('distributed ->', distributed.__file__); print('coffea ->', coffea.__file__)"

