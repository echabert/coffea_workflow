# Point `python` / `python3` at the self-contained condaEnv used by the SLURM
# jobs. The coffea/dask packages are installed INSIDE this env (see
# install_env.sh), so it works identically on the login node and on any SLURM
# compute node (it does not rely on the per-node user site ~/.local).
export PATH="/home-pbs/echabert/MCP/coffea_workflow/condaEnv/bin:$PATH"
alias python=/home-pbs/echabert/MCP/coffea_workflow/condaEnv/bin/python3.12
alias python3=/home-pbs/echabert/MCP/coffea_workflow/condaEnv/bin/python3.12
