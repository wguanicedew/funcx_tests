set -o vi
ulimit -S -s unlimited

unset SUDO_UID SUDO_GID SUDO_USER

# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/global/common/software/nersc/pm-2022q2/sw/python/3.9-anaconda-2021.11/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/global/common/software/nersc/pm-2022q2/sw/python/3.9-anaconda-2021.11/etc/profile.d/conda.sh" ]; then
        . "/global/common/software/nersc/pm-2022q2/sw/python/3.9-anaconda-2021.11/etc/profile.d/conda.sh"
    else
        export PATH="/global/common/software/nersc/pm-2022q2/sw/python/3.9-anaconda-2021.11/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<

nvidia-smi
#module load tensorflow/2.6.0
conda activate myenv
