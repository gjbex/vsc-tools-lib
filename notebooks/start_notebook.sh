#!/bin/bash

case ${VSC_INSTITUTE_CLUSTER} in
    thinking)
        source $VSC_DATA/miniconda2/env.sh
        source activate admin
        ;;
    *)
        module load conda
        source activate py2k
        ;;
esac


export PYTHONPATH="../lib:${PYTHONPATH}"
jupyter notebook
