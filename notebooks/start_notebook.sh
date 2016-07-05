#!/bin/bash

module load conda
source activate py2k

export PYTHONPATH="../lib:${PYTHONPATH}"
jupyter notebook
