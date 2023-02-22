#!/bin/bash

wd=/afs/cern.ch/user/v/vannerom/work/MET/coffeaMET/

cd $wd

source setup_lcg.sh

cd $wd/analysis/

echo $PYTHONPATH

python run_Run2.py -i $1
