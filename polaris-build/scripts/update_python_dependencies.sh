#!/usr/bin/env bash

if [[ -f requirements.txt  && -d polaris-build ]]
then
    echo "-----------------------Install dependencies for python package: ${PWD}-----------------------------------"
    pip wheel  -w /project/wheels --find-links /project/wheels -c polaris-build/system-pip-constraints.txt -r requirements.txt
    echo "pip install: ${PWD}"
    pip install --no-index --find-links /project/wheels -c polaris-build/system-pip-constraints.txt -r requirements.txt
    echo "-------------------------Finished installing dependencies for python package: ${PWD}---------------------"
else
    echo "Current working directory does not have requirements.txt or a polaris-build directory. Cannot continue"
fi
