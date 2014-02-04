#!/bin/bash

source $HOME/.bash_profile

here=$( cd $(dirname $0); pwd -P )

if [ -z "${CUSTOM_PYTHON_VIRTUALENV}" ]; then
    echo "The path to the custom Python virtualenv is not set in the CUSTOM_PYTHON_VIRTUALENV variable."
    exit 1
fi

source ${CUSTOM_PYTHON_VIRTUALENV}/bin/activate

echo "Failover Monitor starting at $(date)"
echo
python -W ignore::DeprecationWarning $here/hourly-monitor.py
echo

