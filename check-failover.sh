#!/bin/bash

source $HOME/.bash_profile

here=$( cd $(dirname $0); pwd -P )

if [ -z "${CUSTOM_PYTHON_VIRTUALENV}" ]; then
    echo "Please set the path to the custom Python virtualenv is in the CUSTOM_PYTHON_VIRTUALENV variable."
    exit 1
fi

echo "Failover Monitor starting at $(date)"
echo
${CUSTOM_PYTHON_VIRTUALENV}/bin/python -W ignore::DeprecationWarning $here/hourly-monitor.py
echo
echo "Failover Monitor done at $(date)"

