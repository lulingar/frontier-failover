#!/bin/bash

source $HOME/.bash_profile

here=$( cd $(dirname $0); pwd -P )

if [ -z "${CUSTOM_PYTHON_VIRTUALENV}" ]; then
    echo "Please set the path to the custom Python virtualenv is in the CUSTOM_PYTHON_VIRTUALENV variable."
    exit 1
fi

record_files=$( grep -o '[^"]*\.csv' $here/instance_config.json )
for file in ${record_files}; do
    cp $file $file.0
done

echo
echo "========================================================="
echo "Failover Monitor starting at $(date)"
echo
${CUSTOM_PYTHON_VIRTUALENV}/bin/python -W ignore::DeprecationWarning $here/hourly-monitor.py
echo
echo "Failover Monitor done at $(date)"

