#!/bin/bash

source $HOME/.bashrc

main()
{
    here=$( cd $(dirname $0); pwd -P )
    config_file="${here}/config.json"

    if [ -z "${CUSTOM_PYTHON_VIRTUALENV}" ]; then
        echo "Please set the path to the custom Python virtualenv is in the CUSTOM_PYTHON_VIRTUALENV variable."
        exit 1
    fi

    cd ${here}
    record_files=$( grep -o '[^"]*\.csv' ${config_file} )
    for file in ${record_files}; do
        cp -v $file $file.0
    done

    io_echo
    io_echo "========================================================="
    io_echo "Failover Monitor starting at $(date)"
    io_echo
    ${CUSTOM_PYTHON_VIRTUALENV}/bin/python -W ignore::DeprecationWarning $here/hourly-monitor.py ${config_file} 
    io_echo
    io_echo "Failover Monitor done at $(date)"
}

io_echo()
{
    echo "$@"
    echo "$@" >&2
}

main "$@"
