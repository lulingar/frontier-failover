#!/bin/bash

here=$( cd $(dirname $0); pwd -P )
log="/tmp/new_mon.log"
delta_minutes=20

delta=$(( delta_minutes*60 ))

echo "My PID is $$" > ${log}
python --version 2>> ${log}
echo >> ${log}

while true; do

    now=$( date +%s )

    echo "New monitor starting at $(date)" >> ${log}
    echo >> ${log}
    python -W ignore::DeprecationWarning $here/hourly-monitor.py >> ${log} 2>&1
    echo >> ${log}

    echo "Running again in ${delta_minutes} minutes..." >> ${log}
    new_now=$( date +%s )
    difference=$(( delta - new_now + now ))
    sleep ${difference} 
    echo >> ${log}

done

