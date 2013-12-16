#!/bin/bash

here=$( cd $(dirname $0); pwd -P )
log="/tmp/new_mon.log"
delta=300

echo "My PID is $$" > ${log}
python --version 2>> ${log}
echo >> ${log}

while true; do

    now=$( date +%s )

    echo "New monitor starting at $(date)" >> ${log}
    echo >> ${log}
    python $here/hourly-monitor.py >> ${log} 
    echo >> ${log}

    echo "Running again in ${delta} seconds..." >> ${log}
    new_now=$( date +%s )
    difference=$(( delta - new_now + now ))
    sleep ${difference} 
    echo >> ${log}

done

