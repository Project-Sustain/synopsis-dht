#!/bin/bash

r_pid=`ps -ef | grep 'java' | grep ${USER} | grep 'synopsis' | cut -d ' ' -f3`

if [ -z ${r_pid} ]; then
    echo 'No Sustain process found!'
else
    `kill -9 ${r_pid}`
    echo 'Killed the Sustain process! PID: '${r_pid}
fi
