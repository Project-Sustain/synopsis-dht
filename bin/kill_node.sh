#!/bin/bash

# Finds a process corresponding to Sustain Node and kills it.

# First use JPS to locate the DHT process.
r_pid=`jps | grep 'DHTStarter' | cut -d' ' -f1`

# If there is no DHT process, look for proxy process.
if [ -z ${r_pid} ]; then
    r_pid=`jps | grep 'ProxyStarter' | cut -d' ' -f1`
fi

# If JPS fails, use ps and grep
if [ -z ${r_pid} ]; then 
    r_pid=`ps -ef | grep 'java' | grep ${USER} | grep 'synopsis' | cut -d' ' -f2`
fi

if [ -z ${r_pid} ]; then
    echo 'No Sustain process found!'
else
    `kill -9 ${r_pid}`
    echo 'Killed the Sustain process! PID: '${r_pid}
fi
