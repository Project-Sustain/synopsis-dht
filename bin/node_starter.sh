#!/bin/bash

if [[ $1 == "proxy" ]]; then
  MAIN_CLASS=sustain.synopsis.proxy.ProxyStarter
  echo 'Starting a DHT node'
else
  MAIN_CLASS=sustain.synopsis.dht.DHTNodeStarter
  echo 'Starting a proxy node'
fi

total_m=$(awk '/MemTotal/{print $2}' /proc/meminfo | xargs -I {} echo "scale=4; {}/1024^2" | bc)
heap_m=$(echo "$total_m * 0.75" | bc -l)
xmx=$(( ${heap_m%.*} + 1))

java_opts="-Xmx"${xmx}"g"
echo 'Allocated Heap: '$xmx'g, Total: '$total_m'g'

if [[ ! -d "../lib/" ]]; then
  echo "Could not locate Sustain jars. Exiting!"
  exit 1
fi

PRG="$0"
PRGDIR=$(dirname "$PRG")
SST_HOME=$(cd "$PRGDIR/.." || exit ; pwd)

echo 'Sustain Home: '${SST_HOME}

# initialize the classpath
SST_CLASSPATH=""
for f in ../lib/*
do
  if [ "$SST_CLASSPATH" == "" ]
    then
    SST_CLASSPATH=$f
  else
    SST_CLASSPATH="$SST_CLASSPATH":$f
  fi
done

#echo ${GOSSAMER_CLASSPATH}
# location of the log4j properties file
log4j_conf_file='file:'${SST_HOME}'/lib/log4j.properties'
# append it to java opts
java_opts=${java_opts}' -Dlog4j.configuration='${log4j_conf_file}


java \
    ${java_opts} \
    -cp ${SST_CLASSPATH} \
    ${MAIN_CLASS} ${SST_HOME}/conf/dht-node-config.yaml
