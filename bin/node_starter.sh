#!/bin/bash

java_opts="-Xmx4096m"

if [[ ! -d "../lib/" ]]; then
  echo "Could not locate Sustain jars. Exiting!"
  exit 1
fi

PRG="$0"
PRGDIR=`dirname "$PRG"`
SST_HOME=`cd "$PRGDIR/.." ; pwd`

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
    sustain.synopsis.dht.DHTNodeStarter ${SST_HOME}/conf/dht-node-config.yaml
