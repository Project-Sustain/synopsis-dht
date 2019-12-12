#!/bin/bash

java_opts="-Xmx4096m"

if [[ ! -d "../lib/" ]]; then
  echo "Could not locate Gossamer jars. Exiting!"
  exit 1
fi

PRG="$0"
PRGDIR=`dirname "$PRG"`
SYN_HOME=`cd "$PRGDIR/.." ; pwd`

echo 'Synopsis2 Home: '${SYN_HOME}

# initialize the classpath
SYN_CLASSPATH=""
for f in ../lib/*
do
  if [ "$SYN_CLASSPATH" == "" ]
    then
    SYN_CLASSPATH=$f
  else
    SYN_CLASSPATH="$SYN_CLASSPATH":$f
  fi
done

#echo ${GOSSAMER_CLASSPATH}
# location of the log4j properties file
log4j_conf_file='file:'${SYN_HOME}'/lib/log4j.properties'
# append it to java opts
java_opts=${java_opts}' -Dlog4j.configuration='${log4j_conf_file}

#echo $java_opts

java \
    ${java_opts} \
    -cp ${SYN_CLASSPATH} \
    NodeStarter ${SYN_HOME}/conf/synopsis2.properties
