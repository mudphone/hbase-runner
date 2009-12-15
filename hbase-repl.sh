#!/bin/bash

# Set this to where you cloned the hbase-runner repository:
if [ -z "${HBASE_RUNNER_HOME}" ]; then
  ABS_PATH="$(cd "${0%/*}" 2>/dev/null; echo "$PWD"/"${0##*/}")"
  export HBASE_RUNNER_HOME=`dirname ${ABS_PATH}`
fi

# You can probably leave the rest alone:
LIB_DIR="${HBASE_RUNNER_HOME}/lib"
CP_JARS="${LIB_DIR}/*"
CP="${CP_JARS}:${HBASE_RUNNER_HOME}/src"

# Add extra classpath, if any -- for example the location of hbase-site.xml.
if [ ! -z "${HBR_XCP}" ]; then
  CP="${CP}:${HBR_XCP}"
fi

# REPL initialization
REPL_INIT="-i ${HBASE_RUNNER_HOME}/init.clj"

# If script given, run it:
if [ ! -z "$1" ]; then 
  scriptname=$1
  echo "Running script: ${scriptname}"
  java -cp ${CP} clojure.main ${REPL_INIT} $scriptname -- $*
  exit
fi

# For rlwrap
RLWRAP_PATH=`which rlwrap`
CLJ_COMPLETIONS_PATH="${HOME}/.clj_completions"
if [ -x "${RLWRAP_PATH}" -a -r "${CLJ_COMPLETIONS_PATH}" ]; then
  BREAK_CHARS="(){}[],^%$#@\"\";:''|\\"
  java_cmd="java -cp ${CP} clojure.main ${REPL_INIT} -r"
  cmd="rlwrap --remember -c -b ${BREAK_CHARS} -f ${CLJ_COMPLETIONS_PATH} ${java_cmd}"
else
  # If no rlwrap available, use JLine instead:
    cmd="java -cp ${CP} jline.ConsoleRunner clojure.main ${REPL_INIT} -r"
fi
 
echo "Running: ${cmd}"
${cmd}
