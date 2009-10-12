#!/bin/bash

# Set this to where you cloned the hbase-runner repository:
HBASE_RUNNER_HOME="${HOME}/work/clojure/hbase-runner"


# You can probably leave the rest alone:
CLOJURE_DIR="${HBASE_RUNNER_HOME}/lib/java"
CP_JARS="${CLOJURE_DIR}/*"
CP="${CP_JARS}:${HBASE_RUNNER_HOME}/src"

# If script given, run it:
if [ ! -z "$1" ]; then 
  scriptname=$1
  echo "Running script: ${scriptname}"
  java -cp ${CP} clojure.lang.Script $scriptname -- $*
  exit
fi

# For rlwrap
RLWRAP_PATH=`which rlwrap`
CLJ_COMPLETIONS_PATH="${HOME}/.clj_completions"
if [ -x "${RLWRAP_PATH}" -a -r "${CLJ_COMPLETIONS_PATH}" ]; then
  BREAK_CHARS="(){}[],^%$#@\"\";:''|\\"
  java_cmd="java -cp ${CP} clojure.lang.Repl"
  cmd="rlwrap --remember -c -b ${BREAK_CHARS} -f ${CLJ_COMPLETIONS_PATH} ${java_cmd}"
else
  cmd="java -cp ${CP} jline.ConsoleRunner clojure.lang.Repl"
fi
 
echo "Running: ${cmd}"
${cmd}
