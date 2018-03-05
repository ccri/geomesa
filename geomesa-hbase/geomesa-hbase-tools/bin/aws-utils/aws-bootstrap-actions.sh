#!/usr/bin/env bash
#
# Core Bootstrap Script for GeoMesa on AWS EMR
#
# Note this script runs twice. It is initiated by AWS before the cluster is ready. During its lifetime it downloads a
# copy of the tools distribution and on the master spawns a clone of itself that waits for the cluster to finish
# setting up before continuing the bootstrap process.

logFile=/tmp/bootstrap.log
function log() {
  timeStamp=$(date +"%T")
  echo "${timeStamp}| ${@}" | tee -a $logFile
}

log "Bootstrap Actions Spawned"

ARGS=($@) # Save the input args so we can pass them to the child.
VERSION=%%project.version%%
GM_TOOLS_DIST="%%gmtools.assembly.name%%"
GM_TOOLS_HOME="/opt/${GM_TOOLS_DIST}"
log "GM TOOLS DIST: ${GM_TOOLS_DIST}"
CHILD=false

while [[ $# -gt 0 ]]; do
  case $1 in
    -z|--zeppelin)
      ZEPPELIN=true
      log "Zeppelin Enabled"
      shift
      ;;
    -j*|--jupyter*)
      JUPYTER=true
      JUPYTER_PASSWORD="${i#*=}"
      if [[ -z "${JUPYTER_PASSWORD}" ]]; then
        JUPYTER_PASSWORD="geomesa"
      fi
      log "Jupyter Enabled. Password: ${JUPYTER_PASSWORD}"
      shift
      ;;
    -c=*|--container=*)
      CONTAINER="${i#*=}"
      log "Container: ${CONTAINER}"
      shift
      ;;
    --child)
      CHILD=true
      shift
      ;;
    *)
      log "[Warning] Unknown parameter: ${i}"
      shift
      ;;
  esac
done

# Validate Parameters
if [[ -z "${CONTAINER}" ]]; then
  log "S3 container is required"
  exit 2
elif [[ "${CONTAINER}" != */ ]]; then
  # We need a trailing '/' for consistency
  CONTAINER="${CONTAINER}/"
fi

if [[ "${CHILD}" != "true" ]]; then
  log "Parent Process"

  # Parses a configuration file put in place by EMR to determine the role of this node
  isMaster=$(jq '.isMaster' /mnt/var/lib/info/instance.json)
  log "isMaster: ${isMaster}" 

  ### MAIN ####
  log "Main" 
  if [[ "${isMaster}" == "true" ]]; then
    log "Copying Resources Locally" 
    #sudo curl -sSfo 'https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-${VERSION}/${GM_TOOLS_DIST}-bin.tar.gz' '/opt/${GM_TOOLS_DIST}-bin.tar.gz'
    sudo aws s3 cp ${CONTAINER}${GM_TOOLS_DIST}-bin.tar.gz /opt/${GM_TOOLS_DIST}-bin.tar.gz

    log "Moving to /opt/" 
    pushd /opt/

    log "Extracting resources" 
    sudo tar xf ${GM_TOOLS_DIST}-bin.tar.gz

    log "Setting Permissions"
    sudo chown -R ec2-user ${GM_TOOLS_HOME}

    log "Starting Child Script" 
    sudo nohup ${GM_TOOLS_HOME}/aws-utils/aws-bootstrap-actions.sh --child ${ARGS[@]} &>/dev/null &

    log "Parent Done"
    exit 0

  else
    log "NO-OP"
    exit 0
  fi

else
  log "Child Process"

  # Wait until hbase is installed
  log "Waiting for HBase to be installed" 
  hbaseInstalled=$(ls /usr/bin/ | grep hbase)
  while [[ -z "${hbaseInstalled}" ]]; do
    sleep 3
    hbaseInstalled=$(ls /usr/bin/ | grep hbase)
    log "Sleeping" 
  done

  # Wait until hbase is running
  log "Setting up HBase status script" 
  echo "status" >> /tmp/status
  echo "exit" >> /tmp/status
  timeout=0
  log "Waiting for HBase to start" 
  status=$(/usr/bin/hbase shell < /tmp/status | grep "active master")
  log "${status}" 
  while [[ -z "${status}" ]]; do
    let "timeout=timeout+1"
    log "Sleeping" 
    sleep 1
    status=$(/usr/bin/hbase shell < /tmp/status | grep "active master")
  done

  log "Bootstrapping GeoMesa" 
  sudo ${GM_TOOLS_HOME}/aws-utils/aws-bootstrap-geomesa-hbase.sh

  if [[ -n "${ZEPPELIN}" ]]; then
    ( ${GM_TOOLS_HOME}/bin/aws-utils/aws-bootstrap-geomesa-zeppelin.sh )
  fi

  if [[ -n "${JUPYTER}" ]]; then
    ( ${GM_TOOLS_HOME}/bin/aws-utils/aws-bootstrap-geomesa-jupyter.sh "${JUPYTER_PASSWORD}" )
  fi

  log "Bootstrap Complete"

fi
