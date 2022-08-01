#!/bin/bash
# ------------------------------------------------------------------
# [Author] Title
#          Description
# ------------------------------------------------------------------

VERSION=0.1.0
SUBJECT=nuts-deploy
USAGE="Usage: deploy -hv [dev,prod] [all, <s2,s2est,dem,demest>]"

S2="S2_workflow.py"
S2EST="S2EST_workflow.py"
DEM="DEM_workflow.py"
DEMEST="DEMEST_workflow.py"
PRJ="NUTS"

S2_DEV="S2_DEV_workflow.py"
S2EST_DEV="S2EST_DEV_workflow.py"
DEM_DEV="DEM_DEV_workflow.py"
DEMEST_DEV="DEMEST_DEV_workflow.py"
PRJ_DEV="NUTS-DEV"

# --- Options processing -------------------------------------------
if [ $# == 0 ] ; then
    echo $USAGE
    exit 1;
fi

while getopts ":i:vh" optname
  do
    case "$optname" in
      "v")
        echo "Version $VERSION"
        exit 0;
        ;;
      "i")
        echo "-i argument: $OPTARG"
        ;;
      "h")
        echo $USAGE
        exit 0;
        ;;
      "?")
        echo "Unknown option $OPTARG"
        exit 0;
        ;;
      ":")
        echo "No argument value for option $OPTARG"
        exit 0;
        ;;
      *)
        echo "Unknown error while processing options"
        exit 0;
        ;;
    esac
  done

shift $(($OPTIND - 1))

param1=$1
param2=$2

if [[ -z $param2 ]]; then
  echo "Selelet all, s2, s2est, dem or demest"
  exit
fi


# --- Locks -------------------------------------------------------
LOCK_FILE=/tmp/$SUBJECT.lock
if [ -f "$LOCK_FILE" ]; then
   echo "Script is already running"
   exit
fi

trap "rm -f $LOCK_FILE" EXIT
touch $LOCK_FILE

# --- Body --------------------------------------------------------
#  SCRIPT LOGIC GOES HERE
case "$param1" in
      "dev")
        S2=$S2_DEV
        S2EST=$S2EST_DEV
        DEM=$DEM_DEV
        DEMEST=$DEMEST_DEV
        PRJ=$PRJ_DEV
        python config.py dev
        ;;
      "prod")
        python config.py prod
        ;;
      *)
        echo "Unknown environment '$param1' "
        exit 0;
        ;;
esac

echo "Build and Deploy '$param1' environment"

if [[ $param2 == "all"* ]]; then
  VAR_WF=" -p $S2 -p $S2EST -p $DEM -p $DEMEST"
else
  VAR_WF=""
  for ((i=2; i<=$#; i++)); do
      #echo "${!i}"
      case "${!i}" in
            "s2")
              VAR_WF+=" -p $S2"
              ;;
            "s2est")
              VAR_WF+=" -p $S2EST"
              ;;
            "dem")
              VAR_WF+=" -p $DEM"
              ;;
            "demest")
              VAR_WF+=" -p $DEMEST"
              ;;
            *)
              echo "Unknown flow '"${!i}"' "
              exit 0;
              ;;
        esac
  done
fi

if [[ -z $VAR_WF ]]; then
  echo "No flow to deploy!"
  exit
else
  PREFECT__SERVER__HOST=https://XXXXXXXXXX/prefect-api/?apikey=XXXXXXXXXX prefect register$VAR_WF --project "$PRJ" -l k8s
fi

# -----------------------------------------------------------------
