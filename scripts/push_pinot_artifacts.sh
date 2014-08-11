#!/bin/bash

###################################################################################################
##                   Script to push pinot artifacts to artifactory                              ###
## Usage ./scripts/push_pinot_artifacts.sh <Path_to_Artifactory_Config_file> <Root Directory>   ###
###################################################################################################

# if [ "$1" = "" ]; then
#   echo "Please provide the path to push_artifact credentials config file (e.g: /export/home/tester/.conf/push-artifact/pinot.conf)"
#   exit -1
# fi

# if [ "$2" = "" ]; then
#   echo "Please provide the path to the directory which has the parent pom.xml file"
#   exit -1
# fi


conf_file=$1;
root_dir=$2;
version=`grep "<version>" pom.xml | head -1 | sed -e 's/<version>//' | sed -e 's/<\\/version>//' | perl -lape 's/\s+//sg'`;


declare -a projects=("pinot-api" "pinot-broker" "pinot-common" "pinot-controller" "pinot-core" "pinot-hadoop" "pinot-server" "pinot-tools" "pinot-transport" "pinot-util");

for project in "${projects[@]}"
do
  echo "working on $project"
  (ls $project/target/*jar $project/target/*ivy | xargs -I pkg push-artifact --config-file  $conf_file  com.linkedin.pinot $project $version MNY pkg)
  if [ $? -ne 0 ]; then
    echo "Unable to push $project packages"
    exit -1
  fi
done