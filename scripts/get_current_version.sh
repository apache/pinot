#!/bin/bash

if [ "$1" = "" ]; then
  echo "Please provide the path to the directory which has the parent pom.xml file"
  exit -1
fi

root_dir=$1;
version=`grep "<version>" $root_dir/pom.xml | head -1 | sed -e 's/<version>//' | sed -e 's/<\\/version>//' | perl -lape 's/\s+//sg'`;
echo "$version"
