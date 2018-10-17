#!/bin/bash
echo "*******************************************************"
echo "Building Pinot components"
echo "*******************************************************"

cd ..
mvn install -DskipTests -pl pinot-common,pinot-core,pinot-api -am -Pbuild-shaded-jar || exit 1
cd thirdeye

echo "*******************************************************"
echo "Building ThirdEye"
echo "*******************************************************"

mvn install -DskipTests || exit 1
