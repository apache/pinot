#!/bin/bash

# [Optional] Uncomment below lines to build with latest Pinot changes
# cd ..
# mvn install -DskipTests -pl pinot-common,pinot-core,pinot-spi,pinot-java-client -am -Pbuild-shaded-jar || exit 1
# cd thirdeye

echo "*******************************************************"
echo "Building ThirdEye"
echo "*******************************************************"

mvn install -DskipTests || exit 1
