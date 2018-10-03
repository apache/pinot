#!/bin/bash
echo "*******************************************************"
echo "Attempting to build ThirdEye with Maven"
echo "*******************************************************"

mvn install -DskipTests || exit 1
