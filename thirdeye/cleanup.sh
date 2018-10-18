#!/bin/bash
echo "*******************************************************"
echo "Cleaning up ThirdEye build and demo database"
echo "*******************************************************"

mvn clean

rm ./thirdeye-pinot/config/h2db.mv.db

