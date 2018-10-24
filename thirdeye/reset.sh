#!/bin/bash
echo "*******************************************************"
echo "Cleaning up ThirdEye demo database"
echo "*******************************************************"

if [ -f ./thirdeye-pinot/config/h2db.mv.db ]; then
  rm ./thirdeye-pinot/config/h2db.mv.db
fi
