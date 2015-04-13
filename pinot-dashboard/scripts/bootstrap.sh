#!/bin/bash

if pwd | grep -qP '/scripts$'; then
  cd ..
fi

# Create venv if doesn't exist and install pip packages.
# This is a stopgap until this uses mint/etc

if [ -e activate ]; then
  . activate
else
  mkdir .env
  virtualenv .env/pfui
  ln -s .env/pfui/bin/activate 
  . activate
fi

pip install -r requirements.txt

mkdir -p logs
