# Pinot Web UI

This is a simple flask app that lets you browse Pinot 2.0 clusters. It gathers info by hitting pinot-controller endpoints and querying ZK.

## Install

You need "pip":

    sudo yum install python-pip

Install Python libraries and create virtual environment:

    scripts/bootstrap.sh

Create config file:

    cp config.sample.yml config.yml
    vim config.yml # optional

## Running

Run in background:

    scripts/start.sh
    scripts/stop.sh

Or run in foreground:

    . activate
    python run.py

Logs will be written to logs/webui.log
