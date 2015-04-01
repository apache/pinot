#!/usr/bin/env python2.6

import os
from flask import Flask, jsonify, request

from pinot_resource import PinotResource
from pinot_fabric import PinotFabric
from zk import PinotZk

from config import ConfigManager

app = Flask(__name__)
app.config['DEBUG'] = True

config = ConfigManager(app.logger)
config.load()


@app.route('/runpql/<string:fabric>')
def send_pql(fabric):
  pql = request.args.get('pql')
  pinot_fabric = PinotFabric(config, fabric)
  return jsonify(dict(pinot_fabric.run_pql(pql)))


@app.route('/clusters/<string:fabric>')
def list_resources(fabric):
  pinot_fabric = PinotFabric(config, fabric)
  return jsonify(dict(clusters=pinot_fabric.get_resources()))


@app.route('/cluster/<string:fabric>/<string:cluster>')
def cluster_info(fabric, cluster):
  resource = PinotResource(config, app.logger, fabric, cluster)
  zk = PinotZk(config, fabric)
  return jsonify(dict(info=resource.get_info(), nodes=resource.get_nodes(zk)))


@app.route('/segments/<string:fabric>/<string:cluster>/<string:table>')
def get_segments(fabric, cluster, table):
  resource = PinotResource(config, app.logger, fabric, cluster)
  return jsonify(dict(segments=resource.get_table_segments(table)))


@app.route('/fabrics')
def list_fabrics():
  return jsonify(dict(fabrics=config.get_fabrics()))


@app.route('/')
def index():

  # not using flask.render_template() as the angular-js {{ }} notation throws it off
  return open(os.path.join(os.path.dirname(__file__), 'templates/home.html')).read()
