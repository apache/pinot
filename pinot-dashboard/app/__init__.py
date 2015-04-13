#!/usr/bin/env python2.6

import os
from flask import Flask, jsonify, request

from pinot_resource import PinotResource
from pinot_fabric import PinotFabric
from exceptions import PinotException
from zk import PinotZk

from config import ConfigManager

app = Flask(__name__)
app.config['DEBUG'] = True

config = ConfigManager(app.logger)
config.load()


@app.route('/runpql/<string:fabric>')
def send_pql(fabric):
  try:
    pinot_fabric = PinotFabric(config, app.logger, fabric)
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed getting fabric {0}'.format(e)))

  pql = request.args.get('pql')

  try:
    return jsonify(dict(success=True, result=pinot_fabric.run_pql(pql)))
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed running PQL: {0}'.format(e)))


@app.route('/clusters/<string:fabric>')
def list_resources(fabric):
  try:
    resources = PinotFabric(config, app.logger, fabric).get_resources()
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed getting fabric: {0}'.format(e)))

  return jsonify(dict(success=True, clusters=resources))


@app.route('/cluster/<string:fabric>/<string:cluster>')
def cluster_info(fabric, cluster):
  resource = PinotResource(config, app.logger, fabric, cluster)

  try:
    zk = PinotZk(config, app.logger, fabric)
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed getting ZK: {0}'.format(e)))

  return jsonify(dict(success=True, info=resource.get_info(), nodes=resource.get_nodes(zk)))


@app.route('/segments/<string:fabric>/<string:cluster>/<string:table>')
def get_segments(fabric, cluster, table):
  resource = PinotResource(config, app.logger, fabric, cluster)
  return jsonify(dict(success=True, segments=resource.get_table_segments(table)))


@app.route('/fabrics')
def list_fabrics():
  try:
    return jsonify(dict(success=True, fabrics=config.get_fabrics()))
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed getting fabrics: {0}'.format(e)))


@app.route('/')
def index():

  # not using flask.render_template() as the angular-js {{ }} notation throws it off
  return open(os.path.join(os.path.dirname(__file__), 'templates/home.html')).read()
