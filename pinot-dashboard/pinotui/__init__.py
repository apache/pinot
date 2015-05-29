#!/usr/bin/env python2.6
#
# Copyright (C) 2015 LinkedIn Corp. (pinot-core@linkedin.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from flask import Blueprint, Flask, jsonify, request, send_from_directory, g

from pinot_resource import PinotResource
from pinot_fabric import PinotFabric
from exceptions import PinotException
from zk import PinotZk
from addict import Dict
import logging
import re

from config import ConfigManager

app = Flask(__name__)
app.config['DEBUG'] = True

logger = logging.getLogger()
pinotui = Blueprint('pinotui', __name__, static_folder='static')

config = ConfigManager(logger)
config.load()


@pinotui.record_once
def init(state):

  newconf = Dict()
  for k, v in state.app.config.iteritems():
    m = re.match('fabrics:([^:]+):([^$]+)', k)
    if m:
      newconf['fabrics'][m.group(1)][m.group(2)] = v
  config.update(newconf)


@pinotui.route('/runpql/<string:fabric>')
def send_pql(fabric):
  try:
    pinot_fabric = PinotFabric(config, logger, fabric)
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed getting fabric {0}'.format(e)))

  pql = request.args.get('pql')

  try:
    return jsonify(dict(success=True, result=pinot_fabric.run_pql(pql)))
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed running PQL: {0}'.format(e)))


@pinotui.route('/clusters/<string:fabric>')
def list_resources(fabric):
  try:
    pinot_fabric = PinotFabric(config, logger, fabric)
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed getting fabric {0}'.format(e)))

  try:
    resources = pinot_fabric.get_resources()
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed getting fabric: {0}'.format(e)))

  try:
    zk = PinotZk(config, logger, fabric)
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed getting ZK: {0}'.format(e)))

  return jsonify(dict(success=True, clusters=resources, nodes=pinot_fabric.get_nodes(zk.get_handle())))


@pinotui.route('/cluster/<string:fabric>/<string:cluster>')
def cluster_info(fabric, cluster):
  resource = PinotResource(config, logger, fabric, cluster)

  try:
    zk = PinotZk(config, logger, fabric)
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed getting ZK: {0}'.format(e)))

  zkhandle = zk.get_handle()

  return jsonify(dict(success=True, info={}, tables=resource.get_tables(zkhandle), nodes=resource.get_nodes(zkhandle)))


@pinotui.route('/segments/<string:fabric>/<string:cluster>/<string:table>')
def get_segments(fabric, cluster, table):
  resource = PinotResource(config, logger, fabric, cluster)

  try:
    zk = PinotZk(config, logger, fabric)
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed getting ZK: {0}'.format(e)))

  zkhandle = zk.get_handle()
  return jsonify(dict(success=True, segments=resource.get_table_segments(table, zkhandle)))


@pinotui.route('/fabrics')
def list_fabrics():
  try:
    return jsonify(dict(success=True, fabrics=config.get_fabrics()))
  except PinotException as e:
    return jsonify(dict(success=False, error_message='Failed getting fabrics: {0}'.format(e)))


# Store our blueprint's static content in a different path to not conflict with
# tools team's static
@pinotui.route('/pinot_static/<path:filename>')
def get_static(filename):
  return send_from_directory(os.path.join(os.path.dirname(__file__), 'static'), filename)


@pinotui.route('/authinfo')
def authinfo():

  try:
    # Hack to pull details from the mppy wrapper
    is_authenticated = g.identity._is_authenticated

    try:
      username = g.identity.get_username()
    except:
      username = None

    return jsonify(dict(
      success=True,
      supported=True,
      authed=is_authenticated,
      username=username
    ))

  except:
    return jsonify(dict(success=True, supported=False))


@pinotui.route('/')
def index():

  # not using flask.render_template() as the angular-js {{ }} notation throws it off
  return open(os.path.join(os.path.dirname(__file__), 'templates/home.html')).read()
