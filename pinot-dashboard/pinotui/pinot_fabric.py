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

import requests
import os
import json
from exceptions import PinotException
from requests.exceptions import RequestException


class PinotFabric(object):

  def __init__(self, config, logger, fabric):
    self.config = config
    self.fabric = fabric
    self.logger = logger

  def get_resources(self):
    host = self.config.get_controller_url(self.fabric)
    url = '{0}/tenants?type=server'.format(host)
    try:
      r = requests.get(url)
    except RequestException:
      error = 'Failed hitting {0}'.format(url)
      self.logger.exception(error)
      raise PinotException(error)

    try:
      data = r.json()
    except ValueError:
      error = 'Failed parsing json data'
      self.logger.exception(error)
      raise PinotException(error)

    return map(lambda x: x.split('_O')[0], data['SERVER_TENANTS'])

  def get_nodes(self, zk):

    root = self.config.get_zk_root(self.fabric)
    state_path = os.path.join(root, 'CONFIGS', 'PARTICIPANT')

    nodes = []

    for node in zk.get_children(state_path):
      data = zk.get(os.path.join(state_path, node))
      extracted = json.loads(data[0])
      nodes.append({
        'nodename': node,
        'host': extracted['simpleFields']['HELIX_HOST'].split('_', 1)[-1],
        'type': node.split('_')[0],
        'tags': extracted['listFields']['TAG_LIST']
      })

    return nodes

  def run_pql(self, pql):
    host = self.config.get_controller_url(self.fabric)
    url = '{0}/pql'.format(host)

    try:
      r = requests.get(url, params={'pql': pql})
    except RequestException:
      error = 'Failed hitting {0}'.format(url)
      self.logger.exception(error)
      raise PinotException(error)

    try:
      result = r.json()
    except ValueError:
      result = r.text
      raise PinotException(result)

    return {
      'success': True,
      'result': result
    }

  def create_broker_tenant(self, name, instances):
    url = '{0}/tenants'.format(self.config.get_controller_url(self.fabric))
    try:
      result = requests.post(url, json={
        'tenantRole': 'broker',
        'numberOfInstances': instances,
        'tenantName': name,
      })
    except RequestException:
      error = 'Failed hitting {0}'.format(url)
      self.logger.exception(error)
      raise PinotException(error)

    if result.status_code != 200:
      raise PinotException(result.text)

    return True

  def create_server_tenant(self, name, instances, offline_instances=0, realtime_instances=0):
    url = '{0}/tenants'.format(self.config.get_controller_url(self.fabric))
    try:
      result = requests.post(url, json={
        'tenantRole': 'server',
        'numberOfInstances': instances,
        'tenantName': name,
        'offlineInstances': offline_instances,
        'realtimeInstances': realtime_instances
      })
    except RequestException:
      error = 'Failed hitting {0}'.format(url)
      self.logger.exception(error)
      raise PinotException(error)

    if result.status_code != 200:
      raise PinotException(result.text)

    return True
