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

import re
import json
import os
import kazoo
import requests
import string
from collections import defaultdict
from exceptions import PinotException
from requests.exceptions import RequestException


class PinotResource(object):

  def __init__(self, config, logger, fabric, resource):
    self.config = config
    self.logger = logger
    self.fabric = fabric
    self.resource = resource

  def get_info(self):
    host = self.config.get_controller_url(self.fabric)
    r = requests.get('{0}/dataresources/{1}'.format(host, self.resource))

    # hacky stopgap
    c = r.json()['config'].values().pop()
    results = re.findall('\{(\w+=[^\}]+)\}', c)
    data = {}
    for result in results:
      result = result.split('{')[-1]
      m = re.search('([^=]+)=\[([^]]+)\]', result)
      if m:
        k, v = m.groups()
        data.update({k: map(string.strip, v.split(','))})
      else:
        m = re.findall('(\w+)=([^,]+)', result)
        if m:
          data.update(dict(m))

    return data

  def get_table_segments(self, table):
    host = self.config.get_controller_url(self.fabric)

    try:
      r = requests.get('{0}/dataresources/{1}/{2}/segments'.format(host, self.resource, table))
    except RequestException:
      error = 'Failed requesting /dataresources/ endpoint'
      self.logger.exception(error)
      raise PinotException(error)

    try:
      segments = r.json()['segments']
    except (ValueError, KeyError):
      error = 'Failed parsing json data'
      self.logger.exception(error)
      raise PinotException(error)

    return segments

  def get_nodes(self, pinot_zoo):
    if not re.match('^[a-zA-Z0-9_]+$', self.resource):
      error = 'potentially unsafe resource name: {0}'.format(self.resource)
      self.logger.error(error)
      raise PinotException(error)

    zk = pinot_zoo.get_handle()

    if not zk:
      return False

    root = self.config.get_zk_root(self.fabric)
    state_path = os.path.join(root, 'IDEALSTATES', self.resource)

    try:
      ideal_state = zk.get(state_path)[0]
    except kazoo.exceptions.NoNodeError:
      pinot_zoo.close()
      error = 'Failed getting statefile'
      self.logger.exception(error)
      raise PinotException(error)

    try:
      host_maps = json.loads(ideal_state)['mapFields']
    except (ValueError, KeyError):
      error = 'Failed parsing JSON IDEALSTATES data'
      self.logger.exception(error)
      raise PinotException(error)

    nodes_list = set()

    for servers in host_maps.itervalues():
      for hostname in servers.iterkeys():
        nodes_list.add(hostname)

    nodes_status = defaultdict(dict)

    for node in nodes_list:
      instance_path = os.path.join(root, 'LIVEINSTANCES', node)
      parts = node.split('_')
      node = parts[1]
      nodes_status[node]['helix_port'] = parts[2]
      nodes_status[node]['type'] = parts[0]
      if zk.exists(instance_path):
        nodes_status[node]['online'] = True
      else:
        nodes_status[node]['online'] = False

    pinot_zoo.close()

    return nodes_status
