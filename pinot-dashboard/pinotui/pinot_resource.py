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

import json
import os
import requests
from collections import defaultdict
from exceptions import PinotException
from requests.exceptions import RequestException
from pinot_fabric import PinotFabric


class PinotResource(object):

  def __init__(self, config, logger, fabric, resource):
    self.config = config
    self.logger = logger
    self.fabric = fabric
    self.resource = resource

  def get_info(self):
    return {}

  def get_table_segments(self, table, zk):
    segments = []

    root = self.config.get_zk_root(self.fabric)
    path = os.path.join(root, 'PROPERTYSTORE', 'SEGMENTS', table)

    external_path = os.path.join(root, 'EXTERNALVIEW', table)
    external_data = json.loads(zk.get(external_path)[0])

    for segment in zk.get_children(path):
      segment_path = os.path.join(path, segment)
      segment_data = json.loads(zk.get(segment_path)[0])

      try:
        segments.append({
          'name': segment_data['id'],
          'info': segment_data['simpleFields'],
          'availability': external_data['mapFields'][segment]
        })
      except KeyError:
        self.logger.exception('failed getting segment data from zk')

    return segments

  def get_table_info(self, table, zk):
    root = self.config.get_zk_root(self.fabric)
    path = os.path.join(root, 'PROPERTYSTORE', 'CONFIGS', 'TABLE', table)
    data = json.loads(zk.get(path)[0])
    return data

  def get_nodes(self, zk):
    nodes_status = defaultdict(dict)
    pinot_fabric = PinotFabric(self.config, self.logger, self.fabric)
    for node in pinot_fabric.get_nodes(zk):
      match = False
      for tag in node['tags']:
        if tag.startswith(self.resource):
          match = True
          break
      if not match:
        continue
      instance_path = os.path.join(self.config.get_zk_root(self.fabric), 'LIVEINSTANCES', node['nodename'])
      host = node['host']
      nodes_status[host]['type'] = node['type']
      nodes_status[host]['helix_port'] = 0
      nodes_status[host]['online'] = zk.exists(instance_path)

    return nodes_status

  def get_tables(self, zk):
    tables = []
    root = os.path.join(self.config.get_zk_root(self.fabric), 'PROPERTYSTORE', 'CONFIGS', 'TABLE')
    for table in zk.get_children(root):
      path = os.path.join(root, table)
      data = json.loads(zk.get(path)[0])
      tenants = json.loads(data['simpleFields']['tenants'])
      if tenants['server'] != self.resource:
        continue

      tables.append({
        'name': table,
        'type': data['simpleFields']['tableType']
      })

    return tables

  def create_table(self, settings):

    try:
      data = {
        'tableName': settings['name'],
        'tableConfig': {
          'retention.TimeUnit': settings['retention_unit'],
          'retention.TimeValue': settings['retention_value'],
          'segment.pushFrequency': settings['push_frequency'],
          'segment.pushType': settings['push_type'],
          'replication': settings['replication'],
          'schemaName': settings['schema']
        },
        'tableIndexConfig': {
          'invertedIndexColumns': settings['inverted_index_columns'],
          'loadMode': settings['loadmode'],
          'lazyLoad': settings['lazyload']
        },
        'tenants': {
          'broker': settings['broker_tenant'],
          'server': settings['server_tenant'],
        },
        'tableType': settings['type'],
        'metadata': {
          'd2.name': settings['d2']
        }
      }
    except KeyError as e:
      raise PinotException('Missing key: {0}'.format(e))

    url = '{0}/tables'.format(self.config.get_controller_url(self.fabric))

    try:
      result = requests.post(url, json=data)
    except RequestException as e:
      raise PinotException(e)

    if result.status_code != 200:
      raise PinotException(result.text)

    return True

  def delete_segment(self, segment_name):
    url = '{0}/datafiles/{1}/{2}'.format(self.config.get_controller_url(self.fabric), self.resource, segment_name)
    try:
      result = requests.delete(url)
    except RequestException as e:
      raise PinotException(e)

    if result.status_code != 200:
      raise PinotException(result.text)

    return True

  def delete(self):
    url = '{0}/datafiles/{1}'.format(self.config.get_controller_url(self.fabric), self.resource)
    try:
      result = requests.delete(url)
    except RequestException as e:
      raise PinotException(e)

    if result.status_code != 200:
      raise PinotException(result.text)

    return True
