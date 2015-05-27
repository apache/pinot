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

import yaml
from exceptions import PinotException


class ConfigManager(object):
  def __init__(self, logger):
    self.path = 'config.yml'
    self.logger = logger
    self.config = {}

  def load(self):
    try:
      with open(self.path, 'r') as h:
        contents = h.read()
    except IOError:
      self.logger.exception('Failed reading config file')
      return

    try:
      self.config = yaml.load(contents)
    except yaml.YAMLError:
      self.logger.exception('Failed parsing config yaml')

  def update(self, conf):
    self.config.update(conf)

  def get_controller_url(self, fabric):
    try:
      return self.config['fabrics'][fabric]['controller_url']
    except KeyError:
      error = 'Failed getting controller url from config'
      self.logger.exception(error)
      raise PinotException(error)

  def get_zk_host(self, fabric):
    try:
      return self.config['fabrics'][fabric]['zk_host']
    except KeyError:
      error = 'Failed getting zookeeper host from config'
      self.logger.exception(error)
      raise PinotException(error)

  def get_zk_root(self, fabric):
    try:
      return self.config['fabrics'][fabric]['zk_root']
    except KeyError:
      error = 'Failed getting zookeeper root from config'
      self.logger.exception(error)
      raise PinotException(error)

  def get_fabrics(self):
    try:
      return self.config['fabrics'].keys()
    except KeyError:
      error = 'Failed getting list of fabrics from config'
      self.logger.exception(error)
      raise PinotException(error)

  def get_flask_port(self):
    try:
      return int(self.config['listen_port'])
    except (KeyError, ValueError):
      self.logger.exception('Failed getting flask port from config')
