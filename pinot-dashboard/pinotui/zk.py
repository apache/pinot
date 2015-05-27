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

from kazoo.client import KazooClient
import kazoo
from exceptions import PinotException


class PinotZk(object):

  def __init__(self, config, logger, fabric):
    self.config = config
    self.fabric = fabric
    self.logger = logger
    self.zk = None

  def get_handle(self):
    host = self.config.get_zk_host(self.fabric)

    if not self.zk:
      try:
        self.zk = KazooClient(hosts=host)
        self.zk.start()
      except kazoo.exceptions.KazooException:
        error = 'Failed connecting to zk  {0}'.format(host)
        self.logger.exception(error)
        raise PinotException(error)

    return self.zk

  def close(self):
    if self.zk:
      self.zk.stop()
      self.zk.close()
