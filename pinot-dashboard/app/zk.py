#!/usr/bin/env python2.6

from kazoo.client import KazooClient


class PinotZk(object):

  def __init__(self, config, fabric):
    self.config = config
    self.fabric = fabric
    self.zk = None

  def get_handle(self):
    if not self.zk:
      host = self.config.get_zk_host(self.fabric)
      self.zk = KazooClient(hosts=host)
      self.zk.start()
    return self.zk

  def close(self):
    if self.zk:
      self.zk.stop()
      self.zk.close()
