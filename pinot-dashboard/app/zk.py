#!usr/bin/env python2.6

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
