#!/usr/bin/env python2.6

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
