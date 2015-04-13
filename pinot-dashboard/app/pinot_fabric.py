#!/usr/bin/env python2.6

import requests
from exceptions import PinotException
from requests.exceptions import RequestException


class PinotFabric(object):

  def __init__(self, config, logger, fabric):
    self.config = config
    self.fabric = fabric
    self.logger = logger

  def get_resources(self):
    host = self.config.get_controller_url(self.fabric)
    url = '{0}/dataresources/'.format(host)
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

    return filter(lambda x: x != 'brokerResource', data['resources'])

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
