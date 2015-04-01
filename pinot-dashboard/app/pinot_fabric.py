#!/usr/bin/env python2.6

import requests


class PinotFabric(object):

  def __init__(self, config, fabric):
    self.config = config
    self.fabric = fabric

  def get_resources(self):
    host = self.config.get_controller_url(self.fabric)
    r = requests.get('{0}/dataresources/'.format(host))
    data = r.json()
    return filter(lambda x: x != 'brokerResource', data['resources'])

  def run_pql(self, pql):
    host = self.config.get_controller_url(self.fabric)
    r = requests.get('{0}/pql'.format(host), params={'pql': pql})
    success = True
    try:
      result = r.json()
    except ValueError:
      result = r.text
      success = False
    return {
      'success': success,
      'result': result
    }
