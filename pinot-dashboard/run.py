#!/usr/bin/env python2.6

from werkzeug.serving import run_simple
from app import app, config
import logging
import os

if __name__ == '__main__':

  log_path = os.path.join(os.path.dirname(__file__), 'logs', 'webui.log')
  logging.basicConfig(filename=log_path, level=logging.DEBUG)

  run_simple('0.0.0.0', config.get_flask_port(), app, threaded=True)
