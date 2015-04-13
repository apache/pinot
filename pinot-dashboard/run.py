#!/usr/bin/env python2.6

from werkzeug.serving import run_simple
from flask import Flask
from pinotui import pinotui, config
import logging
import os

if __name__ == '__main__':

  app = Flask(__name__, static_folder=None)
  app.register_blueprint(pinotui)

  log_path = os.path.join(os.path.dirname(__file__), 'logs', 'webui.log')
  logging.basicConfig(filename=log_path, level=logging.DEBUG)

  run_simple('0.0.0.0', config.get_flask_port(), app, threaded=True)
