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
