/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.admin.command;

import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.tools.BatchQuickstartWithMinion;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.HybridQuickstart;
import org.apache.pinot.tools.Quickstart;
import org.apache.pinot.tools.RealtimeQuickStart;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QuickStartCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuickStartCommand.class.getName());

  @Option(name = "-type", required = false, metaVar = "<String>", usage = "Type of quickstart, supported: STREAM/BATCH/HYBRID")
  private String _type;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "QuickStart";
  }

  public QuickStartCommand setType(String type) {
    _type = type;
    return this;
  }

  @Override
  public String toString() {
    return ("QuickStart -type " + _type);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Run Pinot QuickStart.";
  }

  @Override
  public boolean execute()
      throws Exception {
    PluginManager.get().init();
    switch (_type.toUpperCase()) {
      case "OFFLINE":
      case "BATCH":
        new Quickstart().execute();
        break;
      case "OFFLINE_MINION":
      case "BATCH_MINION":
      case "OFFLINE-MINION":
      case "BATCH-MINION":
        new BatchQuickstartWithMinion().execute();
        break;
      case "REALTIME":
      case "STREAM":
        new RealtimeQuickStart().execute();
        break;
      case "HYBRID":
        new HybridQuickstart().execute();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported QuickStart type: " + _type);
    }
    return true;
  }
}
