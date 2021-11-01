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
import org.apache.pinot.tools.JoinQuickStart;
import org.apache.pinot.tools.JsonIndexQuickStart;
import org.apache.pinot.tools.OfflineComplexTypeHandlingQuickStart;
import org.apache.pinot.tools.QuickStartBase;
import org.apache.pinot.tools.Quickstart;
import org.apache.pinot.tools.RealtimeComplexTypeHandlingQuickStart;
import org.apache.pinot.tools.RealtimeJsonIndexQuickStart;
import org.apache.pinot.tools.RealtimeQuickStart;
import org.apache.pinot.tools.RealtimeQuickStartWithMinion;
import org.apache.pinot.tools.UpsertQuickStart;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QuickStartCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuickStartCommand.class.getName());

  @Option(name = "-type", required = false, metaVar = "<String>",
      usage = "Type of quickstart, supported: STREAM/BATCH/HYBRID")
  private String _type;

  @Option(name = "-tmpDir", required = false, aliases = {"-quickstartDir", "-dataDir"}, metaVar = "<String>",
      usage = "Temp Directory to host quickstart data")
  private String _tmpDir;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
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

  public String getTmpDir() {
    return _tmpDir;
  }

  public void setTmpDir(String tmpDir) {
    _tmpDir = tmpDir;
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
    QuickStartBase quickstart;
    switch (_type.toUpperCase()) {
      case "OFFLINE":
      case "BATCH":
        quickstart = new Quickstart();
        break;
      case "OFFLINE_MINION":
      case "BATCH_MINION":
      case "OFFLINE-MINION":
      case "BATCH-MINION":
        quickstart = new BatchQuickstartWithMinion();
        break;
      case "REALTIME-MINION":
      case "REALTIME_MINION":
        quickstart = new RealtimeQuickStartWithMinion();
        break;
      case "REALTIME":
      case "STREAM":
        quickstart = new RealtimeQuickStart();
        break;
      case "HYBRID":
        quickstart = new HybridQuickstart();
        break;
      case "JOIN":
        quickstart = new JoinQuickStart();
        break;
      case "UPSERT":
        quickstart = new UpsertQuickStart();
        break;
      case "OFFLINE_JSON_INDEX":
      case "OFFLINE-JSON-INDEX":
      case "BATCH_JSON_INDEX":
      case "BATCH-JSON-INDEX":
        quickstart = new JsonIndexQuickStart();
        break;
      case "REALTIME_JSON_INDEX":
      case "REALTIME-JSON-INDEX":
      case "STREAM_JSON_INDEX":
      case "STREAM-JSON-INDEX":
        quickstart = new RealtimeJsonIndexQuickStart();
        break;
      case "OFFLINE_COMPLEX_TYPE":
      case "OFFLINE-COMPLEX-TYPE":
      case "BATCH_COMPLEX_TYPE":
      case "BATCH-COMPLEX-TYPE":
        quickstart = new OfflineComplexTypeHandlingQuickStart();
        break;
      case "REALTIME_COMPLEX_TYPE":
      case "REALTIME-COMPLEX-TYPE":
      case "STREAM_COMPLEX_TYPE":
      case "STREAM-COMPLEX-TYPE":
        quickstart = new RealtimeComplexTypeHandlingQuickStart();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported QuickStart type: " + _type);
    }
    if (_tmpDir != null) {
      quickstart.setTmpDir(_tmpDir);
    }
    quickstart.execute();
    return true;
  }
}
