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

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VerifySegmentState extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(VerifySegmentState.class);

  @Option(name = "-zkAddress", required = true, metaVar = "<http>", usage = "Zookeeper server:port/cluster")
  String _zkAddress = AbstractBaseCommand.DEFAULT_ZK_ADDRESS + "/pinot-cluster";

  @Option(name = "-clusterName", required = true, metaVar = "<String>", usage = "Helix cluster name")
  String _clusterName;

  @Option(name = "-tablePrefix", required = false, metaVar = "<String>",
      usage = "Table name prefix. (Ex: myTable, my or myTable_OFFLINE)")
  String _tablePrefix = "";

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute()
      throws Exception {
    ZKHelixAdmin helixAdmin = new ZKHelixAdmin(_zkAddress);
    List<String> resourcesInCluster = helixAdmin.getResourcesInCluster(_clusterName);

    for (String resourceName : resourcesInCluster) {
      // Skip non-table resources
      if (!TableNameBuilder.isTableResource(resourceName)) {
        continue;
      }

      if (resourceName.startsWith(_tablePrefix)) {
        IdealState resourceIdealState = helixAdmin.getResourceIdealState(_clusterName, resourceName);
        ExternalView resourceExternalView = helixAdmin.getResourceExternalView(_clusterName, resourceName);
        Map<String, Map<String, String>> mapFieldsFromIS = resourceIdealState.getRecord().getMapFields();
        Map<String, Map<String, String>> mapFieldsFromEV = resourceExternalView.getRecord().getMapFields();
        boolean error = false;
        if (mapFieldsFromIS.size() != mapFieldsFromEV.size()) {
          LOGGER.info("Table: {}, idealState size: {} does NOT match external view size: {}", resourceName,
              mapFieldsFromIS.size(), mapFieldsFromEV.size());
          error = true;
        }
        if (!mapFieldsFromIS.keySet().equals(mapFieldsFromEV.keySet())) {
          Set<String> idealStateKeys = mapFieldsFromIS.keySet();
          Set<String> externalViewKeys = mapFieldsFromEV.keySet();
          Sets.SetView<String> isToEVDiff = Sets.difference(idealStateKeys, externalViewKeys);
          for (String segmentName : isToEVDiff) {
            LOGGER.info("Segment: {} is missing in external view, ideal state: {}", segmentName,
                mapFieldsFromIS.get(segmentName));
          }

          Sets.SetView<String> evToISDiff = Sets.difference(externalViewKeys, idealStateKeys);
          for (String segmentName : evToISDiff) {
            LOGGER.error("Segment: {} is missing in ideal state, external view: {}", segmentName,
                mapFieldsFromEV.get(segmentName));
          }
          error = true;
        }

        for (Map.Entry<String, Map<String, String>> idealEntry : mapFieldsFromIS.entrySet()) {
          String segmentName = idealEntry.getKey();
          Map<String, String> segmentIdealState = idealEntry.getValue();
          // try to format consistently for tool based parsing
          if (!mapFieldsFromEV.containsKey(segmentName)) {
            LOGGER
                .info("Segment: {} idealstate: {} is MISSING in external view: {}", segmentName, segmentIdealState, "");
          }
          Map<String, String> segmentExternalView = mapFieldsFromEV.get(segmentName);

          if (!segmentIdealState.equals(segmentExternalView)) {
            LOGGER.info("Segment: {} idealstate: {} does NOT match external view: {}", segmentName, segmentIdealState,
                segmentExternalView);
            error = true;
          }
        }
        LOGGER.info(resourceName + " = " + (error ? "ERROR" : "OK"));
      }
    }
    return true;
  }

  @Override
  public String description() {
    return "Compares helix IdealState and ExternalView for specified table prefixes";
  }

  public static void main(String[] args)
      throws Exception {
    VerifySegmentState verifier = new VerifySegmentState();
    CmdLineParser cmdLineParser = new CmdLineParser(verifier);
    try {
      cmdLineParser.parseArgument(args);
    } catch (CmdLineException e) {
      LOGGER.error("Failed to read command line arguments: ", e);
      cmdLineParser.printUsage(System.err);
      System.exit(1);
    }
    verifier.execute();
  }
}
