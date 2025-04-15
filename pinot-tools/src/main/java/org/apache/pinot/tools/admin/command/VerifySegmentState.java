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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "VerifySegmentState", mixinStandardHelpOptions = true)
public class VerifySegmentState extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(VerifySegmentState.class);

  @CommandLine.Option(names = {"-zkAddress"}, required = true, description = "Zookeeper server:port/cluster")
  String _zkAddress = AbstractBaseCommand.DEFAULT_ZK_ADDRESS + "/pinot-cluster";

  @CommandLine.Option(names = {"-clusterName"}, required = true, description = "Helix cluster name")
  String _clusterName;

  @CommandLine.Option(names = {"-tablePrefix"}, required = false,
      description = "Table name prefix. (Ex: myTable, my or myTable_OFFLINE)")
  String _tablePrefix = "";

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
        // EV may be null for a given segment if all instances of that segment are OFFLINE in IS. This is valid, so
        // detect this scenario and update the EV size and segment list accordingly to include this case
        int evSize = mapFieldsFromEV.size();
        Set<String> evSegments = new HashSet<>(mapFieldsFromEV.keySet());
        Set<String> evEmptySegmentsWithISOffline = new HashSet<>();
        for (Map.Entry<String, Map<String, String>> idealEntry : mapFieldsFromIS.entrySet()) {
          String segmentName = idealEntry.getKey();
          if (!mapFieldsFromEV.containsKey(segmentName)) {
            boolean isISAllOffline = true;
            for (Map.Entry<String, String> instanceState : idealEntry.getValue().entrySet()) {
              if (!CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE.equals(instanceState.getValue())) {
                LOGGER.info("Table: {}, idealState has non-OFFLINE state: {} for instance: {} whereas EV is null for "
                        + "given segment: {}", resourceName, instanceState.getValue(), instanceState.getKey(),
                    segmentName);
                isISAllOffline = false;
                error = true;
                break;
              }
            }
            if (isISAllOffline) {
              // We count this as a valid EV since EV can be null if all segments in IS are OFFLINE
              evSize++;
              evEmptySegmentsWithISOffline.add(segmentName);
              evSegments.add(segmentName);
            }
          }
        }

        if (mapFieldsFromIS.size() != evSize) {
          LOGGER.info("Table: {}, idealState size: {} does NOT match calculated external view size: {}, actual "
                  + "external view size: {}", resourceName, mapFieldsFromIS.size(), evSize, mapFieldsFromEV.size());
          error = true;
        }
        if (!mapFieldsFromIS.keySet().equals(evSegments)) {
          Set<String> idealStateKeys = mapFieldsFromIS.keySet();
          Sets.SetView<String> isToEVDiff = Sets.difference(idealStateKeys, evSegments);
          for (String segmentName : isToEVDiff) {
            LOGGER.info("Segment: {} is missing in external view, ideal state: {}", segmentName,
                mapFieldsFromIS.get(segmentName));
          }

          Sets.SetView<String> evToISDiff = Sets.difference(evSegments, idealStateKeys);
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
                .info("Segment: {} idealstate: {} is MISSING in external view: {}, isISAllOffline: {}", segmentName,
                    segmentIdealState, "", evEmptySegmentsWithISOffline.contains(segmentName));
          }
          Map<String, String> segmentExternalView = mapFieldsFromEV.get(segmentName);

          if (!segmentIdealState.equals(segmentExternalView) && !evEmptySegmentsWithISOffline.contains(segmentName)) {
            LOGGER.info("Segment: {} idealstate: {} does NOT match external view: {}, checking for empty EV for "
                    + "OFFLINE IS", segmentName, segmentIdealState, segmentExternalView);
            for (Map.Entry<String, String> instanceToState : segmentIdealState.entrySet()) {
              String instanceName = instanceToState.getKey();
              String isState = instanceToState.getValue();
              String evState = segmentExternalView == null ? null : segmentExternalView.get(instanceName);
              if (evState == null && CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE.equals(isState)) {
                LOGGER.info("Segment: {} ideal state: {} for instance: {} is OFFLINE, and external view is null, "
                    + "this is valid", segmentName, isState, instanceName);
              } else if (evState == null || !evState.equals(isState)) {
                LOGGER.info("Segment: {} ideal state: {} for instance: {} does NOT match external view: {}",
                    segmentName, isState, instanceName, evState);
                error = true;
              }
            }
          }
        }
        LOGGER.info("{} = {}", resourceName, error ? "ERROR" : "OK");
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
    CommandLine commandLine = new CommandLine(verifier);
    try {
      commandLine.execute(args);
    } catch (Exception e) {
      LOGGER.error("Failed to parse/execute", e);
      System.exit(1);
    }
  }
}
