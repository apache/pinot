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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.util.SegmentIntervalUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Pinot admin command to list all offline segments with invalid intervals, group by table name
 */
@CommandLine.Command(name = "OfflineSegmentIntervalChecker")
public class OfflineSegmentIntervalCheckerCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(OfflineSegmentIntervalCheckerCommand.class);

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @CommandLine.Option(names = {"-zkAddress"}, required = true, description = "Zookeeper server:port/cluster")
  private String _zkAddress;

  @CommandLine.Option(names = {"-clusterName"}, required = true, description = "Helix cluster name")
  private String _clusterName;

  @CommandLine.Option(names = {"-tableNames"},
      description = "Comma separated list of tables to check for invalid segment intervals")
  private String _tableNames;

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, help = true, description = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String toString() {
    return "OfflineSegmentIntervalChecker";
  }

  @Override
  public String getName() {
    return "OfflineSegmentIntervalChecker";
  }

  @Override
  public void cleanup() {
  }

  @Override
  public String description() {
    return "Prints out offline segments with invalid time intervals";
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Executing command: " + toString());

    ZKHelixAdmin helixAdmin = new ZKHelixAdmin.Builder().setZkAddress(_zkAddress).build();
    _propertyStore = new ZkHelixPropertyStore<>(_zkAddress, new ZNRecordSerializer(),
        PropertyPathBuilder.propertyStore(_clusterName));

    List<String> offlineTables = new ArrayList<>();
    if (StringUtils.isBlank(_tableNames)) {
      List<String> resourcesInCluster = helixAdmin.getResourcesInCluster(_clusterName);
      for (String tableName : resourcesInCluster) {
        if (TableNameBuilder.isOfflineTableResource(tableName)) {
          offlineTables.add(tableName);
        }
      }
    } else {
      for (String tableName : _tableNames.split(",")) {
        if (ZKMetadataProvider.getOfflineTableConfig(_propertyStore, tableName) != null) {
          offlineTables.add(tableName);
        } else {
          LOGGER.warn("Table config not found for table {}. Skipping", tableName);
        }
      }
    }

    LOGGER.info("Tables to check: {}", offlineTables);
    for (String offlineTableName : offlineTables) {
      LOGGER.info("Checking table {}", offlineTableName);
      List<String> segmentsWithInvalidIntervals = checkOfflineTablesSegmentIntervals(offlineTableName);
      if (CollectionUtils.isNotEmpty(segmentsWithInvalidIntervals)) {
        LOGGER.info("Table: {} has {} segments with invalid interval: {}", offlineTableName,
            segmentsWithInvalidIntervals.size(), segmentsWithInvalidIntervals);
      }
    }

    return true;
  }

  /**
   * Checks segments of table for invalid intervals and prints them out
   * @param offlineTableName
   */
  private List<String> checkOfflineTablesSegmentIntervals(String offlineTableName) {
    TableConfig tableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, offlineTableName);
    List<SegmentZKMetadata> segmentsZKMetadata =
        ZKMetadataProvider.getSegmentsZKMetadata(_propertyStore, offlineTableName);

    // Collect segments with invalid start/end time
    List<String> segmentsWithInvalidIntervals = new ArrayList<>();
    if (SegmentIntervalUtils.eligibleForSegmentIntervalCheck(tableConfig.getValidationConfig())) {
      for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
        long startTimeMs = segmentZKMetadata.getStartTimeMs();
        long endTimeMs = segmentZKMetadata.getEndTimeMs();
        if (!TimeUtils.timeValueInValidRange(startTimeMs) || !TimeUtils.timeValueInValidRange(endTimeMs)) {
          segmentsWithInvalidIntervals.add(segmentZKMetadata.getSegmentName());
        }
      }
    }
    return segmentsWithInvalidIntervals;
  }
}
