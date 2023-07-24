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
package org.apache.pinot.controller.helix;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealtimeConsumerMonitor extends ControllerPeriodicTask<RealtimeConsumerMonitor.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeConsumerMonitor.class);
  private static final int DEFAULT_TIMEOUT_MS = 10000;
  private final ConsumingSegmentInfoReader _consumingSegmentInfoReader;

  @VisibleForTesting
  public RealtimeConsumerMonitor(ControllerConf controllerConf, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerMetrics controllerMetrics,
      ConsumingSegmentInfoReader consumingSegmentInfoReader) {
    super("RealtimeConsumerMonitor", controllerConf.getRealtimeConsumerMonitorRunFrequency(),
        controllerConf.getRealtimeConsumerMonitorInitialDelayInSeconds(), pinotHelixResourceManager,
        leadControllerManager, controllerMetrics);
    _consumingSegmentInfoReader = consumingSegmentInfoReader;
  }

  public RealtimeConsumerMonitor(ControllerConf controllerConf, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerMetrics controllerMetrics,
      ExecutorService executorService) {
    this(controllerConf, pinotHelixResourceManager, leadControllerManager, controllerMetrics,
        new ConsumingSegmentInfoReader(executorService, new BasicHttpClientConnectionManager(), pinotHelixResourceManager));
  }

  @Override
  protected void setUpTask() {
    LOGGER.info("Setting up RealtimeConsumerMonitor task");
  }

  @Override
  protected void processTable(String tableNameWithType) {
    if (!TableType.REALTIME.equals(TableNameBuilder.getTableTypeFromTableName(tableNameWithType))) {
      return;
    }
    try {
      ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap segmentsInfoMap =
          _consumingSegmentInfoReader.getConsumingSegmentsInfo(tableNameWithType, DEFAULT_TIMEOUT_MS);
      Map<String, List<Long>> partitionToLagSet = new HashMap<>();
      Map<String, List<Long>> partitionToAvailabilityLagSet = new HashMap<>();

      for (List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> info
          : segmentsInfoMap._segmentToConsumingInfoMap.values()) {
        info.forEach(segment -> {
          segment._partitionOffsetInfo._recordsLagMap.forEach((k, v) -> {
            if (!PartitionLagState.NOT_CALCULATED.equals(v)) {
              try {
                long recordsLag = Long.parseLong(v);
                partitionToLagSet.computeIfAbsent(k, k1 -> new ArrayList<>()).add(recordsLag);
              } catch (NumberFormatException nfe) {
                // skip this as we are unable to parse the lag string
              }
            }
          });
          segment._partitionOffsetInfo._availabilityLagMap.forEach((k, v) -> {
            if (!PartitionLagState.NOT_CALCULATED.equals(v)) {
              try {
                long availabilityLagMs = Long.parseLong(v);
                partitionToAvailabilityLagSet.computeIfAbsent(k, k1 -> new ArrayList<>()).add(availabilityLagMs);
              } catch (NumberFormatException nfe) {
                // skip this as we are unable to parse the lag string
              }
            }
          });
        });
      }
      partitionToLagSet.forEach((partition, lagSet) -> {
        _controllerMetrics.setValueOfPartitionGauge(tableNameWithType, Integer.parseInt(partition),
            ControllerGauge.MAX_RECORDS_LAG, Collections.max(lagSet));
      });

      partitionToAvailabilityLagSet.forEach((partition, lagSet) -> {
        _controllerMetrics.setValueOfPartitionGauge(tableNameWithType, Integer.parseInt(partition),
            ControllerGauge.MAX_RECORD_AVAILABILITY_LAG_MS, Collections.max(lagSet));
      });
    } catch (Exception e) {
      LOGGER.error("Failed to fetch consuming segments info. Unable to update table consumption status metrics");
    }
  }

  public static final class Context { }
}
