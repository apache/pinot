package org.apache.pinot.controller.helix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealtimeConsumerMonitor extends ControllerPeriodicTask<RealtimeConsumerMonitor.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeConsumerMonitor.class);
  private final ConsumingSegmentInfoReader _consumingSegmentInfoReader;

  public RealtimeConsumerMonitor(ControllerConf controllerConfig, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerMetrics controllerMetrics,
      ExecutorService executorService) {
    super("RealtimeConsumerMonitor", controllerConfig.getRealtimeConsumerMonitorRunFrequency(),
        controllerConfig.getRealtimeConsumerMonitorInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);
    _consumingSegmentInfoReader = new ConsumingSegmentInfoReader(executorService, new SimpleHttpConnectionManager(),
        pinotHelixResourceManager);
  }

  @Override
  protected void setUpTask() {
    LOGGER.info("Setting up RealtimeConsumerMonitor task");
  }

  @Override
  protected void processTable(String tableNameWithType) {
    try {
      ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap segmentsInfoMap =
          _consumingSegmentInfoReader.getConsumingSegmentsInfo(tableNameWithType, 10000);
      Map<String, List<Long>> partitionToLagSet = new HashMap<>();
      for (List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> info :
          segmentsInfoMap._segmentToConsumingInfoMap.values()) {
        info.forEach(segment -> {
          segment._partitionOffsetInfo._recordsLagMap.forEach((k, v) -> {
            if (!PartitionLagState.NOT_CALCULATED.equals(v)) {
              try {
                long recordsLag = Long.parseLong(v);
                partitionToLagSet.computeIfAbsent(k, k1 -> new ArrayList<>());
                partitionToLagSet.get(k).add(recordsLag);
              } catch (NumberFormatException nfe) {
                // skip this as we are unable to parse the lag string
              }
            }
          });
        });
      }
      partitionToLagSet.forEach((partition, lagSet) -> {
        System.out.println("Info - "
            + _controllerMetrics.getValueOfGlobalGauge(ControllerGauge.MAX_CONSUMPTION_RECORDS_LAG) + " - "
            + Arrays.toString(lagSet.toArray()));
        _controllerMetrics.setValueOfPartitionGauge(tableNameWithType, Integer.parseInt(partition),
            ControllerGauge.MAX_CONSUMPTION_RECORDS_LAG, Collections.max(lagSet));
      });
    } catch (Exception e) {
      LOGGER.error("Failed to fetch consuming segments info. Unable to update table consumption status metrics");
    }
  }

  public static final class Context {

  }
}
