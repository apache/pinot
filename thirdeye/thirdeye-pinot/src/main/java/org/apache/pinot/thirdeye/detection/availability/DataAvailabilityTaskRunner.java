/*
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

package org.apache.pinot.thirdeye.detection.availability;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.anomaly.task.TaskInfo;
import org.apache.pinot.thirdeye.anomaly.task.TaskResult;
import org.apache.pinot.thirdeye.anomaly.task.TaskRunner;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.detection.DetectionPipelineTaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil.*;


public class DataAvailabilityTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DataAvailabilityTaskRunner.class);
  private final DetectionConfigManager detectionDAO;

  /**
   * Default constructor for ThirdEye task execution framework.
   * Loads dependencies from DAORegitry and CacheRegistry
   *
   * @see DAORegistry
   * @see ThirdEyeCacheRegistry
   */
  public DataAvailabilityTaskRunner() {
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    dataAvailabilityTaskCounter.inc();

    try {
      DetectionPipelineTaskInfo info = (DetectionPipelineTaskInfo) taskInfo;

      DetectionConfigDTO config = this.detectionDAO.findById(info.getConfigId());
      if (config == null) {
        throw new IllegalArgumentException(String.format("Could not resolve config id %d", info.getConfigId()));
      }

      LOG.info("Check data availability for config {} between {} and {}", config.getId(), info.getStart(), info.getEnd());

      // TODO: Add logic for data/sla availability

      // 0. Get a list of all the datasets & SLA settings being monitored by this detection config.
      //    For each dataset config,
      // 1. Get the refresh timestamp(rfts) from datasetconfig
      // 2. If rfts >= start, data is available -> 0 anomalies
      // 3. Else, -> fetch latest timestamp from data source (ltds)
      // 4. If ltds >= start, data is available -> 0 anomalies
      // 5. Else -> create new da anomaly

      LOG.info("End data availability for config {} between {} and {}. Detected {} anomalies.", config.getId(),
          info.getStart(), info.getEnd(), 0);
      return Collections.emptyList();
    } catch(Exception e) {
      throw e;
    }
  }
}