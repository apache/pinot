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

package org.apache.pinot.thirdeye.detection;

import java.util.Collections;
import java.util.List;
import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.anomaly.task.TaskInfo;
import org.apache.pinot.thirdeye.anomaly.task.TaskResult;
import org.apache.pinot.thirdeye.anomaly.task.TaskRunner;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalySubscriptionGroupNotificationManager;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalySubscriptionGroupNotificationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DetectionPipelineTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionPipelineTaskRunner.class);
  private final DetectionConfigManager detectionDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final EvaluationManager evaluationDAO;
  private final TaskManager taskDAO;
  private final AnomalySubscriptionGroupNotificationManager anomalySubscriptionGroupNotificationDAO;
  private final DetectionPipelineLoader loader;
  private final DataProvider provider;
  private final ModelMaintenanceFlow maintenanceFlow;

  private static final Long dummyDetectionId = 123456789L;

  /**
   * Default constructor for ThirdEye task execution framework.
   * Loads dependencies from DAORegitry and CacheRegistry
   *
   * @see DAORegistry
   * @see ThirdEyeCacheRegistry
   */
  public DetectionPipelineTaskRunner() {
    this.loader = new DetectionPipelineLoader();
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.evaluationDAO = DAORegistry.getInstance().getEvaluationManager();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.anomalySubscriptionGroupNotificationDAO =
        DAORegistry.getInstance().getAnomalySubscriptionGroupNotificationManager();
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    EventManager eventDAO = DAORegistry.getInstance().getEventDAO();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache(), ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, this.anomalyDAO, this.evaluationDAO,
        timeseriesLoader, aggregationLoader, this.loader, TimeSeriesCacheBuilder.getInstance(),
        AnomaliesCacheBuilder.getInstance());
    this.maintenanceFlow = new ModelRetuneFlow(this.provider, DetectionRegistry.getInstance());
  }

  /**
   * Alternate constructor for dependency injection.
   *
   * @param detectionDAO detection config DAO
   * @param anomalyDAO merged anomaly DAO
   * @param evaluationDAO the evaluation DAO
   * @param loader pipeline loader
   * @param provider pipeline data provider
   */
  public DetectionPipelineTaskRunner(DetectionConfigManager detectionDAO, MergedAnomalyResultManager anomalyDAO,
      EvaluationManager evaluationDAO, DetectionPipelineLoader loader, DataProvider provider) {
    this.detectionDAO = detectionDAO;
    this.anomalyDAO = anomalyDAO;
    this.evaluationDAO = evaluationDAO;
    this.loader = loader;
    this.provider = provider;
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.anomalySubscriptionGroupNotificationDAO =
        DAORegistry.getInstance().getAnomalySubscriptionGroupNotificationManager();
    this.maintenanceFlow = new ModelRetuneFlow(this.provider, DetectionRegistry.getInstance());
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    ThirdeyeMetricsUtil.detectionTaskCounter.inc();

    try {
      DetectionPipelineTaskInfo info = (DetectionPipelineTaskInfo) taskInfo;
      DetectionConfigDTO config;
      if (info.isOnline()) {
        config = taskDAO.extractDetectionConfig(info);
        // Online detection is not saved into DB so it does not have an ID
        // To prevent later on pipeline throws a false error for null ID, use a dummy id here
        config.setId(dummyDetectionId);
        Preconditions.checkNotNull(config,
            "Could not find detection config for online task info: " + info);
      } else {
        config = this.detectionDAO.findById(info.configId);

        if (config == null) {
          throw new IllegalArgumentException(String.format("Could not resolve config id %d", info.configId));
        }
      }

      LOG.info("Start detection for config {} between {} and {}", config.getId(), info.start, info.end);
      DetectionPipeline pipeline = this.loader.from(this.provider, config, info.start, info.end);
      DetectionPipelineResult result = pipeline.run();

      if (result.getLastTimestamp() < 0) {
        LOG.info("No detection ran for config {} between {} and {}", config.getId(), info.start, info.end);
        return Collections.emptyList();
      }

      config.setLastTimestamp(result.getLastTimestamp());

      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : result.getAnomalies()) {
        this.anomalyDAO.save(mergedAnomalyResultDTO);
        if (mergedAnomalyResultDTO.getId() == null) {
          LOG.warn("Could not store anomaly:\n{}", mergedAnomalyResultDTO);
        }
      }

      for (EvaluationDTO evaluationDTO : result.getEvaluations()) {
        this.evaluationDAO.save(evaluationDTO);
      }

      try {
        // run maintenance flow to update model
        config = maintenanceFlow.maintain(config, Instant.now());
      } catch (Exception e) {
        LOG.warn("Re-tune pipeline {} failed", config.getId(), e);
      }
      this.detectionDAO.update(config);

      // re-notify the anomalies if any
      for (MergedAnomalyResultDTO anomaly : result.getAnomalies()) {
        // if an anomaly should be re-notified, update the notification lookup table in the database
        if (anomaly.isRenotify()) {
          DetectionUtils.renotifyAnomaly(anomaly);
        }
      }

      ThirdeyeMetricsUtil.detectionTaskSuccessCounter.inc();
      LOG.info("End detection for config {} between {} and {}. Detected {} anomalies.", config.getId(), info.start,
          info.end, result.getAnomalies());
      return Collections.emptyList();
    } catch(Exception e) {
      ThirdeyeMetricsUtil.detectionTaskExceptionCounter.inc();
      throw e;
    }
  }
}
