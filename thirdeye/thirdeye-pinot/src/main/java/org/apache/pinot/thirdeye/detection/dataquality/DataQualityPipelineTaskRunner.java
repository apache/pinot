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

package org.apache.pinot.thirdeye.detection.dataquality;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.anomaly.task.TaskInfo;
import org.apache.pinot.thirdeye.anomaly.task.TaskResult;
import org.apache.pinot.thirdeye.anomaly.task.TaskRunner;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionPipelineTaskInfo;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataQualityPipelineTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DataQualityPipelineTaskRunner.class);
  private final DetectionConfigManager detectionDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final EvaluationManager evaluationDAO;
  private final DetectionPipelineLoader loader;
  private final DataProvider provider;

  /**
   * Default constructor for ThirdEye task execution framework.
   * Loads dependencies from DAORegitry and CacheRegistry
   *
   * @see DAORegistry
   * @see ThirdEyeCacheRegistry
   */
  public DataQualityPipelineTaskRunner() {
    this.loader = new DetectionPipelineLoader();
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.evaluationDAO = DAORegistry.getInstance().getEvaluationManager();
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
        timeseriesLoader, aggregationLoader, new DetectionPipelineLoader(), TimeSeriesCacheBuilder.getInstance(),
        AnomaliesCacheBuilder.getInstance());
  }

  public DataQualityPipelineTaskRunner(DetectionConfigManager detectionDAO, MergedAnomalyResultManager anomalyDAO,
      EvaluationManager evaluationDAO, DetectionPipelineLoader loader, DataProvider provider) {
    this.detectionDAO = detectionDAO;
    this.anomalyDAO = anomalyDAO;
    this.evaluationDAO = evaluationDAO;
    this.loader = loader;
    this.provider = provider;
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    //ThirdeyeMetricsUtil.detectionTaskCounter.inc();

    try {
      DetectionPipelineTaskInfo info = (DetectionPipelineTaskInfo) taskInfo;

      DetectionConfigDTO config = this.detectionDAO.findById(info.getConfigId());
      if (config == null) {
        throw new IllegalArgumentException(String.format("Could not resolve config id %d", info.getConfigId()));
      }

      LOG.info("Start sla check for config {} between {} and {}", config.getId(), info.getStart(), info.getEnd());
      Map<String, Object> props =  config.getProperties();
      config.setProperties(config.getDataQualityProperties());
      DetectionPipeline pipeline = this.loader.from(this.provider, config, info.getStart(), info.getEnd());
      DetectionPipelineResult result = pipeline.run();
      config.setProperties(props);

      if (result.getLastTimestamp() < 0) {
        LOG.info("No sla check ran for config {} between {} and {}", config.getId(), info.getStart(), info.getEnd());
        return Collections.emptyList();
      }

      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : result.getAnomalies()) {
        this.anomalyDAO.save(mergedAnomalyResultDTO);
        if (mergedAnomalyResultDTO.getId() == null) {
          LOG.warn("Could not store anomaly:\n{}", mergedAnomalyResultDTO);
        }
      }

      //ThirdeyeMetricsUtil.detectionTaskSuccessCounter.inc();
      LOG.info("End sla check for config {} between {} and {}. Detected {} anomalies.", config.getId(), info.getStart(),
          info.getEnd(), result.getAnomalies());
      return Collections.emptyList();
    } catch(Exception e) {
      //ThirdeyeMetricsUtil.detectionTaskExceptionCounter.inc();
      throw e;
    }
  }
}
