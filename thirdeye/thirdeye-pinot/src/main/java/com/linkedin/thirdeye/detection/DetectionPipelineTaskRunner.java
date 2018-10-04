/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.loader.AggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultAggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import com.linkedin.thirdeye.datasource.loader.TimeSeriesLoader;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DetectionPipelineTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionPipelineTaskRunner.class);
  private final DetectionConfigManager detectionDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final DetectionPipelineLoader loader;
  private final DataProvider provider;

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

    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    EventManager eventDAO = DAORegistry.getInstance().getEventDAO();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, this.anomalyDAO,
        timeseriesLoader, aggregationLoader, this.loader);
  }

  /**
   * Alternate constructor for dependency injection.
   *
   * @param detectionDAO detection config DAO
   * @param anomalyDAO merged anomaly DAO
   * @param loader pipeline loader
   * @param provider pipeline data provider
   */
  public DetectionPipelineTaskRunner(DetectionConfigManager detectionDAO, MergedAnomalyResultManager anomalyDAO,
      DetectionPipelineLoader loader, DataProvider provider) {
    this.detectionDAO = detectionDAO;
    this.anomalyDAO = anomalyDAO;
    this.loader = loader;
    this.provider = provider;
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    ThirdeyeMetricsUtil.detectionTaskCounter.inc();

    try {
      DetectionPipelineTaskInfo info = (DetectionPipelineTaskInfo) taskInfo;

      DetectionConfigDTO config = this.detectionDAO.findById(info.configId);
      if (config == null) {
        throw new IllegalArgumentException(String.format("Could not resolve config id %d", info.configId));
      }

      DetectionPipeline pipeline = this.loader.from(this.provider, config, info.start, info.end);
      DetectionPipelineResult result = pipeline.run();

      if (result.getLastTimestamp() < 0) {
        return Collections.emptyList();
      }

      config.setLastTimestamp(result.getLastTimestamp());
      this.detectionDAO.update(config);

      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : result.getAnomalies()) {
        this.anomalyDAO.save(mergedAnomalyResultDTO);
        if (mergedAnomalyResultDTO.getId() == null) {
          LOG.warn("Could not store anomaly:\n{}", mergedAnomalyResultDTO);
        }
      }

      return Collections.emptyList();

    } catch(Exception e) {
      ThirdeyeMetricsUtil.detectionTaskExceptionCounter.inc();
      throw e;

    } finally {
      ThirdeyeMetricsUtil.detectionTaskSuccessCounter.inc();
    }
  }
}
