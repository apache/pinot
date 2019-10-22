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

package org.apache.pinot.thirdeye.detection.datasla;

import com.google.common.collect.ArrayListMultimap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.anomaly.AnomalyType;
import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.anomaly.task.TaskInfo;
import org.apache.pinot.thirdeye.anomaly.task.TaskResult;
import org.apache.pinot.thirdeye.anomaly.task.TaskRunner;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.DetectionPipelineTaskInfo;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil.*;


/**
 * Runner that generates Data SLA anomalies. DATA_MISSING anomalies are created if
 * the data is not available for the sla detection window within the configured SLA.
 */
public class DatasetSlaTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetSlaTaskRunner.class);

  private final DetectionConfigManager detectionDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final EvaluationManager evaluationDAO;
  private DataProvider provider;

  private final String DEFAULT_DATA_SLA = "3_DAYS";

  public DatasetSlaTaskRunner() {
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

  public DatasetSlaTaskRunner(DetectionConfigManager detectionDAO, MergedAnomalyResultManager anomalyDAO,
      EvaluationManager evaluationDAO, DataProvider provider) {
    this.detectionDAO = detectionDAO;
    this.anomalyDAO = anomalyDAO;
    this.evaluationDAO = evaluationDAO;
    this.provider = provider;
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

      LOG.info("Check data sla for config {} between {} and {}", config.getId(), info.getStart(), info.getEnd());
      Map<String, Object> metricUrnToSlaMap = config.getDataSLAProperties();
      if (MapUtils.isNotEmpty(metricUrnToSlaMap)) {
        for (Map.Entry<String, Object> metricUrnToSlaMapEntry : metricUrnToSlaMap.entrySet()) {
          MetricEntity me = MetricEntity.fromURN(metricUrnToSlaMapEntry.getKey());
          MetricConfigDTO
              metricConfigDTO = this.provider.fetchMetrics(Collections.singletonList(me.getId())).get(me.getId());
          Map<String, DatasetConfigDTO> datasetToConfigMap = provider.fetchDatasets(Collections.singletonList(metricConfigDTO.getDataset()));
          for (Map.Entry<String, DatasetConfigDTO> datasetSLA : datasetToConfigMap.entrySet()) {
            try {
              runSLACheck(me, datasetSLA.getValue(), info, ConfigUtils.getMap(metricUrnToSlaMapEntry.getValue()));
            } catch (Exception e) {
              LOG.error("Failed to run sla check on metric URN %s", datasetSLA.getKey(), e);
            }
          }
        }
      }

      //LOG.info("End data availability for config {} between {} and {}. Detected {} anomalies.", config.getId(),
      //    info.getStart(), info.getEnd(), anomaliesList.size());
      return Collections.emptyList();
    } catch(Exception e) {
      throw e;
    }
  }

  /**
   * Runs the data sla check for the window (info) on the given metric (me) using the configured sla properties
   */
  private void runSLACheck(MetricEntity me, DatasetConfigDTO datasetConfig, DetectionPipelineTaskInfo info,
      Map<String, Object> slaProps) {
    if (me == null || datasetConfig == null || info == null) {
      //nothing to check
      return;
    }

    try {
      long datasetLastRefreshTime = datasetConfig.getLastRefreshTime();
      if (datasetLastRefreshTime <= 0) {
        // assume we have processed data till the current detection start
        datasetLastRefreshTime = info.getStart() - 1;
      }

      if (isMissingData(datasetLastRefreshTime, info)) {
        // Double check with data source as 2 things are possible.
        // 1. This dataset/source may not support availability events
        // 2. The data availability event pipeline has some issue.
        // We want to measure the overall dataset availability (filters are not taken into account)
        MetricSlice metricSlice = MetricSlice.from(me.getId(), info.getStart(), info.getEnd(), ArrayListMultimap.<String, String>create());
        DataFrame dataFrame = this.provider.fetchTimeseries(Collections.singleton(metricSlice)).get(metricSlice);
        if (dataFrame == null || dataFrame.isEmpty()) {
          // no data
          if (hasMissedSLA(datasetLastRefreshTime, slaProps, info.getEnd())) {
            createDataSLAAnomaly(datasetLastRefreshTime + 1, info.getEnd(), info.getConfigId(), datasetConfig);
          }
        } else {
          datasetLastRefreshTime = dataFrame.getDoubles("timestamp").max().longValue();
          if (isPartialData(datasetLastRefreshTime, info, datasetConfig)) {
            if (hasMissedSLA(datasetLastRefreshTime, slaProps, info.getEnd())) {
              createDataSLAAnomaly(datasetLastRefreshTime + 1, info.getEnd(), info.getConfigId(), datasetConfig);
            }
          }
        }
      } else if (isPartialData(datasetLastRefreshTime, info, datasetConfig)) {
        // Optimize for the common case - the common case is that the data availability events are arriving
        // correctly and we need not re-fetch the data to double check.
        if (hasMissedSLA(datasetLastRefreshTime, slaProps, info.getEnd())) {
          createDataSLAAnomaly(datasetLastRefreshTime + 1, info.getEnd(), info.getConfigId(), datasetConfig);
        }
      }
    } catch (Exception e) {
      LOG.error(String.format("Failed to run sla check on metric URN %s", me.getUrn()), e);
    }
  }

  /**
   * We say the data is missing we do not have data in the sla detection window.
   * Or more specifically if the dataset watermark is below the sla detection window.
   */
  private boolean isMissingData(long datasetLastRefreshTime, DetectionPipelineTaskInfo info) {
    return datasetLastRefreshTime < info.getStart();
  }

  /**
   * We say the data is partial if we do not have all the data points in the sla detection window.
   * Or more specifically if we have at least 1 data-point missing in the sla detection window.
   *
   * For example:
   * Assume that the data is delayed and our current sla detection window is [1st Feb to 3rd Feb). During this scan,
   * let's say data for 1st Feb arrives. Now, we have a situation where partial data is present. In other words, in the
   * current sla detection window [1st to 3rd Feb) we have data for 1st Feb but data for 2nd Feb is missing. This is the
   * partial data scenario.
   */
  private boolean isPartialData(long datasetLastRefreshTime, DetectionPipelineTaskInfo info, DatasetConfigDTO datasetConfig) {
    long granularity = datasetConfig.bucketTimeGranularity().toMillis();
    return (info.getEnd() - datasetLastRefreshTime) * 1.0 / granularity > 1;
  }

  /**
   * Validates if the data is delayed or not based on the user specified SLA configuration
   */
  private boolean hasMissedSLA(long datasetLastRefreshTime, Map<String, Object> slaProps, long slaDetectionEndTime) {
    // fetch the user configured SLA, otherwise default 3_DAYS.
    long delay = TimeGranularity.fromString(MapUtils.getString(slaProps,  "sla", DEFAULT_DATA_SLA))
        .toPeriod().toStandardDuration().getMillis();

    return (slaDetectionEndTime - datasetLastRefreshTime) >= delay;
  }

  /**
   * Align and round off start time to the upper boundary of the granularity
   */
  private static long alignToUpperBoundary(long start, DatasetConfigDTO datasetConfig) {
    Period granularityPeriod = datasetConfig.bucketTimeGranularity().toPeriod();
    DateTimeZone timezone = DateTimeZone.forID(datasetConfig.getTimezone());
    DateTime startTime = new DateTime(start - 1, timezone).plus(granularityPeriod);
    return startTime.getMillis() / granularityPeriod.toStandardDuration().getMillis() * granularityPeriod.toStandardDuration().getMillis();
  }

  /**
   * Merges one DATA_MISSING anomaly with remaining existing anomalies.
   */
  private void mergeSLAAnomalies(MergedAnomalyResultDTO anomaly, List<MergedAnomalyResultDTO> existingAnomalies) {
    // Extract the parent SLA anomaly. We can have only 1 parent DATA_MISSING anomaly in a window.
    existingAnomalies.removeIf(MergedAnomalyResultBean::isChild);
    MergedAnomalyResultDTO existingParentSLAAnomaly = existingAnomalies.get(0);

    if (isDuplicateSLAAnomaly(existingParentSLAAnomaly, anomaly)) {
      // Ensure anomalies are not duplicated. Ignore and return.
      // Example: daily data with hourly cron should generate only 1 sla alert if data is missing
      return;
    }
    existingParentSLAAnomaly.setChild(true);
    anomaly.setChild(false);
    anomaly.setChildren(Collections.singleton(existingParentSLAAnomaly));
    this.anomalyDAO.save(anomaly);
    if (anomaly.getId() == null) {
      LOG.warn("Could not store data sla check failed anomaly:\n{}", anomaly);
    }
  }

  /**
   * Creates a DATA_MISSING anomaly from ceiling(start) to ceiling(end) for the detection id.
   * If existing DATA_MISSING anomalies are present, then it will be merged accordingly.
   */
  private void createDataSLAAnomaly(long start, long end, long detectionId, DatasetConfigDTO datasetConfig) {
    MergedAnomalyResultDTO anomaly = DetectionUtils.makeAnomaly(
        alignToUpperBoundary(start, datasetConfig),
        alignToUpperBoundary(end, datasetConfig),
        detectionId);
    anomaly.setType(AnomalyType.DATA_MISSING);

    List<MergedAnomalyResultDTO> existingAnomalies = anomalyDAO.findAnomaliesWithinBoundary(start, end, detectionId);
    if (!existingAnomalies.isEmpty()) {
      mergeSLAAnomalies(anomaly, existingAnomalies);
    } else {
      // no merging required
      this.anomalyDAO.save(anomaly);
      if (anomaly.getId() == null) {
        LOG.warn("Could not store data sla check failed anomaly:\n{}", anomaly);
      }
    }
  }

  /**
   * We say one DATA_MISSING anomaly is a duplicate of another if they belong to the same detection with the same type
   * and span the exact same duration.
   */
  private boolean isDuplicateSLAAnomaly(MergedAnomalyResultDTO anomaly1, MergedAnomalyResultDTO anomaly2) {
    if (anomaly1 == null && anomaly2 == null) {
      return true;
    }

    if (anomaly1 == null || anomaly2 == null) {
      return false;
    }

    // anomalies belong to the same detection, same type & span over the same duration
    return anomaly1.getDetectionConfigId().equals(anomaly2.getDetectionConfigId())
        && anomaly1.getType() == anomaly2.getType()
        && anomaly1.getStartTime() == anomaly2.getStartTime()
        && anomaly1.getEndTime() == anomaly2.getEndTime();
  }
}