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

package org.apache.pinot.thirdeye.detection.dataquality.components;

import com.google.common.collect.ArrayListMultimap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.anomaly.AnomalyType;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.annotation.Param;
import org.apache.pinot.thirdeye.detection.annotation.PresentationOption;
import org.apache.pinot.thirdeye.detection.dataquality.spec.DataSlaQualityCheckerSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionResult;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generates Data SLA anomalies. DATA_MISSING anomalies are created if
 * the data is not available for the sla detection window within the configured SLA.
 */
@Components(title = "DataSla", type = "DATA_SLA", tags = {
    DetectionTag.RULE_DETECTION}, description = "", presentation = {
    @PresentationOption(name = "absolute value", template = "is lower than ${min} or higher than ${max}")}, params = {
    @Param(name = "min", placeholder = "value"), @Param(name = "max", placeholder = "value")})
public class DataSlaQualityChecker implements AnomalyDetector<DataSlaQualityCheckerSpec>, BaselineProvider<DataSlaQualityCheckerSpec> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSlaQualityChecker.class);

  private String sla;
  private InputDataFetcher dataFetcher;
  private final String DEFAULT_DATA_SLA = "3_DAYS";

  @Override
  public DetectionResult runDetection(Interval window, String metricUrn) {
    return DetectionResult.from(runSLACheck(MetricEntity.fromURN(metricUrn), window));
  }

  @Override
  public TimeSeries computePredictedTimeSeries(MetricSlice slice) {
    return TimeSeries.empty();
  }

  @Override
  public void init(DataSlaQualityCheckerSpec spec, InputDataFetcher dataFetcher) {
    this.sla = spec.getSla();
    this.dataFetcher = dataFetcher;
  }

  /**
   * Runs the data sla check for the window on the given metric
   */
  private List<MergedAnomalyResultDTO> runSLACheck(MetricEntity me, Interval window) {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();

    // We want to measure the overall dataset availability (filters are ignored)
    long startTime = window.getStart().getMillis();
    long endTime = window.getEnd().getMillis();
    MetricSlice metricSlice = MetricSlice.from(me.getId(), startTime, endTime, ArrayListMultimap.<String, String>create());
    InputData data = this.dataFetcher.fetchData(new InputDataSpec()
        .withTimeseriesSlices(Collections.singletonList(metricSlice))
        .withMetricIdsForDataset(Collections.singletonList(me.getId()))
        .withMetricIds(Collections.singletonList(me.getId())));
    DatasetConfigDTO datasetConfig = data.getDatasetForMetricId().get(me.getId());

    try {
      long datasetLastRefreshTime = datasetConfig.getLastRefreshTime();
      if (datasetLastRefreshTime <= 0) {
        // no availability event -> assume we have processed data till the current detection start
        datasetLastRefreshTime = startTime - 1;
      }

      MetricSlice slice = MetricSlice.from(me.getId(), datasetLastRefreshTime + 1, endTime);
      if (isMissingData(datasetLastRefreshTime, startTime)) {
        // Double check with data source as 2 things are possible.
        // 1. This dataset/source may not support availability events
        // 2. The data availability event pipeline has some issue.

        DataFrame dataFrame = data.getTimeseries().get(metricSlice);
        if (dataFrame == null || dataFrame.isEmpty()) {
          // no data
          if (hasMissedSLA(datasetLastRefreshTime, endTime)) {
            anomalies.add(createDataSLAAnomaly(slice, datasetConfig));
          }
        } else {
          datasetLastRefreshTime = dataFrame.getDoubles("timestamp").max().longValue();
          if (isPartialData(datasetLastRefreshTime, endTime, datasetConfig)) {
            if (hasMissedSLA(datasetLastRefreshTime, endTime)) {
              slice = MetricSlice.from(me.getId(), datasetLastRefreshTime + 1, endTime);
              anomalies.add(createDataSLAAnomaly(slice, datasetConfig));
            }
          }
        }
      } else if (isPartialData(datasetLastRefreshTime, endTime, datasetConfig)) {
        // Optimize for the common case - the common case is that the data availability events are arriving
        // correctly and we need not re-fetch the data to double check.
        if (hasMissedSLA(datasetLastRefreshTime, endTime)) {
          anomalies.add(createDataSLAAnomaly(slice, datasetConfig));
        }
      }
    } catch (Exception e) {
      LOG.error(String.format("Failed to run sla check on metric URN %s", me.getUrn()), e);
    }

    return anomalies;
  }

  /**
   * We say the data is missing if we do not have data in the sla detection window.
   * Or more specifically if the dataset watermark is below the sla detection window.
   */
  private boolean isMissingData(long datasetLastRefreshTime, long startTime) {
    return datasetLastRefreshTime < startTime;
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
  private boolean isPartialData(long datasetLastRefreshTime, long endTime, DatasetConfigDTO datasetConfig) {
    long granularity = datasetConfig.bucketTimeGranularity().toMillis();
    return (endTime - datasetLastRefreshTime) * 1.0 / granularity > 1;
  }

  /**
   * Validates if the data is delayed or not based on the user specified SLA configuration
   *
   * fetch the user configured SLA, otherwise default 3_DAYS.
   */
  private boolean hasMissedSLA(long datasetLastRefreshTime, long slaDetectionEndTime) {
    String sla = StringUtils.isNotEmpty(this.sla) ? this.sla : DEFAULT_DATA_SLA;
    long delay = TimeGranularity.fromString(sla).toPeriod().toStandardDuration().getMillis();

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
   * Creates a DATA_MISSING anomaly from ceiling(start) to ceiling(end) for the detection id.
   * If existing DATA_MISSING anomalies are present, then it will be merged accordingly.
   */
  private MergedAnomalyResultDTO createDataSLAAnomaly(MetricSlice slice, DatasetConfigDTO datasetConfig) {
    MergedAnomalyResultDTO anomaly = DetectionUtils.makeAnomaly(
        alignToUpperBoundary(slice.getStart(), datasetConfig),
        alignToUpperBoundary(slice.getEnd(), datasetConfig));
    anomaly.setCollection(datasetConfig.getName());
    anomaly.setType(AnomalyType.DATA_MISSING);

    // Store the metadata in the anomaly
    Map<String, String> properties = new HashMap<>();
    properties.put("datasetLastRefreshTime", String.valueOf(slice.getStart() - 1));
    properties.put("sla", sla);
    anomaly.setProperties(properties);

/*    List<MergedAnomalyResultDTO> existingAnomalies = anomalyDAO.findAnomaliesWithinBoundary(start, end, detectionId);
    if (!existingAnomalies.isEmpty()) {
      mergeSLAAnomalies(anomaly, existingAnomalies);
    } else {
      // no merging required
      this.anomalyDAO.save(anomaly);
      if (anomaly.getId() == null) {
        LOG.warn("Could not store data sla check failed anomaly:\n{}", anomaly);
      }
    }*/

    return anomaly;
  }

  /**
   * Merges one DATA_MISSING anomaly with remaining existing anomalies.
   */
/*  private void mergeSLAAnomalies(MergedAnomalyResultDTO anomaly, List<MergedAnomalyResultDTO> existingAnomalies) {
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
  }*/

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
