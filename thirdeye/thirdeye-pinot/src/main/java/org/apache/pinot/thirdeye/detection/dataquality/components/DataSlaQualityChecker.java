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
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
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
 * Performs data sla checks for the window and generates DATA_SLA anomalies.
 *
 * Data SLA is verified based on the following information.
 * a. The dataset refresh timestamp updated by the event based data availability pipeline (if applicable).
 * b. Otherwise, we will query the data source and run sla checks.
 */
@Components(title = "Data Sla Quality Checker",
    type = "DATA_SLA",
    tags = {DetectionTag.RULE_DETECTION},
    description = "Checks if data is missing or not based on the configured sla",
    presentation = {
        @PresentationOption(name = "data sla", template = "is ${sla}")
    },
    params = {
        @Param(name = "sla", placeholder = "value")
    })
public class DataSlaQualityChecker implements AnomalyDetector<DataSlaQualityCheckerSpec>, BaselineProvider<DataSlaQualityCheckerSpec> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSlaQualityChecker.class);

  private String sla;
  private InputDataFetcher dataFetcher;

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
   * Runs the data sla check for the window on the given metric.
   */
  private List<MergedAnomalyResultDTO> runSLACheck(MetricEntity me, Interval window) {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    long expectedDatasetRefreshTime = window.getEnd().getMillis();
    InputData data = this.dataFetcher.fetchData(new InputDataSpec()
        .withMetricIdsForDataset(Collections.singletonList(me.getId()))
        .withMetricIds(Collections.singletonList(me.getId())));
    DatasetConfigDTO datasetConfig = data.getDatasetForMetricId().get(me.getId());

    try {
      long datasetLastRefreshTime = fetchLatestDatasetRefreshTime(data, me, window);
      long datasetLastRefreshTimeRoundUp = alignToUpperBoundary(datasetLastRefreshTime, datasetConfig);
      MetricSlice slice = MetricSlice.from(me.getId(), datasetLastRefreshTimeRoundUp, expectedDatasetRefreshTime);

      if (isSLAViolated(datasetLastRefreshTimeRoundUp, expectedDatasetRefreshTime)) {
        anomalies.add(createDataSLAAnomaly(slice, datasetConfig, datasetLastRefreshTime, datasetLastRefreshTimeRoundUp));
      }
    } catch (Exception e) {
      LOG.error(String.format("Failed to run sla check on metric URN %s", me.getUrn()), e);
    }

    return anomalies;
  }

  /**
   * Fetches the latest timestamp for the dataset. It relies on 2 sources:
   * a. Data-trigger/availability based refresh timestamp
   * b. If not available, check by directly querying the data-source.
   */
  private long fetchLatestDatasetRefreshTime(InputData data, MetricEntity me, Interval window) {
    long startTime = window.getStart().getMillis();
    long endTime = window.getEnd().getMillis();
    // Note that we only measure the overall dataset availability. Filters are not considered as the
    // data availability events fire at the dataset level.
    MetricSlice metricSlice = MetricSlice.from(me.getId(), startTime, endTime, ArrayListMultimap.create());

    // Fetch dataset refresh time based on the data availability events
    DatasetConfigDTO datasetConfig = data.getDatasetForMetricId().get(me.getId());
    long eventBasedRefreshTime = datasetConfig.getLastRefreshTime();
    if (eventBasedRefreshTime <= 0) {
      // no availability event -> assume we have processed data till the current detection start
      eventBasedRefreshTime = startTime - 1;
    }

    // If the data availability event indicates no data or partial data, we will confirm with the data source.
    if (eventBasedRefreshTime < startTime || isPartialData(eventBasedRefreshTime, endTime, datasetConfig)) {
      // Double check with data source. This can happen if,
      // 1. This dataset/source doesn't not support data trigger/availability events
      // 2. The data trigger event didn't arrive due to some upstream issue.
      DataFrame dataFrame = this.dataFetcher.fetchData(new InputDataSpec()
          .withTimeseriesSlices(Collections.singletonList(metricSlice))).getTimeseries().get(metricSlice);
      if (dataFrame != null && !dataFrame.isEmpty()) {
        // Fetches the latest timestamp from the source. This value is already in epoch (UTC).
        return dataFrame.getDoubles("timestamp").max().longValue();
      }
    }

    return eventBasedRefreshTime;
  }

  /**
   * We say the data is partial if we do not have all the data points in the sla detection window.
   * Or more specifically if we have at least 1 data-point missing in the sla detection window.
   *
   * For example:
   * Assume that the data is delayed and our current sla detection window is [1st Feb to 3rd Feb). During this scan,
   * let's say data for 1st Feb arrives. Now, we have a situation where partial data is present. In other words, in the
   * current sla detection window [1st to 3rd Feb) we have data for 1st Feb but data for 2nd Feb is missing.
   *
   * Based on this information, we can smartly decide if we need to query the data source or not.
   */
  private boolean isPartialData(long actual, long expected, DatasetConfigDTO datasetConfig) {
    long granularity = datasetConfig.bucketTimeGranularity().toMillis();
    return (expected - actual) * 1.0 / granularity > 1;
  }

  /**
   * Validates if the data is delayed or not based on the user specified SLA configuration
   *
   * fetch the user configured SLA, otherwise default 3_DAYS.
   */
  private boolean isSLAViolated(long actualRefreshTime, long expectedRefreshTime) {
    long delay = TimeGranularity.fromString(this.sla).toPeriod().toStandardDuration().getMillis();
    return (expectedRefreshTime - actualRefreshTime) > delay;
  }

  /**
   * Align and round off timestamp to the upper boundary of the granularity
   *
   * Examples:
   * a. 20th Aug 05:00 pm will be rounded up to 21th Aug 12:00 am
   * b. 20th Aug 12:00 am will be rounded up to 21th Aug 12:00 am
   */
  private static long alignToUpperBoundary(long timestamp, DatasetConfigDTO datasetConfig) {
    Period granularityPeriod = datasetConfig.bucketTimeGranularity().toPeriod();
    DateTimeZone timezone = DateTimeZone.forID(datasetConfig.getTimezone());
    DateTime startTime = new DateTime(timestamp, timezone).plus(granularityPeriod);
    return (startTime.getMillis() / granularityPeriod.toStandardDuration().getMillis())
        * granularityPeriod.toStandardDuration().getMillis();
  }

  /**
   * Creates a DATA_SLA anomaly from ceiling(start) to ceiling(end) for the detection id.
   * If existing DATA_SLA anomalies are present, then it will be merged accordingly.
   *
   * @param datasetLastRefreshTime the timestamp corresponding to the last record in the data (UTC)
   * @param datasetLastRefreshTimeRoundUp the timestamp rounded off to the upper granular bucket. See
   * @{link #alignToUpperBoundary}
   */
  private MergedAnomalyResultDTO createDataSLAAnomaly(MetricSlice slice, DatasetConfigDTO datasetConfig,
      long datasetLastRefreshTime, long datasetLastRefreshTimeRoundUp) {
    MergedAnomalyResultDTO anomaly = DetectionUtils.makeAnomaly(slice.getStart(), slice.getEnd());
    anomaly.setCollection(datasetConfig.getName());
    anomaly.setType(AnomalyType.DATA_SLA);
    anomaly.setAnomalyResultSource(AnomalyResultSource.DATA_QUALITY_DETECTION);

    // Store the metadata in the anomaly
    Map<String, String> properties = new HashMap<>();
    properties.put("datasetLastRefreshTime", String.valueOf(datasetLastRefreshTime));
    properties.put("datasetLastRefreshTimeRoundUp", String.valueOf(datasetLastRefreshTimeRoundUp));
    properties.put("sla", this.sla);
    anomaly.setProperties(properties);

    return anomaly;
  }
}
