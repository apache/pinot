/*
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

package com.linkedin.thirdeye.detection.wrapper;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobSchedulerUtils;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.DetectionUtils;
import com.linkedin.thirdeye.detection.spi.components.AnomalyDetector;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Anomaly Detector Wrapper. This wrapper runs a anomaly detector and return the anomalies.
 * Optionally set the detection window to be moving window fashion. This wrapper will call detection multiple times with
 * sliding window. Each sliding window start time and end time is aligned to the data granularity. Each window size is set by the spec.
 */
public class AnomalyDetectorWrapper extends DetectionPipeline {
  private static final String PROP_METRIC_URN = "metricUrn";

  // moving window detection properties
  private static final String PROP_MOVING_WINDOW_DETECTION = "isMovingWindowDetection";
  private static final String PROP_WINDOW_DELAY = "windowDelay";
  private static final String PROP_WINDOW_DELAY_UNIT = "windowDelayUnit";
  private static final String PROP_WINDOW_SIZE = "windowSize";
  private static final String PROP_WINDOW_UNIT = "windowUnit";
  private static final String PROP_FREQUENCY = "frequency";
  private static final String PROP_DETECTOR = "detector";
  private static final Logger LOG = LoggerFactory.getLogger(
      AnomalyDetectorWrapper.class);

  private final String metricUrn;
  private final AnomalyDetector anomalyDetector;

  private final int windowDelay;
  private final TimeUnit windowDelayUnit;
  private final int windowSize;
  private final TimeUnit windowUnit;
  private final boolean isMovingWindowDetection;
  private DatasetConfigDTO dataset;
  private DateTimeZone dateTimeZone;
  // need to specify run frequency for minute level detection. Used for moving monitoring window alignment, default to be 15 minutes.
  private final TimeGranularity functionFrequency;

  public AnomalyDetectorWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    this.metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);

    Preconditions.checkArgument(this.config.getProperties().containsKey(PROP_DETECTOR));
    String detectorReferenceKey = DetectionUtils.getComponentName(MapUtils.getString(config.getProperties(), PROP_DETECTOR));
    Preconditions.checkArgument(this.config.getComponents().containsKey(detectorReferenceKey));
    this.anomalyDetector = (AnomalyDetector) this.config.getComponents().get(detectorReferenceKey);

    this.isMovingWindowDetection = MapUtils.getBooleanValue(config.getProperties(), PROP_MOVING_WINDOW_DETECTION, false);
    // delays to wait for data becomes available
    this.windowDelay = MapUtils.getIntValue(config.getProperties(), PROP_WINDOW_DELAY, 0);
    this.windowDelayUnit = TimeUnit.valueOf(MapUtils.getString(config.getProperties(), PROP_WINDOW_DELAY_UNIT, "DAYS"));
    // detection window size
    this.windowSize = MapUtils.getIntValue(config.getProperties(), PROP_WINDOW_SIZE, 1);
    this.windowUnit = TimeUnit.valueOf(MapUtils.getString(config.getProperties(), PROP_WINDOW_UNIT, "DAYS"));
    Map<String, Object> frequency = MapUtils.getMap(config.getProperties(), PROP_FREQUENCY, Collections.emptyMap());
    this.functionFrequency = new TimeGranularity(MapUtils.getIntValue(frequency, "size", 15), TimeUnit.valueOf(MapUtils.getString(frequency, "unit", "MINUTES")));
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    List<Interval> monitoringWindows = this.getMonitoringWindows();
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    for (Interval window : monitoringWindows) {
      anomalies.addAll(anomalyDetector.runDetection(window, this.metricUrn));
    }

    MetricEntity me = MetricEntity.fromURN(this.metricUrn);
    MetricConfigDTO metric = provider.fetchMetrics(Collections.singleton(me.getId())).get(me.getId());

    for (MergedAnomalyResultDTO anomaly : anomalies) {
      anomaly.setDetectionConfigId(this.config.getId());
      anomaly.setMetricUrn(this.metricUrn);
      anomaly.setMetric(metric.getName());
      anomaly.setCollection(metric.getDataset());
      anomaly.setDimensions(DetectionUtils.toFilterMap(me.getFilters()));
    }
    return new DetectionPipelineResult(anomalies);
  }

  List<Interval> getMonitoringWindows() {
    if (this.isMovingWindowDetection) {
      try{
        List<Interval> monitoringWindows = new ArrayList<>();
        MetricEntity me = MetricEntity.fromURN(this.metricUrn);
        MetricConfigDTO metricConfigDTO =
            this.provider.fetchMetrics(Collections.singletonList(me.getId())).get(me.getId());
        dataset = this.provider.fetchDatasets(Collections.singletonList(metricConfigDTO.getDataset()))
            .get(metricConfigDTO.getDataset());
        dateTimeZone = DateTimeZone.forID(dataset.getTimezone());
        List<Long> monitoringWindowEndTimes = getMonitoringWindowEndTimes();
        for (long monitoringEndTime : monitoringWindowEndTimes) {
          long endTime = monitoringEndTime - TimeUnit.MILLISECONDS.convert(windowDelay, windowDelayUnit);
          long startTime = endTime - TimeUnit.MILLISECONDS.convert(windowSize, windowUnit);
          monitoringWindows.add(new Interval(startTime, endTime, dateTimeZone));
        }
        return monitoringWindows;
      } catch (Exception e) {
        LOG.info("can't generate moving monitoring windows, calling with single detection window", e);
      }
    }
    return Collections.singletonList(new Interval(startTime, endTime));
  }

  private List<Long> getMonitoringWindowEndTimes() {
    List<Long> endTimes = new ArrayList<>();

    // get current hour/day, depending on granularity of dataset,
    DateTime currentEndTime = new DateTime(getBoundaryAlignedTimeForDataset(new DateTime(endTime, dateTimeZone)), dateTimeZone);

    DateTime lastDateTime = new DateTime(getBoundaryAlignedTimeForDataset(new DateTime(startTime, dateTimeZone)), dateTimeZone);
    Period bucketSizePeriod = getBucketSizePeriodForDataset();
    while (lastDateTime.isBefore(currentEndTime)) {
      lastDateTime = lastDateTime.plus(bucketSizePeriod);
      endTimes.add(lastDateTime.getMillis());
    }
    return endTimes;
  }

  private long getBoundaryAlignedTimeForDataset(DateTime currentTime) {
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(dataset);
    TimeUnit dataUnit = timeSpec.getDataGranularity().getUnit();

    // For nMINUTE level datasets, with frequency defined in nMINUTES in the function, (make sure size doesnt exceed 30 minutes, just use 1 HOUR in that case)
    // Calculate time periods according to the function frequency
    if (dataUnit.equals(TimeUnit.MINUTES) || dataUnit.equals(TimeUnit.MILLISECONDS) || dataUnit.equals(
        TimeUnit.SECONDS)) {
      if (functionFrequency.getUnit().equals(TimeUnit.MINUTES) && (functionFrequency.getSize() <= 30)) {
        int minuteBucketSize = functionFrequency.getSize();
        int roundedMinutes = (currentTime.getMinuteOfHour() / minuteBucketSize) * minuteBucketSize;
        currentTime = currentTime.withTime(currentTime.getHourOfDay(), roundedMinutes, 0, 0);
      } else {
        currentTime = DetectionJobSchedulerUtils.getBoundaryAlignedTimeForDataset(currentTime,
            TimeUnit.HOURS); // default to HOURS
      }
    } else {
      currentTime = DetectionJobSchedulerUtils.getBoundaryAlignedTimeForDataset(currentTime, dataUnit);
    }

    return currentTime.getMillis();
  }

  public Period getBucketSizePeriodForDataset() {
    Period bucketSizePeriod = null;
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(dataset);
    TimeUnit dataUnit = timeSpec.getDataGranularity().getUnit();

    // For nMINUTE level datasets, with frequency defined in nMINUTES in the function, (make sure size doesnt exceed 30 minutes, just use 1 HOUR in that case)
    // Calculate time periods according to the function frequency
    if (dataUnit.equals(TimeUnit.MINUTES) || dataUnit.equals(TimeUnit.MILLISECONDS) || dataUnit.equals(
        TimeUnit.SECONDS)) {
      if (functionFrequency.getUnit().equals(TimeUnit.MINUTES) && (functionFrequency.getSize() <= 30)) {
        bucketSizePeriod = new Period(0, 0, 0, 0, 0, functionFrequency.getSize(), 0, 0);
      } else {
        bucketSizePeriod = DetectionJobSchedulerUtils.getBucketSizePeriodForUnit(TimeUnit.HOURS); // default to 1 HOUR
      }
    } else {
      bucketSizePeriod = DetectionJobSchedulerUtils.getBucketSizePeriodForUnit(dataUnit);
    }
    return bucketSizePeriod;
  }
}
