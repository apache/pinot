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

package org.apache.pinot.thirdeye.detection.wrapper;

import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.anomaly.detection.DetectionJobSchedulerUtils;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.api.TimeGranularity;
import org.apache.pinot.thirdeye.api.TimeSpec;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
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

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


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
  private static final String PROP_DETECTOR_COMPONENT_NAME = "detectorComponentName";
  private static final String PROP_TIMEZONE = "timezone";

  private static final Logger LOG = LoggerFactory.getLogger(
      AnomalyDetectorWrapper.class);

  private final String metricUrn;
  private final AnomalyDetector anomalyDetector;

  private final int windowDelay;
  private final TimeUnit windowDelayUnit;
  private final int windowSize;
  private final TimeUnit windowUnit;
  private final MetricConfigDTO metric;
  private final MetricEntity metricEntity;
  private final boolean isMovingWindowDetection;
  // need to specify run frequency for minute level detection. Used for moving monitoring window alignment, default to be 15 minutes.
  private final TimeGranularity functionFrequency;
  private final String detectorName;
  private final long windowSizeMillis;
  private final DatasetConfigDTO dataset;
  private final DateTimeZone dateTimeZone;


  public AnomalyDetectorWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    this.metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    this.metricEntity = MetricEntity.fromURN(this.metricUrn);
    this.metric = provider.fetchMetrics(Collections.singleton(this.metricEntity.getId())).get(this.metricEntity.getId());

    Preconditions.checkArgument(this.config.getProperties().containsKey(PROP_DETECTOR));
    this.detectorName = DetectionUtils.getComponentName(MapUtils.getString(config.getProperties(), PROP_DETECTOR));
    Preconditions.checkArgument(this.config.getComponents().containsKey(this.detectorName));
    this.anomalyDetector = (AnomalyDetector) this.config.getComponents().get(this.detectorName);

    // emulate moving window or now
    this.isMovingWindowDetection = MapUtils.getBooleanValue(config.getProperties(), PROP_MOVING_WINDOW_DETECTION, false);
    // delays to wait for data becomes available
    this.windowDelay = MapUtils.getIntValue(config.getProperties(), PROP_WINDOW_DELAY, 0);
    // window delay unit
    this.windowDelayUnit = TimeUnit.valueOf(MapUtils.getString(config.getProperties(), PROP_WINDOW_DELAY_UNIT, "DAYS"));
    // detection window size
    this.windowSize = MapUtils.getIntValue(config.getProperties(), PROP_WINDOW_SIZE, 1);
    this.windowUnit = TimeUnit.valueOf(MapUtils.getString(config.getProperties(), PROP_WINDOW_UNIT, "DAYS"));
    this.windowSizeMillis = TimeUnit.MILLISECONDS.convert(windowSize, windowUnit);
    // run frequency, used to determine moving windows for minute-level detection
    Map<String, Object> frequency = MapUtils.getMap(config.getProperties(), PROP_FREQUENCY, Collections.emptyMap());
    this.functionFrequency = new TimeGranularity(MapUtils.getIntValue(frequency, "size", 15), TimeUnit.valueOf(MapUtils.getString(frequency, "unit", "MINUTES")));

    MetricConfigDTO metricConfigDTO = this.provider.fetchMetrics(Collections.singletonList(this.metricEntity.getId())).get(this.metricEntity.getId());
    this.dataset = this.provider.fetchDatasets(Collections.singletonList(metricConfigDTO.getDataset()))
        .get(metricConfigDTO.getDataset());
    // date time zone for moving windows. use dataset time zone as default
    this.dateTimeZone = DateTimeZone.forID(MapUtils.getString(config.getProperties(), PROP_TIMEZONE, "America/Los_Angeles"));
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    List<Interval> monitoringWindows = this.getMonitoringWindows();
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    for (Interval window : monitoringWindows) {
      List<MergedAnomalyResultDTO> anomaliesForOneWindow = new ArrayList<>();
      try {
        LOG.info("[New Pipeline] running detection for config {} metricUrn {}. start time {}, end time{}", config.getId(), metricUrn, window.getStart(), window.getEnd());
        anomaliesForOneWindow = anomalyDetector.runDetection(window, this.metricUrn);
      } catch (Exception e) {
        LOG.warn("[DetectionConfigID{}] detecting anomalies for window {} to {} failed.", this.config.getId(), window.getStart(), window.getEnd(), e);
      }
      anomalies.addAll(anomaliesForOneWindow);
    }

    for (MergedAnomalyResultDTO anomaly : anomalies) {
      anomaly.setDetectionConfigId(this.config.getId());
      anomaly.setMetricUrn(this.metricUrn);
      anomaly.setMetric(this.metric.getName());
      anomaly.setCollection(this.metric.getDataset());
      anomaly.setDimensions(DetectionUtils.toFilterMap(this.metricEntity.getFilters()));
      anomaly.getProperties().put(PROP_DETECTOR_COMPONENT_NAME, this.detectorName);
    }
    return new DetectionPipelineResult(anomalies, this.getLastTimeStamp());
  }

  // guess-timate next time stamp
  // there are two cases. If the data is complete, next detection starts from the end time of this detection
  // If data is incomplete, next detection starts from the latest available data's time stamp plus the one time granularity.
  long getLastTimeStamp(){
    long end = this.endTime;
    if (this.dataset != null) {
      MetricSlice metricSlice = MetricSlice.from(this.metricEntity.getId(),
          this.startTime,
          this.endTime,
          this.metricEntity.getFilters());
      DoubleSeries timestamps = this.provider.fetchTimeseries(Collections.singleton(metricSlice)).get(metricSlice).getDoubles(COL_TIME);
      if (timestamps.size() == 0) {
        // no data available, don't update time stamp
        return -1;
      }
      Period period = dataset.bucketTimeGranularity().toPeriod();
      DateTimeZone timezone = DateTimeZone.forID(dataset.getTimezone());
      long lastTimestamp = timestamps.getLong(timestamps.size() - 1);

      end = new DateTime(lastTimestamp, timezone).plus(period).getMillis();
    }

    // truncate at analysis end time
    return Math.min(end, this.endTime);
  }

  // get a list of the monitoring window, if no sliding window used, use start time and end time as window
  List<Interval> getMonitoringWindows() {
    if (this.isMovingWindowDetection) {
      try{
        List<Interval> monitoringWindows = new ArrayList<>();
        List<Long> monitoringWindowEndTimes = getMonitoringWindowEndTimes();
        for (long monitoringEndTime : monitoringWindowEndTimes) {
          long endTime = monitoringEndTime - TimeUnit.MILLISECONDS.convert(windowDelay, windowDelayUnit);
          long startTime = endTime - this.windowSizeMillis;
          monitoringWindows.add(new Interval(startTime, endTime, dateTimeZone));
        }
        for (Interval window : monitoringWindows){
          LOG.info("running detections in windows {}", window);
        }
        return monitoringWindows;
      } catch (Exception e) {
        LOG.info("can't generate moving monitoring windows, calling with single detection window", e);
      }
    }
    return Collections.singletonList(new Interval(startTime, endTime));
  }

  // get the list of monitoring window end times
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

  /**
   * round this time to earlier boundary, depending on granularity of dataset
   * e.g. 12:15pm on HOURLY dataset should be treated as 12pm
   * any dataset with granularity finer than HOUR, will be rounded as per function frequency (assumption is that this is in MINUTES)
   * so 12.53 on 5 MINUTES dataset, with function frequency 15 MINUTES will be rounded to 12.45
   * See also {@link DetectionJobSchedulerUtils#getBoundaryAlignedTimeForDataset(DateTime, TimeUnit)}
   */
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

  /**
   * get bucket size in millis, according to data granularity of dataset
   * Bucket size are 1 HOUR for hourly, 1 DAY for daily
   * For MINUTE level data, bucket size is calculated based on anomaly function frequency
   * See also {@link DetectionJobSchedulerUtils#getBucketSizePeriodForDataset(DatasetConfigDTO, AnomalyFunctionDTO)} (DateTime, TimeUnit)}
   */
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
