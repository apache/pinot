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

import com.google.common.collect.Multimap;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.BooleanSeries;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.components.RuleBaselineProvider;
import org.apache.pinot.thirdeye.detection.spec.RuleBaselineProviderSpec;
import org.apache.pinot.thirdeye.detection.spi.components.BaseComponent;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.PeriodType;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


public class DetectionUtils {
  private static final String PROP_BASELINE_PROVIDER_COMPONENT_NAME = "baselineProviderComponentName";

  // TODO anomaly should support multimap
  public static DimensionMap toFilterMap(Multimap<String, String> filters) {
    DimensionMap map = new DimensionMap();
    for (Map.Entry<String, Collection<String>> entry: filters.asMap().entrySet()){
      map.put(entry.getKey(), String.join(", ", entry.getValue()));
    }
    return map;
  }

  // Check if a string is a component reference
  public static boolean isReferenceName(String key) {
    return key.startsWith("$");
  }

  // Extracts the component key from the reference key
  // e.g., "$myRule:ALGORITHM" -> "myRule:ALGORITHM"
  public static String getComponentKey(String componentRefKey) {
    if (isReferenceName(componentRefKey)) return componentRefKey.substring(1);
    else throw new IllegalArgumentException("not a component reference key. should starts with $");
  }

  // Extracts the component type from the component key
  // e.g., "myRule:ALGORITHM" -> "ALGORITHM"
  public static String getComponentType(String componentKey) {
    if (componentKey != null && componentKey.contains(":")) {
      return componentKey.substring(componentKey.lastIndexOf(":") + 1);
    }
    throw new IllegalArgumentException("componentKey is invalid; must be of type componentName:type");
  }

  // get the spec class name for a component class
  public static String getSpecClassName(Class<BaseComponent> componentClass) {
    ParameterizedType genericSuperclass = (ParameterizedType) componentClass.getGenericInterfaces()[0];
    return (genericSuperclass.getActualTypeArguments()[0].getTypeName());
  }


  /**
   * Helper for creating a list of anomalies from a boolean series.
   *
   * @param slice metric slice
   * @param df time series with COL_TIME and at least one boolean value series
   * @param seriesName name of the value series
   * @param endTime end time of this detection window
   * @param monitoringGranularityPeriod the monitoring granularity period
   * @param dataset dataset config for the metric
   * @return list of anomalies
   */
  public static List<MergedAnomalyResultDTO> makeAnomalies(MetricSlice slice, DataFrame df, String seriesName, long endTime, Period monitoringGranularityPeriod, DatasetConfigDTO dataset) {
    if (df.isEmpty()) {
      return Collections.emptyList();
    }

    df = df.filter(df.getLongs(COL_TIME).between(slice.getStart(), slice.getEnd())).dropNull(COL_TIME);

    if (df.isEmpty()) {
      return Collections.emptyList();
    }

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    LongSeries sTime = df.getLongs(COL_TIME);
    BooleanSeries sVal = df.getBooleans(seriesName);

    int lastStart = -1;
    for (int i = 0; i < df.size(); i++) {
      if (sVal.isNull(i) || !BooleanSeries.booleanValueOf(sVal.get(i))) {
        // end of a run
        if (lastStart >= 0) {
          long start = sTime.get(lastStart);
          long end = sTime.get(i);
          anomalies.add(makeAnomaly(slice.withStart(start).withEnd(end)));
        }
        lastStart = -1;

      } else {
        // start of a run
        if (lastStart < 0) {
          lastStart = i;
        }
      }
    }

    // end of current run
    if (lastStart >= 0) {
      long start = sTime.get(lastStart);
      long end = start + 1;

      // guess-timate of next time series timestamp
      if (dataset != null) {
        DateTimeZone timezone = DateTimeZone.forID(dataset.getTimezone());

        long lastTimestamp = sTime.getLong(sTime.size() - 1);

        end = new DateTime(lastTimestamp, timezone).plus(monitoringGranularityPeriod).getMillis();
      }

      // truncate at analysis end time
      end = Math.min(end, endTime);

      anomalies.add(makeAnomaly(slice.withStart(start).withEnd(end)));
    }

    return anomalies;
  }

  /**
   * Helper for creating an anomaly for a given metric slice. Injects properties such as
   * metric name, filter dimensions, etc.
   *
   * @param slice metric slice
   * @return anomaly template
   */
  public static MergedAnomalyResultDTO makeAnomaly(MetricSlice slice) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(slice.getStart());
    anomaly.setEndTime(slice.getEnd());

    return anomaly;
  }

  /**
   * Helper for consolidate last time stamps in all nested detection pipelines
   * @param nestedLastTimeStamps all nested last time stamps
   * @return the last time stamp
   */
  public static long consolidateNestedLastTimeStamps(Collection<Long> nestedLastTimeStamps){
    if(nestedLastTimeStamps.isEmpty()){
      return -1L;
    }
    return Collections.max(nestedLastTimeStamps);
  }

  /**
   * Get the predicted baseline for a anomaly in a time range. Will return wo1w if baseline provider not available.
   * @param anomaly the anomaly
   * @param config the detection config
   * @param start start time
   * @param end end time
   * @param loader detection pipeline loader
   * @param provider data provider
   * @return baseline time series
   * @throws Exception
   */
  public static TimeSeries getBaselineTimeseries(MergedAnomalyResultDTO anomaly, Multimap<String, String> filters, Long metricId, DetectionConfigDTO config,
      long start, long end, DetectionPipelineLoader loader, DataProvider provider) throws Exception {
    String baselineProviderComponentName = anomaly.getProperties().get(PROP_BASELINE_PROVIDER_COMPONENT_NAME);
    BaselineProvider baselineProvider = new RuleBaselineProvider();

    if (baselineProviderComponentName != null && config != null &&
        config.getComponentSpecs().containsKey(baselineProviderComponentName)) {
      // load pipeline and init components
      loader.from(provider, config, start, end);
      baselineProvider = (BaselineProvider) config.getComponents().get(baselineProviderComponentName);
    } else {
      // use wow instead
      RuleBaselineProviderSpec spec = new RuleBaselineProviderSpec();
      spec.setOffset("wo1w");
      InputDataFetcher dataFetcher = new DefaultInputDataFetcher(provider, config.getId());
      baselineProvider.init(spec, dataFetcher);
    }
    return baselineProvider.computePredictedTimeSeries(MetricSlice.from(metricId, start, end, filters));
  }

  /**
   * Get the joda period for a monitoring granularity
   * @param monitoringGranularity
   */
  public static Period getMonitoringGranularityPeriod(String monitoringGranularity, DatasetConfigDTO datasetConfigDTO) {
    if (monitoringGranularity.equals(MetricSlice.NATIVE_GRANULARITY.toAggregationGranularityString())) {
      return datasetConfigDTO.bucketTimeGranularity().toPeriod();
    }
    if (monitoringGranularity.equals("1_MONTHS")) {
      return new Period(0,1,0,0,0,0,0,0, PeriodType.months());
    }
    return TimeGranularity.fromString(monitoringGranularity).toPeriod();
  }

  public static Period periodFromTimeUnit(int size, TimeUnit unit) {
    switch (unit) {
      case DAYS:
        return Period.days(size);
      case HOURS:
        return Period.hours(size);
      case MINUTES:
        return Period.minutes(size);
      case SECONDS:
        return Period.seconds(size);
      case MILLISECONDS:
        return Period.millis(size);
      default:
        return new Period(TimeUnit.MILLISECONDS.convert(size, unit));
    }
  }
}
