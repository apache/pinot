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

package org.apache.pinot.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * Detection pipeline for dimension exploration with a configurable nested detection pipeline.
 * Loads and prunes a metric's dimensions and sequentially retrieves data to run detection on
 * each filtered time series.
 */
public class DimensionWrapper extends DetectionPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionWrapper.class);

  // prototyping
  private static final String PROP_NESTED = "nested";

  private static final String PROP_NESTED_METRIC_URN_KEY = "nestedMetricUrnKey";
  private static final String PROP_NESTED_METRIC_URN_KEY_DEFAULT = "metricUrn";

  private static final String PROP_NESTED_METRIC_URNS = "nestedMetricUrns";

  private static final String PROP_CLASS_NAME = "className";

  private final String metricUrn;
  private final int k;
  private final double minContribution;
  private final double minValue;
  private final double minValueHourly;
  private final double minValueDaily;
  private final double minLiveZone;
  private final double liveBucketPercentageThreshold;
  private final Period lookback;
  private final DateTimeZone timezone;
  private DateTime start;
  private DateTime end;

  protected final String nestedMetricUrnKey;
  protected final List<String> dimensions;
  protected final Collection<String> nestedMetricUrns;
  protected final List<Map<String, Object>> nestedProperties;

  public DimensionWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    // the metric used in dimension exploration
    this.metricUrn = MapUtils.getString(config.getProperties(), "metricUrn", null);
    this.minContribution = MapUtils.getDoubleValue(config.getProperties(), "minContribution", Double.NaN);
    this.minValue = MapUtils.getDoubleValue(config.getProperties(), "minValue", Double.NaN);
    this.minValueHourly = MapUtils.getDoubleValue(config.getProperties(), "minValueHourly", Double.NaN);
    this.minValueDaily = MapUtils.getDoubleValue(config.getProperties(), "minValueDaily", Double.NaN);
    this.k = MapUtils.getIntValue(config.getProperties(), "k", -1);
    this.dimensions = ConfigUtils.getList(config.getProperties().get("dimensions"));
    this.lookback = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "lookback", "1w"));
    this.timezone = DateTimeZone.forID(MapUtils.getString(config.getProperties(), "timezone", "America/Los_Angeles"));

    /*
     * A bucket of the time series is taken into consider only if its value is above the minLiveZone. In other words,
     * if a bucket's value is smaller than minLiveZone, then this bucket is ignored when calculating the average value.
     * Used for outlier removal. Replace legacy average threshold filter.
     */
    this.minLiveZone = MapUtils.getDoubleValue(config.getProperties(), "minLiveZone", Double.NaN);
    this.liveBucketPercentageThreshold = MapUtils.getDoubleValue(config.getProperties(), "liveBucketPercentageThreshold", 0.5);

    // the metric to run the detection for
    this.nestedMetricUrns = ConfigUtils.getList(config.getProperties().get(PROP_NESTED_METRIC_URNS), Collections.singletonList(this.metricUrn));
    this.nestedMetricUrnKey = MapUtils.getString(config.getProperties(), PROP_NESTED_METRIC_URN_KEY, PROP_NESTED_METRIC_URN_KEY_DEFAULT);
    this.nestedProperties = ConfigUtils.getList(config.getProperties().get(PROP_NESTED));

    this.start = new DateTime(this.startTime, this.timezone);
    this.end = new DateTime(this.endTime, this.timezone);

    DateTime minStart = this.end.minus(this.lookback);
    if (minStart.isBefore(this.start)) {
      this.start = minStart;
    }
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    List<MetricEntity> nestedMetrics = new ArrayList<>();

    if (this.metricUrn != null) {
      // metric and dimension exploration
      Period testPeriod = new Period(this.start, this.end);

      MetricEntity metric = MetricEntity.fromURN(this.metricUrn);
      MetricSlice slice = MetricSlice.from(metric.getId(), this.start.getMillis(), this.end.getMillis(), metric.getFilters());

      DataFrame aggregates = this.provider.fetchAggregates(Collections.singletonList(slice), this.dimensions).get(slice);

      if (aggregates.isEmpty()) {
        return new DetectionPipelineResult(Collections.<MergedAnomalyResultDTO>emptyList(), -1);
      }

      final double total = aggregates.getDoubles(COL_VALUE).sum().fillNull().doubleValue();

      // min contribution
      if (!Double.isNaN(this.minContribution)) {
        aggregates = aggregates.filter(aggregates.getDoubles(COL_VALUE).divide(total).gte(this.minContribution)).dropNull();
      }

      // min value
      // check min value if only min live zone not set, other wise use checkMinLiveZone below
      if (!Double.isNaN(this.minValue) && Double.isNaN(this.minLiveZone)) {
        aggregates = aggregates.filter(aggregates.getDoubles(COL_VALUE).gte(this.minValue)).dropNull();
      }

      if (!Double.isNaN(this.minValueHourly)) {
        double multiplier = TimeUnit.HOURS.toMillis(1) / (double) testPeriod.toDurationFrom(start).getMillis();
        aggregates = aggregates.filter(aggregates.getDoubles(COL_VALUE).multiply(multiplier).gte(this.minValueHourly)).dropNull();
      }

      if (!Double.isNaN(this.minValueDaily)) {
        double multiplier = TimeUnit.DAYS.toMillis(1) / (double) testPeriod.toDurationFrom(start).getMillis();
        aggregates = aggregates.filter(aggregates.getDoubles(COL_VALUE).multiply(multiplier).gte(this.minValueDaily)).dropNull();
      }

      // top k
      if (this.k > 0) {
        aggregates = aggregates.sortedBy(COL_VALUE).tail(this.k).reverse();
      }

      for (String nestedMetricUrn : this.nestedMetricUrns) {
        for (int i = 0; i < aggregates.size(); i++) {
          Multimap<String, String> nestedFilter = ArrayListMultimap.create(metric.getFilters());

          for (String dimName : this.dimensions) {
            nestedFilter.removeAll(dimName); // clear any filters for explored dimension
            nestedFilter.put(dimName, aggregates.getString(dimName, i));
          }

          nestedMetrics.add(MetricEntity.fromURN(nestedMetricUrn).withFilters(nestedFilter));
        }
      }

    } else {
      // metric exploration only

      for (String nestedMetricUrn : this.nestedMetricUrns) {
        nestedMetrics.add(MetricEntity.fromURN(nestedMetricUrn));
      }
    }

    if (!Double.isNaN(this.minLiveZone) && !Double.isNaN(this.minValue)) {
      // filters all nested metric that didn't pass live zone check
      nestedMetrics = nestedMetrics.stream().filter(metricEntity -> checkMinLiveZone(metricEntity)).collect(Collectors.toList());
    }

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    Map<String, Object> diagnostics = new HashMap<>();
    Set<Long> lastTimeStamps = new HashSet<>();
    LOG.info("exploring {} metrics", nestedMetrics.size());
    for (MetricEntity metric : nestedMetrics) {
      for (Map<String, Object> properties : this.nestedProperties) {
        LOG.info("running detection for {}", metric.toString());
        DetectionPipelineResult intermediate = this.runNested(metric, properties);
        lastTimeStamps.add(intermediate.getLastTimestamp());
        anomalies.addAll(intermediate.getAnomalies());
        diagnostics.put(metric.getUrn(), intermediate.getDiagnostics());
      }
    }

    return new DetectionPipelineResult(anomalies, DetectionUtils.consolidateNestedLastTimeStamps(lastTimeStamps))
        .setDiagnostics(diagnostics);
  }

  private boolean checkMinLiveZone(MetricEntity me) {
    MetricSlice slice = MetricSlice.from(me.getId(), this.start.getMillis(), this.end.getMillis(), me.getFilters());
    DataFrame df = this.provider.fetchTimeseries(Collections.singleton(slice)).get(slice);
    long totalBuckets = df.size();
    df = df.filter(df.getDoubles(COL_VALUE).gt(this.minLiveZone)).dropNull();
    double liveBucketPercentage = (double) df.size() / (double) totalBuckets;
    if (liveBucketPercentage >= this.liveBucketPercentageThreshold) {
      return df.getDoubles(COL_VALUE).mean().getDouble(0)>= this.minValue;
    }
    return false;
  }

  protected DetectionPipelineResult runNested(MetricEntity metric, Map<String, Object> template) throws Exception {
    Preconditions.checkArgument(template.containsKey(PROP_CLASS_NAME), "Nested missing " + PROP_CLASS_NAME);

    Map<String, Object> properties = new HashMap<>(template);

    properties.put(this.nestedMetricUrnKey, metric.getUrn());

    DetectionConfigDTO nestedConfig = new DetectionConfigDTO();
    nestedConfig.setId(this.config.getId());
    nestedConfig.setName(this.config.getName());
    nestedConfig.setProperties(properties);
    nestedConfig.setComponents(this.config.getComponents());

    DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

    return pipeline.run();
  }
}
