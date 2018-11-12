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

package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * Detection pipeline for dimension exploration with a configurable nested detection pipeline.
 * Loads and prunes a metric's dimensions and sequentially retrieves data to run detection on
 * each filtered time series.
 */
public class DimensionWrapper extends DetectionPipeline {

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
  private final Period lookback;
  private final DateTimeZone timezone;

  protected final String nestedMetricUrnKey;
  protected final List<String> dimensions;
  protected final Collection<String> nestedMetricUrns;
  protected final List<Map<String, Object>> nestedProperties;

  public DimensionWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    // exploration
    this.metricUrn = MapUtils.getString(config.getProperties(), "metricUrn", null);
    this.minContribution = MapUtils.getDoubleValue(config.getProperties(), "minContribution", Double.NaN);
    this.minValue = MapUtils.getDoubleValue(config.getProperties(), "minValue", Double.NaN);
    this.minValueHourly = MapUtils.getDoubleValue(config.getProperties(), "minValueHourly", Double.NaN);
    this.minValueDaily = MapUtils.getDoubleValue(config.getProperties(), "minValueDaily", Double.NaN);
    this.k = MapUtils.getIntValue(config.getProperties(), "k", -1);
    this.dimensions = ConfigUtils.getList(config.getProperties().get("dimensions"));
    this.lookback = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "lookback", "1w"));
    this.timezone = DateTimeZone.forID(MapUtils.getString(config.getProperties(), "timezone", "America/Los_Angeles"));

    // prototyping
    this.nestedMetricUrns = ConfigUtils.getList(config.getProperties().get(PROP_NESTED_METRIC_URNS), Collections.singletonList(this.metricUrn));
    this.nestedMetricUrnKey = MapUtils.getString(config.getProperties(), PROP_NESTED_METRIC_URN_KEY, PROP_NESTED_METRIC_URN_KEY_DEFAULT);
    this.nestedProperties = ConfigUtils.getList(config.getProperties().get(PROP_NESTED));
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    List<MetricEntity> nestedMetrics = new ArrayList<>();

    if (this.metricUrn != null) {
      // metric and dimension exploration

      DateTime start = new DateTime(this.startTime, this.timezone);
      DateTime end = new DateTime(this.endTime, this.timezone);

      DateTime minStart = end.minus(this.lookback);
      if (minStart.isBefore(start)) {
        start = minStart;
      }

      Period testPeriod = new Period(start, end);

      MetricEntity metric = MetricEntity.fromURN(this.metricUrn);
      MetricSlice slice = MetricSlice.from(metric.getId(), start.getMillis(), end.getMillis(), metric.getFilters());

      DataFrame aggregates = this.provider.fetchAggregates(Collections.singletonList(slice), this.dimensions).get(slice);

      if (aggregates.isEmpty()) {
        return new DetectionPipelineResult(Collections.<MergedAnomalyResultDTO>emptyList());
      }

      final double total = aggregates.getDoubles(COL_VALUE).sum().fillNull().doubleValue();

      // min contribution
      if (!Double.isNaN(this.minContribution)) {
        aggregates = aggregates.filter(aggregates.getDoubles(COL_VALUE).divide(total).gte(this.minContribution)).dropNull();
      }

      // min value
      if (!Double.isNaN(this.minValue)) {
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

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    Map<String, Object> diagnostics = new HashMap<>();

    for (MetricEntity metric : nestedMetrics) {
      for (Map<String, Object> properties : this.nestedProperties) {
        DetectionPipelineResult intermediate = this.runNested(metric, properties);

        anomalies.addAll(intermediate.getAnomalies());
        diagnostics.put(metric.getUrn(), intermediate.getDiagnostics());
      }
    }

    return new DetectionPipelineResult(anomalies)
        .setDiagnostics(diagnostics);
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
