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
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.dataframe.BooleanSeries;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.spec.AbstractSpec;
import org.apache.pinot.thirdeye.detection.spi.components.BaseComponent;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;
import static org.apache.pinot.thirdeye.detection.DetectionUtils.*;


/**
 * DetectionPipeline forms the root of the detection class hierarchy. It represents a wireframe
 * for implementing (intermittently stateful) executable pipelines on top of it.
 */
public abstract class DetectionPipeline {
  private static String PROP_CLASS_NAME = "className";
  private static final Logger LOG = LoggerFactory.getLogger(DetectionPipeline.class);

  protected final DataProvider provider;
  protected final DetectionConfigDTO config;
  protected final long startTime;
  protected final long endTime;

  protected DetectionPipeline(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    this.provider = provider;
    this.config = config;
    this.startTime = startTime;
    this.endTime = endTime;
    try {
      this.initComponents();
    } catch (Exception e) {
      throw new IllegalArgumentException("Initialize components failed. Please check rule parameters. " + e.getMessage());
    }
  }

  /**
   * Returns a detection result for the time range between {@code startTime} and {@code endTime}.
   *
   * @return detection result
   * @throws Exception
   */
  public abstract DetectionPipelineResult run() throws Exception;

  /**
   * Initialize all components in the pipeline
   * @throws Exception
   */
  private void initComponents() throws Exception {
    InputDataFetcher dataFetcher = new DefaultInputDataFetcher(this.provider, this.config.getId());
    Map<String, BaseComponent> instancesMap = config.getComponents();
    Map<String, Object> componentSpecs = config.getComponentSpecs();
    if (componentSpecs != null) {
      for (String componentKey : componentSpecs.keySet()) {
        Map<String, Object> componentSpec = MapUtils.getMap(componentSpecs, componentKey);
        if (!instancesMap.containsKey(componentKey)){
          instancesMap.put(componentKey, createComponent(componentSpec));
        }
      }

      for (String componentKey : componentSpecs.keySet()) {
        Map<String, Object> componentSpec = MapUtils.getMap(componentSpecs, componentKey);
        for (Map.Entry<String, Object> entry : componentSpec.entrySet()){
          if (DetectionUtils.isReferenceName(entry.getValue().toString())) {
            componentSpec.put(entry.getKey(), instancesMap.get(DetectionUtils.getComponentKey(entry.getValue().toString())));
          }
        }
        instancesMap.get(componentKey).init(getComponentSpec(componentSpec), dataFetcher);
      }
    }
    config.setComponents(instancesMap);
  }

  private BaseComponent createComponent(Map<String, Object> componentSpec)
      throws Exception {
    Class<BaseComponent> clazz = (Class<BaseComponent>) Class.forName(MapUtils.getString(componentSpec, PROP_CLASS_NAME));
    return clazz.newInstance();
  }

  private AbstractSpec getComponentSpec(Map<String, Object> componentSpec) throws Exception {
    Class clazz = Class.forName(MapUtils.getString(componentSpec, PROP_CLASS_NAME));
    Class<AbstractSpec> specClazz = (Class<AbstractSpec>) Class.forName(getSpecClassName(clazz));
    return AbstractSpec.fromProperties(componentSpec, specClazz);
  }

  /**
   * Helper for creating an anomaly for a given metric slice. Injects properties such as
   * metric name, filter dimensions, etc.
   *
   * @param slice metric slice
   * @return anomaly template
   */
  protected final MergedAnomalyResultDTO makeAnomaly(MetricSlice slice) {
    Map<Long, MetricConfigDTO> metrics = this.provider.fetchMetrics(Collections.singleton(slice.getMetricId()));
    if (!metrics.containsKey(slice.getMetricId())) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.getMetricId()));
    }

    MetricConfigDTO metric = metrics.get(slice.getMetricId());

    return makeAnomaly(slice, metric);
  }

  /**
   * Helper for creating an anomaly for a given metric slice. Injects properties such as
   * metric name, filter dimensions, etc.
   *
   * @param slice metric slice
   * @param metric metric config dto related to slice
   * @return anomaly template
   */
  protected final MergedAnomalyResultDTO makeAnomaly(MetricSlice slice, MetricConfigDTO metric) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(slice.getStart());
    anomaly.setEndTime(slice.getEnd());
    anomaly.setMetric(metric.getName());
    anomaly.setCollection(metric.getDataset());
    anomaly.setMetricUrn(MetricEntity.fromSlice(slice, 1.0).getUrn());
    anomaly.setDimensions(toFilterMap(slice.getFilters()));
    anomaly.setDetectionConfigId(this.config.getId());
    anomaly.setChildren(new HashSet<MergedAnomalyResultDTO>());

    return anomaly;
  }

  /**
   * Helper for creating a list of anomalies from a boolean series. Injects properties via
   * {@code makeAnomaly(MetricSlice, MetricConfigDTO)}.
   *
   * @see DetectionPipeline#makeAnomaly(MetricSlice, MetricConfigDTO)
   *
   * @param slice metric slice
   * @param df time series with COL_TIME and at least one boolean value series
   * @param seriesName name of the value series
   * @return list of anomalies
   */
  protected final List<MergedAnomalyResultDTO> makeAnomalies(MetricSlice slice, DataFrame df, String seriesName) {
    if (df.isEmpty()) {
      return Collections.emptyList();
    }

    df = df.filter(df.getLongs(COL_TIME).between(slice.getStart(), slice.getEnd())).dropNull(COL_TIME);

    if (df.isEmpty()) {
      return Collections.emptyList();
    }

    Map<Long, MetricConfigDTO> metrics = this.provider.fetchMetrics(Collections.singleton(slice.getMetricId()));
    if (!metrics.containsKey(slice.getMetricId())) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.getMetricId()));
    }

    MetricConfigDTO metric = metrics.get(slice.getMetricId());

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
          anomalies.add(makeAnomaly(slice.withStart(start).withEnd(end), metric));
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
      DatasetConfigDTO dataset = this.provider.fetchDatasets(Collections.singleton(metric.getDataset())).get(metric.getDataset());
      if (dataset != null) {
        Period period = dataset.bucketTimeGranularity().toPeriod();
        DateTimeZone timezone = DateTimeZone.forID(dataset.getTimezone());

        long lastTimestamp = sTime.getLong(sTime.size() - 1);

        end = new DateTime(lastTimestamp, timezone).plus(period).getMillis();
      }

      // truncate at analysis end time
      end = Math.min(end, this.endTime);

      anomalies.add(makeAnomaly(slice.withStart(start).withEnd(end), metric));
    }

    return anomalies;
  }

  // TODO anomaly should support multimap
  private DimensionMap toFilterMap(Multimap<String, String> filters) {
    DimensionMap map = new DimensionMap();
    for (Map.Entry<String, String> entry : filters.entries()) {
      map.put(entry.getKey(), entry.getValue());
    }
    return map;
  }

  public DataProvider getProvider() {
    return provider;
  }

  public DetectionConfigDTO getConfig() {
    return config;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }
}
