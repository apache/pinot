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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.anomalydetection.datafilter.DataFilter;
import org.apache.pinot.thirdeye.anomalydetection.datafilter.DataFilterFactory;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.common.metric.MetricSchema;
import org.apache.pinot.thirdeye.common.metric.MetricSpec;
import org.apache.pinot.thirdeye.common.metric.MetricTimeSeries;
import org.apache.pinot.thirdeye.common.metric.MetricType;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detector.function.BaseAnomalyFunction;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Legacy anomaly function algorithm. This can run existing anomaly functions.
 */
public class LegacyAnomalyFunctionAlgorithm extends DetectionPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(LegacyAnomalyFunctionAlgorithm.class);
  private static final String PROP_ANOMALY_FUNCTION_CLASS = "anomalyFunctionClassName";
  private static final String PROP_SPEC = "specs";
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_FAIL_ON_ERROR = "failOnError";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final BaseAnomalyFunction anomalyFunction;
  private final MetricEntity metricEntity;
  private final DataFilter dataFilter;
  private final boolean failOnError;

  /**
   * Instantiates a new Legacy anomaly function algorithm.
   *
   * @param provider the provider
   * @param config the config
   * @param startTime the start time
   * @param endTime the end time
   * @throws Exception the exception
   */
  public LegacyAnomalyFunctionAlgorithm(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) throws Exception {
    super(provider, config, startTime, endTime);
    // TODO: Round start and end time stamps
    Preconditions.checkArgument(config.getProperties().containsKey(PROP_ANOMALY_FUNCTION_CLASS));
    String anomalyFunctionClassName = MapUtils.getString(config.getProperties(), PROP_ANOMALY_FUNCTION_CLASS);

    String specs = OBJECT_MAPPER.writeValueAsString(ConfigUtils.getMap(config.getProperties().get(PROP_SPEC)));
    this.anomalyFunction = (BaseAnomalyFunction) Class.forName(anomalyFunctionClassName).newInstance();
    this.anomalyFunction.init(OBJECT_MAPPER.readValue(specs, AnomalyFunctionDTO.class));

    this.dataFilter = DataFilterFactory.fromSpec(this.anomalyFunction.getSpec().getDataFilter());
    this.failOnError = MapUtils.getBooleanValue(config.getProperties(), PROP_FAIL_ON_ERROR, false);

    if (config.getProperties().containsKey(PROP_METRIC_URN)) {
      this.metricEntity = MetricEntity.fromURN(MapUtils.getString(config.getProperties(), PROP_METRIC_URN));
    } else {
      this.metricEntity = makeEntity(this.anomalyFunction.getSpec());
    }
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    LOG.info("Running legacy anomaly detection for time range {} to {}", this.startTime, this.endTime);

    Collection<MergedAnomalyResultDTO> mergedAnomalyResults = new ArrayList<>();

    try {
      Collection<MergedAnomalyResultDTO> historyMergedAnomalies;
      if (this.anomalyFunction.useHistoryAnomaly() && config.getId() != null) {
        AnomalySlice slice = new AnomalySlice()
            .withDetectionId(this.config.getId())
            .withStart(this.startTime)
            .withEnd(this.endTime);
        historyMergedAnomalies = this.provider.fetchAnomalies(Collections.singletonList(slice)).get(slice);
      } else {
        historyMergedAnomalies = Collections.emptyList();
      }

      final DimensionMap dimension = getDimensionMap();

      final MetricConfigDTO metricConfig =
          this.provider.fetchMetrics(Collections.singleton(this.metricEntity.getId())).get(this.metricEntity.getId());

      // get time series
      DataFrame df = DataFrame.builder(DataFrame.COL_TIME + ":LONG", DataFrame.COL_VALUE + ":DOUBLE").build();
      List<Pair<Long, Long>> timeIntervals = this.anomalyFunction.getDataRangeIntervals(this.startTime, this.endTime);
      for (Pair<Long, Long> startEndInterval : timeIntervals) {
        MetricSlice slice = MetricSlice.from(this.metricEntity.getId(), startEndInterval.getFirst(), startEndInterval.getSecond(), metricEntity.getFilters());
        DataFrame currentDf = this.provider.fetchTimeseries(Collections.singleton(slice)).get(slice);
        df = df.append(currentDf);
      }

      MetricTimeSeries metricTimeSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(
          Collections.singletonList(new MetricSpec(metricConfig.getName(), MetricType.DOUBLE))));

      LongSeries timestamps = df.getLongs(DataFrame.COL_TIME);
      for (int i = 0; i < timestamps.size(); i++) {
        metricTimeSeries.set(timestamps.get(i), metricConfig.getName(), df.getDoubles(
            DataFrame.COL_VALUE).get(i));
      }

      if (!this.dataFilter.isQualified(metricTimeSeries, dimension, this.startTime, this.endTime)) {
        return new DetectionPipelineResult(Collections.<MergedAnomalyResultDTO>emptyList());
      }

      List<AnomalyResult> result = this.anomalyFunction.analyze(dimension, metricTimeSeries,
          new DateTime(this.startTime), new DateTime(this.endTime), new ArrayList<>(historyMergedAnomalies));

      mergedAnomalyResults = Collections2.transform(result, new Function<AnomalyResult, MergedAnomalyResultDTO>() {
        @Override
        public MergedAnomalyResultDTO apply(AnomalyResult result) {
          MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
          anomaly.populateFrom(result);
          anomaly.setFunctionId(null);
          anomaly.setFunction(null);
          anomaly.setDetectionConfigId(LegacyAnomalyFunctionAlgorithm.this.config.getId());
          anomaly.setMetricUrn(metricEntity.getUrn());
          anomaly.setMetric(metricConfig.getName());
          anomaly.setCollection(metricConfig.getDataset());
          anomaly.setDimensions(dimension);
          return anomaly;
        }
      });

    } catch (Exception e) {
      if (this.failOnError) {
        throw e;
      } else {
        LOG.warn("Encountered exception during legacy execution. Skipping.", e);
      }
    }

    LOG.info("Detected {} anomalies for {}", mergedAnomalyResults.size(), this.metricEntity.getUrn());

    return new DetectionPipelineResult(new ArrayList<>(mergedAnomalyResults));
  }

  private DimensionMap getDimensionMap() {
    DimensionMap dimensionMap = new DimensionMap();
    for (Map.Entry<String, String> entry : metricEntity.getFilters().entries()) {
      dimensionMap.put(entry.getKey(), entry.getValue());
    }
    return dimensionMap;
  }

  private static MetricEntity makeEntity(AnomalyFunctionDTO spec) {
    return MetricEntity.fromMetric(1.0, spec.getMetricId(), spec.getFilterSet());
  }
}
