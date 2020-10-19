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
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.apache.pinot.thirdeye.anomaly.merge.AnomalyMergeConfig;
import org.apache.pinot.thirdeye.anomaly.merge.AnomalyMergeStrategy;
import org.apache.pinot.thirdeye.anomaly.merge.AnomalyTimeBasedSummarizer;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
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
import org.apache.pinot.thirdeye.datalayer.util.ThirdEyeDataUtils;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detector.function.BaseAnomalyFunction;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  The legacy merge wrapper. This runs the old anomaly function merger.
 */
public class LegacyMergeWrapper extends DetectionPipeline {
  private static final String PROP_SPEC = "specs";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_ANOMALY_FUNCTION_CLASS = "anomalyFunctionClassName";
  private static final AnomalyMergeConfig DEFAULT_TIME_BASED_MERGE_CONFIG;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(LegacyMergeWrapper.class);

  private final BaseAnomalyFunction anomalyFunction;
  private final Map<String, Object> anomalyFunctionSpecs;
  private final String anomalyFunctionClassName;
  private final long maxGap; // max time gap for merge
  private final List<Map<String, Object>> nestedProperties;
  private final AnomalySlice slice;
  private final AnomalyMergeConfig mergeConfig;

  static {
    DEFAULT_TIME_BASED_MERGE_CONFIG = new AnomalyMergeConfig();
    DEFAULT_TIME_BASED_MERGE_CONFIG.setSequentialAllowedGap(TimeUnit.HOURS.toMillis(2)); // merge anomalies apart 2 hours
    DEFAULT_TIME_BASED_MERGE_CONFIG.setMaxMergeDurationLength(TimeUnit.DAYS.toMillis(7) - 3600_000); // break anomaly longer than 6 days 23 hours
    DEFAULT_TIME_BASED_MERGE_CONFIG.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);
  }

  private static final Comparator<MergedAnomalyResultDTO> COMPARATOR = new Comparator<MergedAnomalyResultDTO>() {
    @Override
    public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
      return Long.compare(o1.getStartTime(), o2.getStartTime());
    }
  };

  /**
   * Instantiates a new Legacy merge wrapper.
   *
   * @param provider the provider
   * @param config the config
   * @param startTime the start time
   * @param endTime the end time
   * @throws Exception the exception
   */
  public LegacyMergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) throws Exception {
    super(provider, config, startTime, endTime);

    this.anomalyFunctionClassName = MapUtils.getString(config.getProperties(), PROP_ANOMALY_FUNCTION_CLASS);
    this.anomalyFunctionSpecs = ConfigUtils.getMap(config.getProperties().get(PROP_SPEC));
    this.anomalyFunction = (BaseAnomalyFunction) Class.forName(this.anomalyFunctionClassName).newInstance();

    String specs = OBJECT_MAPPER.writeValueAsString(this.anomalyFunctionSpecs);
    this.anomalyFunction.init(OBJECT_MAPPER.readValue(specs, AnomalyFunctionDTO.class));

    AnomalyMergeConfig mergeConfig = this.anomalyFunction.getSpec().getAnomalyMergeConfig();
    if (mergeConfig == null) {
      mergeConfig = DEFAULT_TIME_BASED_MERGE_CONFIG;
    }

    this.mergeConfig = mergeConfig;
    this.maxGap = mergeConfig.getSequentialAllowedGap();
    this.slice = new AnomalySlice()
        .withStart(startTime)
        .withEnd(endTime);

    if (config.getProperties().containsKey(PROP_NESTED)) {
      this.nestedProperties = ConfigUtils.getList(config.getProperties().get(PROP_NESTED));
    } else {
      this.nestedProperties = new ArrayList<>(Collections.singletonList(Collections.singletonMap(PROP_CLASS_NAME, (Object) LegacyDimensionWrapper.class.getName())));
    }
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    // generate anomalies
    List<MergedAnomalyResultDTO> generated = new ArrayList<>();

    for (Map<String, Object> properties : this.nestedProperties) {
      if (!properties.containsKey(PROP_SPEC)) {
        properties.put(PROP_SPEC, this.anomalyFunctionSpecs);
      }
      if (!properties.containsKey(PROP_ANOMALY_FUNCTION_CLASS)) {
        properties.put(PROP_ANOMALY_FUNCTION_CLASS, this.anomalyFunctionClassName);
      }
      DetectionPipelineResult intermediate = this.runNested(properties, startTime, endTime);
      generated.addAll(intermediate.getAnomalies());
    }

    // retrieve anomalies
    AnomalySlice effectiveSlice = this.slice
        .withDetectionId(this.config.getId())
        .withStart(this.getStartTime(generated) - this.maxGap)
        .withEnd(this.getEndTime(generated) + this.maxGap);

    List<MergedAnomalyResultDTO> retrieved = new ArrayList<>();
    retrieved.addAll(this.provider.fetchAnomalies(Collections.singleton(effectiveSlice)).get(effectiveSlice));

    return new DetectionPipelineResult(this.merge(generated, retrieved));
  }

  /**
   * Merge anomalies.
   *
   * @param generated the generated anomalies
   * @param retrieved the retrieved anomalies from database
   * @return the list
   */
  private List<MergedAnomalyResultDTO> merge(Collection<MergedAnomalyResultDTO> generated,
      Collection<MergedAnomalyResultDTO> retrieved) {
    List<MergedAnomalyResultDTO> mergedAnomalies = new ArrayList<>();
    this.mergeConfig.setMergeablePropertyKeys(this.anomalyFunction.getMergeablePropertyKeys());

    Map<DimensionMap, List<MergedAnomalyResultDTO>> retrievedAnomaliesByDimension = getAnomaliesByDimension(retrieved);
    Map<DimensionMap, List<MergedAnomalyResultDTO>> generatedAnomaliesByDimension = getAnomaliesByDimension(generated);

    int countRetrieved = 0;
    int countGenerated = 0;
    int countMerged = 0;

    for (Map.Entry<DimensionMap, List<MergedAnomalyResultDTO>> entry : generatedAnomaliesByDimension.entrySet()) {
      countGenerated += entry.getValue().size();

      // get latest overlapped merged anomaly
      MergedAnomalyResultDTO latestOverlappedMergedResult = null;
      List<MergedAnomalyResultDTO> retrievedAnomalies = new ArrayList<>();
      if (retrievedAnomaliesByDimension.containsKey(entry.getKey())) {
        retrievedAnomalies = retrievedAnomaliesByDimension.get(entry.getKey());
        Collections.sort(retrievedAnomalies, COMPARATOR);
        latestOverlappedMergedResult = retrievedAnomalies.get(0);
        countRetrieved += retrievedAnomalies.size();
      }

      List<MergedAnomalyResultDTO> retainedAnomalies = retainNewAnomalies(entry.getValue(), retrievedAnomalies);
      Collections.sort(retainedAnomalies, COMPARATOR);

      List<AnomalyResult> anomalyResults = new ArrayList<>();
      anomalyResults.addAll(retainedAnomalies); // type cast only

      List<MergedAnomalyResultDTO> mergedAnomalyResults;
      try {
        mergedAnomalyResults = AnomalyTimeBasedSummarizer.mergeAnomalies(latestOverlappedMergedResult, anomalyResults, this.mergeConfig);
      } catch (Exception e) {
        LOG.warn("Could not merge anomalies for dimension '{}'. Skipping.", entry.getKey(), e);
        continue;
      }

      countMerged += mergedAnomalyResults.size();

      AnomalyFunctionDTO anomalyFunctionSpec = this.anomalyFunction.getSpec();
      for (MergedAnomalyResultDTO mergedAnomalyResult : mergedAnomalyResults) {
        try {
          mergedAnomalyResult.setCollection(anomalyFunctionSpec.getCollection());
          mergedAnomalyResult.setMetric(anomalyFunctionSpec.getTopicMetric());

          // update current and baseline estimates
          MetricTimeSeries metricTimeSeries = getMetricTimeSeries(entry.getKey());
          this.anomalyFunction.updateMergedAnomalyInfo(mergedAnomalyResult, metricTimeSeries,
              new DateTime(mergedAnomalyResult.getStartTime()), new DateTime(mergedAnomalyResult.getEndTime()), retrievedAnomalies);

          // re-populate anomaly meta data after partial override from
          // AnomalyTimeBasedSummarizer and updateMergedAnomalyInfo()
          SetMultimap<String, String> filters = HashMultimap.create(anomalyFunctionSpec.getFilterSet());
          for (Map.Entry<String, String> dim : mergedAnomalyResult.getDimensions().entrySet()) {
            filters.removeAll(dim.getKey()); // remove pre-existing filters
            filters.put(dim.getKey(), dim.getValue());
          }

          mergedAnomalyResult.setMetricUrn(MetricEntity.fromMetric(1.0, anomalyFunctionSpec.getMetricId(), filters).getUrn());

          mergedAnomalyResult.setFunctionId(null);
          mergedAnomalyResult.setFunction(null);
          mergedAnomalyResult.setDetectionConfigId(this.config.getId());

          // global metric impact
          if (!StringUtils.isBlank(anomalyFunctionSpec.getGlobalMetric())) {
            MetricSlice slice = makeGlobalSlice(anomalyFunctionSpec, mergedAnomalyResult);

            double valGlobal = this.provider.fetchAggregates(Collections.singleton(slice), Collections.<String>emptyList(), -1).get(slice).getDouble(
                DataFrame.COL_VALUE, 0);
            double diffLocal = mergedAnomalyResult.getAvgCurrentVal() - mergedAnomalyResult.getAvgBaselineVal();

            mergedAnomalyResult.setImpactToGlobal(diffLocal / valGlobal);
          }

          mergedAnomalies.add(mergedAnomalyResult);

        } catch (Exception e) {
          LOG.warn("Could not update anomaly info for anomaly '{}'. Skipping.", mergedAnomalyResult, e);
        }
      }
    }

    LOG.info("Merged {} anomalies from {} retrieved and {} generated", countMerged, countRetrieved, countGenerated);

    return mergedAnomalies;
  }

  /**
   * Get metric time series for a dimension.
   */
  private MetricTimeSeries getMetricTimeSeries(DimensionMap dimension) {
    MetricEntity metricEntity = MetricEntity.fromMetric(1.0, anomalyFunction.getSpec().getMetricId(), getFiltersFromDimensionMap(dimension));
    MetricConfigDTO metricConfig = this.provider.fetchMetrics(Collections.singleton(metricEntity.getId())).get(metricEntity.getId());

    DataFrame df = DataFrame.builder(DataFrame.COL_TIME + ":LONG", DataFrame.COL_VALUE + ":DOUBLE").build();
    List<Pair<Long, Long>> timeIntervals = this.anomalyFunction.getDataRangeIntervals(this.startTime, this.endTime);
    for (Pair<Long, Long> startEndInterval : timeIntervals) {
      MetricSlice slice = MetricSlice.from(metricEntity.getId(), startEndInterval.getFirst(), startEndInterval.getSecond(), metricEntity.getFilters());
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
    return metricTimeSeries;
  }

  /**
   * Converts a DimensionMap into a filter multi map.
   *
   * @param dimensionMap map of dimension names and values
   * @return filter multi map
   */
  private Multimap<String, String> getFiltersFromDimensionMap(DimensionMap dimensionMap) {
    Multimap<String, String> filter = HashMultimap.create();
    for (Map.Entry<String, String> dimension : dimensionMap.entrySet()) {
      filter.put(dimension.getKey(), dimension.getValue());
    }
    return filter;
  }

  /**
   * Groups a set of anomalies per metric dimension(s).
   *
   * @param anomalies anomalies
   * @return map of collections of anomalies, keyed by dimensions
   */
  private Map<DimensionMap, List<MergedAnomalyResultDTO>> getAnomaliesByDimension(Collection<MergedAnomalyResultDTO> anomalies) {
    Map<DimensionMap, List<MergedAnomalyResultDTO>> anomaliesByDimension = new HashMap<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      DimensionMap dimension = anomaly.getDimensions();
      if (!anomaliesByDimension.containsKey(dimension)) {
        anomaliesByDimension.put(dimension, new ArrayList<MergedAnomalyResultDTO>());
      }
      anomaliesByDimension.get(dimension).add(anomaly);
    }
    return anomaliesByDimension;
  }

  /**
   * Returns the lowest start time of a collection of anomalies.
   *
   * @param anomalies anomalies
   * @return min start time
   */
  private long getStartTime(Iterable<MergedAnomalyResultDTO> anomalies) {
    long time = this.startTime;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      time = Math.min(anomaly.getStartTime(), time);
    }
    return time;
  }

  /**
   * Returns the highest end time of a collection of anomalies.
   *
   * @param anomalies anomalies
   * @return max end time
   */
  private long getEndTime(Iterable<MergedAnomalyResultDTO> anomalies) {
    long time = this.endTime;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      time = Math.max(anomaly.getEndTime(), time);
    }
    return time;
  }

  /**
   * Prepare input data for the legacy merger. Removes generated anomalies that
   * are already fully contained in existing anomalies.
   *
   * @param generated newly detected anomalies
   * @param existing persisted existing anomalies
   * @return subset of newly detected but not fully contained anomalies
   */
  private static List<MergedAnomalyResultDTO> retainNewAnomalies(Collection<MergedAnomalyResultDTO> generated, Collection<MergedAnomalyResultDTO> existing) {
    List<MergedAnomalyResultDTO> incoming = new ArrayList<>(generated);

    Iterator<MergedAnomalyResultDTO> itInc = incoming.iterator();
    while (itInc.hasNext()) {
      MergedAnomalyResultDTO inc = itInc.next();
      for (MergedAnomalyResultDTO ex : existing) {
        if (ex.getStartTime() <= inc.getStartTime() && ex.getEndTime() >= inc.getEndTime()) {
          itInc.remove();
          break;
        }
      }
    }

    return incoming;
  }

  /**
   * Returns a metrics slice for global impact metric of given anomaly
   *
   * @param spec legacy anomaly spec
   * @param anomaly anomaly
   * @return slice for global metric impact
   */
  private MetricSlice makeGlobalSlice(AnomalyFunctionDTO spec, MergedAnomalyResultDTO anomaly) {
    // TODO separate global metric lookup by name/dataset
    if (!spec.getMetric().equals(spec.getGlobalMetric())) {
      throw new IllegalArgumentException("Different local and global metrics not supported");
    }

    Multimap<String, String> globalFilters = ThirdEyeDataUtils.getFilterSet(spec.getGlobalMetricFilters());
    MetricEntity me = MetricEntity.fromURN(anomaly.getMetricUrn());
    return MetricSlice.from(me.getId(), anomaly.getStartTime(), anomaly.getEndTime(), globalFilters);
  }
}
