package com.linkedin.thirdeye.detection.algorithm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeStrategy;
import com.linkedin.thirdeye.anomaly.merge.AnomalyTimeBasedSummarizer;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 *  The legacy merge wrapper. This runs the old anomaly function merger.
 */
public class LegacyMergeWrapper extends DetectionPipeline {
  private static String PROP_SPEC = "specs";
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
  private AnomalyMergeConfig mergeConfig;

  static {
    DEFAULT_TIME_BASED_MERGE_CONFIG = new AnomalyMergeConfig();
    DEFAULT_TIME_BASED_MERGE_CONFIG.setSequentialAllowedGap(
        TimeUnit.HOURS.toMillis(2)); // merge anomalies apart 2 hours
    DEFAULT_TIME_BASED_MERGE_CONFIG.setMaxMergeDurationLength(
        TimeUnit.DAYS.toMillis(7) - 3600_000); // break anomaly longer than 6 days 23 hours
    DEFAULT_TIME_BASED_MERGE_CONFIG.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);
  }

  private static final Comparator<MergedAnomalyResultDTO> COMPARATOR = new Comparator<MergedAnomalyResultDTO>() {
    @Override
    public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
      // earlier
      int res = Long.compare(o1.getStartTime(), o2.getStartTime());
      if (res != 0) {
        return res;
      }

      // pre-existing
      if (o1.getId() == null && o2.getId() != null) {
        return 1;
      }
      if (o1.getId() != null && o2.getId() == null) {
        return -1;
      }

      // more children
      return -1 * Integer.compare(o1.getChildren().size(), o2.getChildren().size());
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
  public LegacyMergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);

    anomalyFunctionClassName = MapUtils.getString(config.getProperties(), PROP_ANOMALY_FUNCTION_CLASS);
    anomalyFunctionSpecs = MapUtils.getMap(config.getProperties(), PROP_SPEC);
    anomalyFunction = (BaseAnomalyFunction) Class.forName(anomalyFunctionClassName).newInstance();

    String specs = OBJECT_MAPPER.writeValueAsString(anomalyFunctionSpecs);
    anomalyFunction.init(OBJECT_MAPPER.readValue(specs, AnomalyFunctionDTO.class));
    mergeConfig = anomalyFunction.getSpec().getAnomalyMergeConfig();
    if (mergeConfig == null) {
      mergeConfig = DEFAULT_TIME_BASED_MERGE_CONFIG;
    }
    this.maxGap = mergeConfig.getSequentialAllowedGap();
    this.slice = new AnomalySlice().withStart(startTime).withEnd(endTime).withConfigId(config.getId());
    this.nestedProperties = ConfigUtils.getList(config.getProperties().get(PROP_NESTED));
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    // generate anomalies
    List<MergedAnomalyResultDTO> generated = new ArrayList<>();

    for (Map<String, Object> properties : this.nestedProperties) {
      DetectionConfigDTO nestedConfig = new DetectionConfigDTO();

      Preconditions.checkArgument(properties.containsKey(PROP_CLASS_NAME), "Nested missing " + PROP_CLASS_NAME);

      properties.put(PROP_ANOMALY_FUNCTION_CLASS, anomalyFunctionClassName);
      properties.put(PROP_SPEC, anomalyFunctionSpecs);
      nestedConfig.setId(this.config.getId());
      nestedConfig.setName(this.config.getName());
      nestedConfig.setProperties(properties);

      DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

      DetectionPipelineResult intermediate = pipeline.run();

      generated.addAll(intermediate.getAnomalies());
    }

    // retrieve anomalies
    AnomalySlice effectiveSlice = this.slice.withStart(this.getStartTime(generated) - this.maxGap)
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
    mergeConfig.setMergeablePropertyKeys(anomalyFunction.getMergeablePropertyKeys());

    Map<DimensionMap, List<MergedAnomalyResultDTO>> retrievedAnomaliesByDimension = getAnomaliesByDimension(retrieved);
    Map<DimensionMap, List<MergedAnomalyResultDTO>> generatedAnomaliesByDimension = getAnomaliesByDimension(generated);

    for (Map.Entry<DimensionMap, List<MergedAnomalyResultDTO>> entry : generatedAnomaliesByDimension.entrySet()) {
      // get latest overlapped merged anomaly
      MergedAnomalyResultDTO latestOverlappedMergedResult = null;
      List<MergedAnomalyResultDTO> retrievedAnomalies = null;
      if (retrievedAnomaliesByDimension.containsKey(entry.getKey())) {
        retrievedAnomalies = retrievedAnomaliesByDimension.get(entry.getKey());
        Collections.sort(retrievedAnomalies, COMPARATOR);
        latestOverlappedMergedResult = retrievedAnomalies.get(0);
      }

      List<AnomalyResult> generatedAnomalies = new ArrayList<>();
      generatedAnomalies.addAll(entry.getValue());
      List<MergedAnomalyResultDTO> mergedAnomalyResults =
          AnomalyTimeBasedSummarizer.mergeAnomalies(latestOverlappedMergedResult, generatedAnomalies, mergeConfig);

      AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunction.getSpec();
      for (MergedAnomalyResultDTO mergedAnomalyResult : mergedAnomalyResults) {
        mergedAnomalyResult.setFunction(anomalyFunctionSpec);
        mergedAnomalyResult.setCollection(anomalyFunctionSpec.getCollection());
        mergedAnomalyResult.setMetric(anomalyFunctionSpec.getTopicMetric());

        String filters = anomalyFunctionSpec.getFilters();
        MetricEntity me = MetricEntity.fromMetric(1.0, anomalyFunctionSpec.getMetricId()).withFilters(ThirdEyeUtils.getFilterSet(filters));
        String metricUrn = me.getUrn();
        mergedAnomalyResult.setMetricUrn(metricUrn);
        MetricTimeSeries metricTimeSeries = getMetricTimeSeries(entry.getKey());

        try {
          anomalyFunction.updateMergedAnomalyInfo(mergedAnomalyResult, metricTimeSeries,
              new DateTime(mergedAnomalyResult.getStartTime()), new DateTime(mergedAnomalyResult.getEndTime()), retrievedAnomalies);
        } catch (Exception e) {
          LOG.error("anomaly function update merged anomaly info error", e);
        }
        mergedAnomalies.add(mergedAnomalyResult);
      }
    }
    return mergedAnomalies;
  }

  /**
   * Get metric time series for a dimension.
   */
  private MetricTimeSeries getMetricTimeSeries(DimensionMap dimension) {
    MetricEntity metricEntity =
        MetricEntity.fromMetric(1.0, anomalyFunction.getSpec().getMetricId(), getFiltersFromDimensionMap(dimension));
    MetricConfigDTO metricConfig =
        this.provider.fetchMetrics(Collections.singleton(metricEntity.getId())).get(metricEntity.getId());

    DataFrame df = DataFrame.builder(COL_TIME + ":LONG", COL_VALUE + ":DOUBLE").build();
    List<Pair<Long, Long>> timeIntervals = anomalyFunction.getDataRangeIntervals(this.startTime, this.endTime);
    for (Pair<Long, Long> startEndInterval : timeIntervals) {
      MetricSlice slice =
          MetricSlice.from(metricEntity.getId(), startEndInterval.getFirst(), startEndInterval.getSecond(),
              metricEntity.getFilters());
      DataFrame currentDf = this.provider.fetchTimeseries(Collections.singleton(slice)).get(slice);
      df = df.append(currentDf);
    }

    MetricTimeSeries metricTimeSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(
        Collections.singletonList(new MetricSpec(metricConfig.getName(), MetricType.DOUBLE))));

    LongSeries timestamps = df.getLongs(COL_TIME);
    for (int i = 0; i < timestamps.size(); i++) {
      metricTimeSeries.set(timestamps.get(i), metricConfig.getName(), df.getDoubles(COL_VALUE).get(i));
    }
    return metricTimeSeries;
  }

  private Multimap<String, String> getFiltersFromDimensionMap(DimensionMap dimensionMap) {
    Multimap<String, String> filter = HashMultimap.create();
    for (Map.Entry<String, String> dimension : dimensionMap.entrySet()) {
      filter.put(dimension.getKey(), dimension.getValue());
    }
    return filter;
  }

  private Map<DimensionMap, List<MergedAnomalyResultDTO>> getAnomaliesByDimension(
      Collection<MergedAnomalyResultDTO> anomalies) {
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

  private long getStartTime(Iterable<MergedAnomalyResultDTO> anomalies) {
    long time = this.startTime;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      time = Math.min(anomaly.getStartTime(), time);
    }
    return time;
  }

  private long getEndTime(Iterable<MergedAnomalyResultDTO> anomalies) {
    long time = this.endTime;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      time = Math.max(anomaly.getEndTime(), time);
    }
    return time;
  }
}
