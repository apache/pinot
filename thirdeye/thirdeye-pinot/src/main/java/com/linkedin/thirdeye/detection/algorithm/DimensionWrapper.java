package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
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
import org.apache.commons.collections.MapUtils;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class DimensionWrapper extends DetectionPipeline {

  // exploration
  private static final String PROP_METRIC_URN = "metricUrn";

  private static final String PROP_DIMENSIONS = "dimensions";

  private static final String PROP_MIN_VALUE = "minValue";
  private static final double PROP_MIN_VALUE_DEFAULT = Double.NaN;

  private static final String PROP_MIN_CONTRIBUTION = "minContribution";
  private static final double PROP_MIN_CONTRIBUTION_DEFAULT = Double.NaN;

  private static final String PROP_K = "k";
  private static final int PROP_K_DEFAULT = -1;

  // prototyping
  private static final String PROP_PROPERTIES = "properties";

  private static final String PROP_CLASS_NAME = "className";

  private static final String PROP_TARGET = "target";
  private static final String PROP_TARGET_DEFAULT = "metricUrn";

  private final String metricUrn;
  private final List<String> dimensions;
  private final int k;
  private final double minValue;
  private final double minContribution;

  private final String nestedTarget;
  private final String nestedClassName;
  private final Map<String, Object> nestedProperties;

  public DimensionWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    // exploration
    Preconditions.checkArgument(config.getProperties().containsKey(PROP_METRIC_URN), "Missing " + PROP_METRIC_URN);
    Preconditions.checkArgument(config.getProperties().containsKey(PROP_DIMENSIONS), "Missing " + PROP_DIMENSIONS);

    this.metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    this.dimensions = new ArrayList<>((Collection<String>) config.getProperties().get(PROP_DIMENSIONS));
    this.minValue = MapUtils.getDoubleValue(config.getProperties(), PROP_MIN_VALUE, PROP_MIN_VALUE_DEFAULT);
    this.minContribution = MapUtils.getDoubleValue(config.getProperties(), PROP_MIN_CONTRIBUTION, PROP_MIN_CONTRIBUTION_DEFAULT);
    this.k = MapUtils.getIntValue(config.getProperties(), PROP_K, PROP_K_DEFAULT);

    // prototyping
    Preconditions.checkArgument(config.getProperties().containsKey(PROP_CLASS_NAME), "Missing " + PROP_CLASS_NAME);
    Preconditions.checkArgument(config.getProperties().containsKey(PROP_PROPERTIES), "Missing " + PROP_PROPERTIES);

    this.nestedClassName = MapUtils.getString(config.getProperties(), PROP_CLASS_NAME);
    this.nestedProperties = (Map<String, Object>) config.getProperties().get(PROP_PROPERTIES);
    this.nestedTarget = MapUtils.getString(config.getProperties(), PROP_TARGET, PROP_TARGET_DEFAULT);
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    MetricEntity metric = MetricEntity.fromURN(this.metricUrn, 1.0);
    MetricSlice slice = MetricSlice.from(metric.getId(), this.startTime, this.endTime, metric.getFilters());

    DataFrame breakdown = this.provider.fetchAggregates(Collections.singletonList(slice), this.dimensions).get(slice);

    if (breakdown.isEmpty()) {
      return new DetectionPipelineResult(Collections.<MergedAnomalyResultDTO>emptyList(), -1);
    }

    final double total = breakdown.getDoubles(COL_VALUE).sum().fillNull().doubleValue();

    // min value
    if (!Double.isNaN(this.minValue)) {
      breakdown = breakdown.filter(new Series.DoubleConditional() {
        @Override
        public boolean apply(double... values) {
          return values[0] >= DimensionWrapper.this.minValue;
        }
      }, COL_VALUE).dropNull();
    }

    // min contribution
    if (!Double.isNaN(this.minContribution)) {
      breakdown = breakdown.filter(new Series.DoubleConditional() {
        @Override
        public boolean apply(double... values) {
          return values[0] / total >= DimensionWrapper.this.minContribution;
        }
      }, COL_VALUE).dropNull();
    }

    // top k
    if (this.k > 0) {
      breakdown = breakdown.sortedBy(COL_VALUE).tail(this.k).reverse();
    }

    if (breakdown.isEmpty()) {
      return new DetectionPipelineResult(Collections.<MergedAnomalyResultDTO>emptyList(), -1);
    }

    long lastTimestamp = Long.MAX_VALUE;
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();

    for (int i = 0; i < breakdown.size(); i++) {
      Multimap<String, String> filters = ArrayListMultimap.create(metric.getFilters());
      for (String dimName : this.dimensions) {
        filters.put(dimName, breakdown.getString(dimName, i));
      }

      DetectionPipelineResult intermediate = this.runNested(metric.withFilters(filters));
      lastTimestamp = Math.min(lastTimestamp, intermediate.getLastTimestamp());
      anomalies.addAll(intermediate.getAnomalies());
    }

    return new DetectionPipelineResult(anomalies, lastTimestamp);
  }

  private DetectionPipelineResult runNested(MetricEntity metric) throws Exception {
    Map<String, Object> properties = new HashMap<>(this.nestedProperties);
    properties.put(this.nestedTarget, metric.getUrn());

    DetectionConfigDTO nestedConfig = new DetectionConfigDTO();
    nestedConfig.setId(this.config.getId());
    nestedConfig.setName(this.config.getName());
    nestedConfig.setClassName(this.nestedClassName);
    nestedConfig.setProperties(properties);

    DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

    return pipeline.run();
  }
}
