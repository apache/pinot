package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections.MapUtils;


/**
 * The Merge wrapper. Supports time-based merging of anomalies along the same set of dimensions.
 * Forward scan only, greedy clustering. Does not merge clusters that converge.
 */
public class MergeWrapper extends DetectionPipeline {
  private static final String PROP_NESTED = "nested";
  private static final String PROP_CLASS_NAME = "className";

  private static final Comparator<MergedAnomalyResultDTO> COMPARATOR = new Comparator<MergedAnomalyResultDTO>() {
    @Override
    public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
      // earlier
      int res = Long.compare(o1.getStartTime(), o2.getStartTime());
      if (res != 0) return res;

      // pre-existing
      if (o1.getId() == null && o2.getId() != null) return 1;
      if (o1.getId() != null && o2.getId() == null) return -1;

      // more children
      return -1 * Integer.compare(o1.getChildren().size(), o2.getChildren().size());
    }
  };

  private final List<Map<String, Object>> nestedProperties;
  private final long maxGap; // max time gap for merge
  private final long maxDuration; // max overall duration of merged anomaly
  private final AnomalySlice slice;

  /**
   * Instantiates a new merge wrapper.
   *
   * @param provider the provider
   * @param config the config
   * @param startTime the start time
   * @param endTime the end time
   */
  public MergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    this.maxGap = MapUtils.getLongValue(config.getProperties(), "maxGap", 0);
    this.maxDuration = MapUtils.getLongValue(config.getProperties(), "maxDuration", Long.MAX_VALUE);
    this.slice = new AnomalySlice().withStart(startTime).withEnd(endTime).withConfigId(config.getId());
    this.nestedProperties = config.getProperties().containsKey(PROP_NESTED) ?
        new ArrayList<>((Collection<Map<String, Object>>) config.getProperties().get(PROP_NESTED)) :
        Collections.<Map<String,Object>>emptyList();
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    // generate anomalies
    List<MergedAnomalyResultDTO> generated = new ArrayList<>();

    for (Map<String, Object> properties : this.nestedProperties) {
      DetectionConfigDTO nestedConfig = new DetectionConfigDTO();

      Preconditions.checkArgument(properties.containsKey(PROP_CLASS_NAME), "Nested missing " + PROP_CLASS_NAME);

      nestedConfig.setId(this.config.getId());
      nestedConfig.setName(this.config.getName());
      nestedConfig.setProperties(properties);

      DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

      DetectionPipelineResult intermediate = pipeline.run();

      generated.addAll(intermediate.getAnomalies());
    }

    // retrieve anomalies
    AnomalySlice effectiveSlice = this.slice
        .withStart(this.getStartTime(generated) - this.maxGap - 1)
        .withEnd(this.getEndTime(generated) + this.maxGap + 1);

    List<MergedAnomalyResultDTO> retrieved = new ArrayList<>();
    retrieved.addAll(this.provider.fetchAnomalies(Collections.singleton(effectiveSlice)).get(effectiveSlice));

    // merge
    List<MergedAnomalyResultDTO> all = new ArrayList<>();
    all.addAll(retrieved);
    all.addAll(generated);

    return new DetectionPipelineResult(this.merge(all));
  }

  protected List<MergedAnomalyResultDTO> merge(Collection<MergedAnomalyResultDTO> anomalies) {
    List<MergedAnomalyResultDTO> input = new ArrayList<>(anomalies);
    Collections.sort(input, COMPARATOR);

    List<MergedAnomalyResultDTO> output = new ArrayList<>();

    Map<AnomalyKey, MergedAnomalyResultDTO> parents = new HashMap<>();
    for (MergedAnomalyResultDTO anomaly : input) {
      if (anomaly.isChild()) {
        continue;
      }

      AnomalyKey key = AnomalyKey.from(anomaly);
      MergedAnomalyResultDTO parent = parents.get(key);

      if (parent == null || anomaly.getStartTime() - parent.getEndTime() > this.maxGap) {
        // new parent
        parents.put(key, anomaly);
        output.add(anomaly);

      } else if (anomaly.getEndTime() - parent.getStartTime() <= this.maxDuration) {
        // merge, update existing
        parent.setEndTime(anomaly.getEndTime());

      } else if (parent.getEndTime() >= anomaly.getStartTime()) {
        // merge of overlapping, update existing, truncated new anomaly
        long truncationTimestamp = Math.max(parent.getEndTime(), parent.getStartTime() + this.maxDuration);

        parent.setEndTime(truncationTimestamp);

        anomaly.setStartTime(Math.max(truncationTimestamp, anomaly.getStartTime()));
        parents.put(key, anomaly);
        output.add(anomaly);

      } else {
        // default to new parent if merge not possible
        parents.put(key, anomaly);
        output.add(anomaly);

      }
    }

    return output;
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

  private static class AnomalyKey {
    final String metric;
    final String collection;
    final DimensionMap dimensions;

    public AnomalyKey(String metric, String collection, DimensionMap dimensions) {
      this.metric = metric;
      this.collection = collection;
      this.dimensions = dimensions;
    }

    public static AnomalyKey from(MergedAnomalyResultDTO anomaly) {
      return new AnomalyKey(anomaly.getMetric(), anomaly.getCollection(), anomaly.getDimensions());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AnomalyKey that = (AnomalyKey) o;
      return Objects.equals(metric, that.metric) && Objects.equals(collection, that.collection) && Objects.equals(
          dimensions, that.dimensions);
    }

    @Override
    public int hashCode() {

      return Objects.hash(metric, collection, dimensions);
    }
  }
}
