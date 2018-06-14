package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;


/**
 * The Merge wrapper. Supports time-based merging of anomalies along the same set of dimensions.
 * Forward scan only, greedy clustering. Does not merge clusters that converge.
 */
public class MergeWrapper extends DetectionPipeline {
  private static final String PROP_NESTED = "nested";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_MAX_GAP = "maxGap";
  private static final long PROP_MAX_GAP_DEFAULT = 0;
  private static final String PROP_MAX_DURATION = "maxDuration";
  private static final long PROP_MAX_DURATION_DEFAULT = Long.MAX_VALUE;

  private static final Comparator<MergedAnomalyResultDTO> COMPARATOR = new Comparator<MergedAnomalyResultDTO>() {
    @Override
    public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
      int res = Long.compare(o1.getStartTime(), o2.getStartTime());
      if (res != 0) return res;
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

    this.maxGap = MapUtils.getLongValue(config.getProperties(), PROP_MAX_GAP, PROP_MAX_GAP_DEFAULT);
    this.maxDuration = MapUtils.getLongValue(config.getProperties(), PROP_MAX_DURATION, PROP_MAX_DURATION_DEFAULT);
    this.slice = new AnomalySlice().withStart(startTime - this.maxGap).withEnd(endTime + this.maxGap).withConfigId(config.getId());
    this.nestedProperties = config.getProperties().containsKey(PROP_NESTED) ?
        new ArrayList<>((Collection<Map<String, Object>>) config.getProperties().get(PROP_NESTED)) :
        Collections.<Map<String,Object>>emptyList();
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    List<MergedAnomalyResultDTO> allAnomalies = new ArrayList<>(
        this.provider.fetchAnomalies(Collections.singleton(this.slice)).get(this.slice));

    for (Map<String, Object> properties : this.nestedProperties) {
      DetectionConfigDTO nestedConfig = new DetectionConfigDTO();

      Preconditions.checkArgument(properties.containsKey(PROP_CLASS_NAME), "Nested missing " + PROP_CLASS_NAME);

      nestedConfig.setId(this.config.getId());
      nestedConfig.setName(this.config.getName());
      nestedConfig.setProperties(properties);

      DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

      DetectionPipelineResult intermediate = pipeline.run();

      allAnomalies.addAll(intermediate.getAnomalies());
    }

    return new DetectionPipelineResult(mergeOnTime(allAnomalies));
  }

  private List<MergedAnomalyResultDTO> mergeOnTime(Collection<MergedAnomalyResultDTO> anomalies) {
    // TODO make this an O(n) algorithm if necessary

    anomalies = deduplicate(anomalies);

    List<MergedAnomalyResultDTO> candidates = new ArrayList<>(
        Collections2.filter(anomalies, new Predicate<MergedAnomalyResultDTO>() {
          @Override
          public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
            return mergedAnomalyResultDTO != null
                && !mergedAnomalyResultDTO.isChild()
                && mergedAnomalyResultDTO.getChildren().isEmpty();
          }
        }));

    List<MergedAnomalyResultDTO> parents = new ArrayList<>(
        Collections2.filter(anomalies, new Predicate<MergedAnomalyResultDTO>() {
          @Override
          public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
            return mergedAnomalyResultDTO != null
                && !mergedAnomalyResultDTO.isChild()
                && !mergedAnomalyResultDTO.getChildren().isEmpty();
          }
        }));

    Collections.sort(candidates, COMPARATOR);
    Collections.sort(parents, COMPARATOR);

    for (MergedAnomalyResultDTO candidate : candidates) {
      MergedAnomalyResultDTO parent = this.findParent(parents, candidate);

      if (parent != null) {
        if (parent.getChildren().isEmpty()) {
          // create new umbrella anomaly via copy
          parents.remove(parent);
          parent.setChild(true);

          Set<MergedAnomalyResultDTO> children = new HashSet<>();
          children.add(parent);

          parent = copy(parent);
          parent.setChildren(children);

          parents.add(parent);
        }

        // existing umbrella anomaly
        long newStart = Math.min(candidate.getStartTime(), parent.getStartTime());
        long newEnd = Math.max(candidate.getEndTime(), parent.getEndTime());

        parent.setStartTime(newStart);
        parent.setEndTime(newEnd);
        parent.getChildren().add(candidate);

        candidate.setChild(true);

      } else {
        // potential umbrella anomaly
        parents.add(candidate);
      }
    }

    Set<MergedAnomalyResultDTO> output = new HashSet<>();
    output.addAll(candidates);
    output.addAll(parents);

    return new ArrayList<>(output);
  }

  private MergedAnomalyResultDTO findParent(List<MergedAnomalyResultDTO> parents, MergedAnomalyResultDTO candidate) {
    for (MergedAnomalyResultDTO parent : parents) {
      if (!Objects.equals(candidate.getMetric(), parent.getMetric())) {
        continue;
      }

      if (!Objects.equals(candidate.getCollection(), parent.getCollection())) {
        continue;
      }

      if (!Objects.equals(candidate.getDimensions(), parent.getDimensions())) {
        continue;
      }

      if (candidate.getStartTime() > parent.getEndTime() + this.maxGap) {
        continue;
      }

      if (candidate.getEndTime() < parent.getStartTime() - this.maxGap) {
        continue;
      }

      long newStart = Math.min(candidate.getStartTime(), parent.getStartTime());
      long newEnd = Math.max(candidate.getEndTime(), parent.getEndTime());

      if (newEnd - newStart > this.maxDuration) {
        continue;
      }

      return parent;
    }

    return null;
  }

  private static MergedAnomalyResultDTO copy(MergedAnomalyResultDTO anomaly) {
    MergedAnomalyResultDTO newAnomaly = new MergedAnomalyResultDTO();
    newAnomaly.setDetectionConfigId(anomaly.getDetectionConfigId());
    newAnomaly.setMetric(anomaly.getMetric());
    newAnomaly.setCollection(anomaly.getCollection());
    newAnomaly.setStartTime(anomaly.getStartTime());
    newAnomaly.setEndTime(anomaly.getEndTime());
    newAnomaly.setDimensions(anomaly.getDimensions());
    return newAnomaly;
  }

  private static Collection<MergedAnomalyResultDTO> deduplicate(Collection<MergedAnomalyResultDTO> anomalies) {
    Map<AnomalyMetaData, MergedAnomalyResultDTO> output = new HashMap<>();

    // assumes that existing anomalies show up before new ones
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      AnomalyMetaData meta = AnomalyMetaData.from(anomaly);
      if (!output.containsKey(meta)) {
        output.put(meta, anomaly);
      }
    }

    return output.values();
  }

  private static class AnomalyMetaData {
    final String metric;
    final String collection;
    final DimensionMap dimensions;
    final long startTime;
    final long endTime;

    public static AnomalyMetaData from(MergedAnomalyResultDTO anomaly) {
      return new AnomalyMetaData(
          anomaly.getMetric(),
          anomaly.getCollection(),
          anomaly.getDimensions(),
          anomaly.getStartTime(),
          anomaly.getEndTime()
      );
    }

    public AnomalyMetaData(String metric, String collection, DimensionMap dimensions, long startTime, long endTime) {
      this.metric = metric;
      this.collection = collection;
      this.dimensions = dimensions;
      this.startTime = startTime;
      this.endTime = endTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AnomalyMetaData metaData = (AnomalyMetaData) o;
      return startTime == metaData.startTime && endTime == metaData.endTime && Objects.equals(metric, metaData.metric)
          && Objects.equals(collection, metaData.collection) && Objects.equals(dimensions, metaData.dimensions);
    }

    @Override
    public int hashCode() {

      return Objects.hash(metric, collection, dimensions, startTime, endTime);
    }
  }
}
