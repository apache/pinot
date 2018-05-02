package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import java.util.ArrayList;
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
  private static final String PROP_PROPERTIES_MAP = "propertiesMap";
  private static final String PROP_TARGET = "target";
  private static final String PROP_TARGET_DEFAULT = "metricUrn";
  private static final String PROP_METRIC_URN = "metricUrn";
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

  private final Map<String, Map<String, Object>> nestedPropertiesMap;
  private final String nestedTarget;
  private final String metricUrn;
  private final long maxGap; // max time gap for merge
  private final long maxDuration; // max overall duration of merged anomaly
  private final AnomalySlice slice;

  /**
   * Instantiates a new Merge wrapper.
   *
   * @param provider the provider
   * @param config the config
   * @param startTime the start time
   * @param endTime the end time
   */
  public MergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    Preconditions.checkArgument(config.getProperties().containsKey(PROP_PROPERTIES_MAP), "Missing " + PROP_PROPERTIES_MAP);
    Preconditions.checkArgument(config.getProperties().containsKey(PROP_METRIC_URN), "Missing " + PROP_METRIC_URN);

    this.metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    this.nestedPropertiesMap = MapUtils.getMap(config.getProperties(), PROP_PROPERTIES_MAP);
    this.nestedTarget = MapUtils.getString(config.getProperties(), PROP_TARGET, PROP_TARGET_DEFAULT);
    this.maxGap = MapUtils.getLongValue(config.getProperties(), PROP_MAX_GAP, PROP_MAX_GAP_DEFAULT);
    this.maxDuration = MapUtils.getLongValue(config.getProperties(), PROP_MAX_DURATION, PROP_MAX_DURATION_DEFAULT);

    this.slice = new AnomalySlice().withStart(startTime - this.maxGap).withEnd(endTime + this.maxGap).withConfigId(config.getId());
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    List<MergedAnomalyResultDTO> allAnomalies = new ArrayList<>();

    long overallLastTimeStamp = Long.MAX_VALUE;

    for (String className : this.nestedPropertiesMap.keySet()) {
      Map<String, Object> properties = new HashMap<>(this.nestedPropertiesMap.get(className));
      properties.put(this.nestedTarget, metricUrn);

      DetectionConfigDTO nestedConfig = new DetectionConfigDTO();

      nestedConfig.setId(this.config.getId());
      nestedConfig.setName(this.config.getName());
      nestedConfig.setClassName(className);
      nestedConfig.setProperties(properties);

      DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

      DetectionPipelineResult intermediate = pipeline.run();
      overallLastTimeStamp = Math.min(intermediate.getLastTimestamp(), overallLastTimeStamp);
      allAnomalies.addAll(intermediate.getAnomalies());
    }

    allAnomalies.addAll(this.provider.fetchAnomalies(Collections.singleton(this.slice)).get(this.slice));

    return new DetectionPipelineResult(mergeOnTime(allAnomalies), overallLastTimeStamp);
  }

  private List<MergedAnomalyResultDTO> mergeOnTime(List<MergedAnomalyResultDTO> anomalies) {
    // TODO make this an O(n) algorithm if necessary

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
}
