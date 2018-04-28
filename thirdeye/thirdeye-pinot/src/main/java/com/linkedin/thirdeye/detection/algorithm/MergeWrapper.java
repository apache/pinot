package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.StaticDetectionPipeline;
import com.linkedin.thirdeye.detection.StaticDetectionPipelineData;
import com.linkedin.thirdeye.detection.StaticDetectionPipelineModel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.Predicate;


/**
 * The Merge wrapper.
 */
public class MergeWrapper extends StaticDetectionPipeline {
  private static final String PROP_CLASS_NAMES = "classNames";
  private static final String PROP_PROPERTIES_MAP = "propertiesMap";
  private static final String PROP_TARGET = "target";
  private static final String PROP_TARGET_DEFAULT = "metricUrn";
  private static final String PROP_METRIC_URN = "metricUrn";

  private final List<String> nestedClassNames;
  private final Map<String, Map<String, Object>> nestedPropertiesMap;
  private final String nestedTarget;
  private final String metricUrn;
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

    Preconditions.checkArgument(config.getProperties().containsKey(PROP_CLASS_NAMES), "Missing " + PROP_CLASS_NAMES);
    Preconditions.checkArgument(config.getProperties().containsKey(PROP_PROPERTIES_MAP),
        "Missing " + PROP_PROPERTIES_MAP);
    Preconditions.checkArgument(config.getProperties().containsKey(PROP_METRIC_URN), "Missing " + PROP_METRIC_URN);

    Preconditions.checkNotNull(config.getProperties().get(PROP_CLASS_NAMES));
    Preconditions.checkArgument(config.getProperties().get(PROP_CLASS_NAMES) instanceof List);

    this.metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    this.nestedClassNames = (List<String>) config.getProperties().get(PROP_CLASS_NAMES);
    this.nestedPropertiesMap = MapUtils.getMap(config.getProperties(), PROP_PROPERTIES_MAP);
    this.nestedTarget = MapUtils.getString(config.getProperties(), PROP_TARGET, PROP_TARGET_DEFAULT);
    this.slice = new AnomalySlice().withStart(startTime).withEnd(endTime).withConfigId(config.getId());
  }

  @Override
  public StaticDetectionPipelineModel getModel() {
    return new StaticDetectionPipelineModel().withAnomalySlices(Collections.singletonList(this.slice));
  }

  @Override
  public DetectionPipelineResult run(StaticDetectionPipelineData data) throws Exception {
    List<MergedAnomalyResultDTO> allAnomalies = new ArrayList<>();

    long overallLastTimeStamp = Long.MAX_VALUE;

    for (String className : nestedClassNames) {
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

    List<MergedAnomalyResultDTO> existingAnomalies = new ArrayList<>();
    existingAnomalies.addAll(data.getAnomalies().get(this.slice));

    // TODO: put filters into database query
    CollectionUtils.filter(existingAnomalies, new Predicate() {
      @Override
      public boolean evaluate(Object o) {
        Integer parentCount = ((MergedAnomalyResultDTO) o).getParentCount();
        return parentCount == null || parentCount == 0;
      }
    });
    allAnomalies.addAll(existingAnomalies);

    timeBasedAnomaliesMerging(allAnomalies);

    return new DetectionPipelineResult(allAnomalies, overallLastTimeStamp);
  }

  private void timeBasedAnomaliesMerging(List<MergedAnomalyResultDTO> unmergedAnomalies) {

    if (unmergedAnomalies.isEmpty()) {
      return;
    }

    Collections.sort(unmergedAnomalies, new Comparator<MergedAnomalyResultDTO>() {
      @Override
      public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
        return (int) (o1.getStartTime() - o2.getStartTime());
      }
    });

    MergedAnomalyResultDTO anomalyResultDTO = unmergedAnomalies.get(0);
    for (int i = 1; i < unmergedAnomalies.size(); i++) {
      MergedAnomalyResultDTO currentAnomaly = unmergedAnomalies.get(i);

      if (currentAnomaly.getStartTime() <= anomalyResultDTO.getEndTime()) {
        // If overlap, then merge anomalies
        anomalyResultDTO.setEndTime(Math.max(anomalyResultDTO.getEndTime(), currentAnomaly.getEndTime()));
        currentAnomaly.setParentCount(anomalyResultDTO.getParentCount() + 1);
      } else {
        anomalyResultDTO = currentAnomaly;
      }
    }
  }
}
