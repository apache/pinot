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

import com.google.common.base.Preconditions;
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
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.anomaly.AnomalyType;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.PredictionResult;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Merge wrapper. Supports time-based merging of anomalies along the same set of dimensions.
 * Forward scan only, greedy clustering. Does not merge clusters that converge.
 */
public class MergeWrapper extends DetectionPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MergeWrapper.class);

  private static final String PROP_NESTED = "nested";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_MERGE_KEY = "mergeKey";
  private static final String PROP_GROUP_KEY = "groupKey";
  private static final int NUMBER_OF_SPLITED_ANOMALIES_LIMIT = 1000;
  protected static final String PROP_DETECTOR_COMPONENT_NAME = "detectorComponentName";

  protected static final Comparator<MergedAnomalyResultDTO> COMPARATOR = new Comparator<MergedAnomalyResultDTO>() {
    @Override
    public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
      // first order anomalies from earliest startTime to latest
      int res = Long.compare(o1.getStartTime(), o2.getStartTime());
      if (res != 0) return res;

      // order anomalies from earliest createdTime to latest, if startTime are the same
      res = Long.compare(o1.getCreatedTime(), o2.getCreatedTime());
      if (res != 0) return res;

      // pre-existing
      if (o1.getId() == null && o2.getId() != null) return 1;
      if (o1.getId() != null && o2.getId() == null) return -1;

      // more children
      return -1 * Integer.compare(o1.getChildren().size(), o2.getChildren().size());
    }
  };

  protected final List<Map<String, Object>> nestedProperties;
  protected final long maxGap; // max time gap for merge
  protected final long maxDuration; // max overall duration of merged anomaly
  protected final AnomalySlice slice;

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

    this.maxGap = MapUtils.getLongValue(config.getProperties(), "maxGap", TimeUnit.HOURS.toMillis(2));
    this.maxDuration = MapUtils.getLongValue(config.getProperties(), "maxDuration", TimeUnit.DAYS.toMillis(7));
    Preconditions.checkArgument(this.maxDuration > 0 , "Max duration must be a positive number");
    this.slice = new AnomalySlice().withStart(startTime).withEnd(endTime);
    this.nestedProperties = new ArrayList<>();
    List<Map<String, Object>> nested = ConfigUtils.getList(config.getProperties().get(PROP_NESTED));
    for (Map<String, Object> properties : nested) {
      this.nestedProperties.add(new HashMap<>(properties));
    }
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    Map<String, Object> diagnostics = new HashMap<>();

    // generated anomalies
    List<MergedAnomalyResultDTO> generated = new ArrayList<>();
    // predicted time series results
    List<PredictionResult> predictionResults = new ArrayList<>();
    // evaluation for the predictions
    List<EvaluationDTO> evaluations = new ArrayList<>();
    int i = 0;
    Set<Long> lastTimeStamps = new HashSet<>();
    for (Map<String, Object> properties : this.nestedProperties) {
      DetectionPipelineResult intermediate = this.runNested(properties, this.startTime, this.endTime);
      lastTimeStamps.add(intermediate.getLastTimestamp());
      generated.addAll(intermediate.getAnomalies());
      predictionResults.addAll(intermediate.getPredictions());
      evaluations.addAll(intermediate.getEvaluations());
      diagnostics.put(String.valueOf(i), intermediate.getDiagnostics());

      i++;
    }

    // merge
    Set<MergedAnomalyResultDTO> all = new HashSet<>();
    all.addAll(retrieveAnomaliesFromDatabase(generated));
    all.addAll(generated);

    return new DetectionPipelineResult(this.merge(all), DetectionUtils.consolidateNestedLastTimeStamps(lastTimeStamps),
        predictionResults, evaluations).setDiagnostics(diagnostics);
  }

  protected List<MergedAnomalyResultDTO> retrieveAnomaliesFromDatabase(List<MergedAnomalyResultDTO> generated) {
    AnomalySlice effectiveSlice = this.slice
        .withDetectionId(this.config.getId())
        .withStart(this.getStartTime(generated) - this.maxGap - 1)
        .withEnd(this.getEndTime(generated) + this.maxGap + 1);

    return new ArrayList<>(this.provider.fetchAnomalies(Collections.singleton(effectiveSlice)).get(effectiveSlice));
  }

  private boolean isExistingAnomaly(MergedAnomalyResultDTO anomaly) {
    return anomaly.getId() != null;
  }

  // Merge new anomalies into existing anomalies. Return the anomalies that need to update or add.
  // If it is existing anomaly and not updated then it is not returned.
  protected List<MergedAnomalyResultDTO> merge(Collection<MergedAnomalyResultDTO> anomalies) {
    List<MergedAnomalyResultDTO> inputs = new ArrayList<>(anomalies);
    Collections.sort(inputs, COMPARATOR);

    // stores all the existing anomalies that need to modified
    Set<MergedAnomalyResultDTO> modifiedExistingAnomalies = new HashSet<>();
    Set<MergedAnomalyResultDTO> retainedNewAnomalies = new HashSet<>();

    Map<AnomalyKey, MergedAnomalyResultDTO> parents = new HashMap<>();
    for (MergedAnomalyResultDTO anomaly : inputs) {
      AnomalyKey key = AnomalyKey.from(anomaly);
      MergedAnomalyResultDTO parent = parents.get(key);

      if (parent == null || anomaly.getStartTime() - parent.getEndTime() > this.maxGap) {
        // no parent, too far away to merge
        //      parent |-------------|
        //                                  anomaly |---------------|
        //
        parents.put(key, anomaly);
        if (!isExistingAnomaly(anomaly)) {
          retainedNewAnomalies.add(anomaly);
        }
      } else if (anomaly.getEndTime() <= parent.getEndTime() || anomaly.getEndTime() - parent.getStartTime() <= this.maxDuration) {
        // fully cover
        //      parent |-------------------|
        //              anomaly |-------------|
        // or mergeable
        //      parent |-------------------|
        //                      anomaly |-------------|
        // or small gap
        //      parent |-------------------|
        //                                 anomaly |-------------|
        //
        // merge new anomaly to existing anomaly
        if (isExistingAnomaly(parent)) {
          // parent (existing) |---------------------|
          // anomaly (new)          |-------------------|
          parent.setEndTime(Math.max(parent.getEndTime(), anomaly.getEndTime()));
          ThirdEyeUtils.mergeAnomalyProperties(parent.getProperties(), anomaly.getProperties());
          mergeChildren(parent, anomaly);
          modifiedExistingAnomalies.add(parent);
        } else if (isExistingAnomaly(anomaly)) {
          // parent (new)       |---------------------|
          // anomaly (existing)      |-------------------|
          anomaly.setStartTime(Math.min(parent.getStartTime(), anomaly.getStartTime()));
          anomaly.setEndTime(Math.max(parent.getEndTime(), anomaly.getEndTime()));
          ThirdEyeUtils.mergeAnomalyProperties(anomaly.getProperties(), parent.getProperties());
          mergeChildren(anomaly, parent);
          modifiedExistingAnomalies.add(anomaly);
          retainedNewAnomalies.remove(parent);
          parents.put(key, anomaly);
        } else {
          // parent (new)       |---------------------|
          // anomaly (new)             |-------------------|
          parent.setEndTime(Math.max(parent.getEndTime(), anomaly.getEndTime()));
          ThirdEyeUtils.mergeAnomalyProperties(parent.getProperties(), anomaly.getProperties());
          mergeChildren(parent, anomaly);
        }
      } else if (parent.getEndTime() >= anomaly.getStartTime()) {
        // mergeable but exceeds maxDuration, then truncate
        //      parent |---------------------|
        //                        anomaly |------------------------|
        //
        long truncationTimestamp = Math.max(parent.getEndTime(), parent.getStartTime() + this.maxDuration);

        parent.setEndTime(truncationTimestamp);
        anomaly.setStartTime(Math.max(truncationTimestamp, anomaly.getStartTime()));
        anomaly.setEndTime(Math.max(truncationTimestamp, anomaly.getEndTime()));

        parents.put(key, anomaly);
        if (!isExistingAnomaly(anomaly)) {
          retainedNewAnomalies.add(anomaly);
        }
        if (isExistingAnomaly(parent)) {
          modifiedExistingAnomalies.add(parent);
        }
      } else {
        // default to new parent if merge not possible
        parents.put(key, anomaly);
        if (!isExistingAnomaly(anomaly)) {
          retainedNewAnomalies.add(anomaly);
        }
      }
    }

    modifiedExistingAnomalies.addAll(retainedNewAnomalies);
    Collection<MergedAnomalyResultDTO> splitAnomalies
        = enforceMaxDuration(new ArrayList<>(modifiedExistingAnomalies));
    return new ArrayList<>(splitAnomalies);
  }

  /**
   * Make the children of both anomalies as the merged children of the parent anomaly.
   * However, if the anomaly id clashes then we consider the anomaly from the latest detection.
   *
   * Why? Since we start merging anomalies bottom-up, the child anomalies detected by the latest
   * detection will merge with the historical anomalies and the updated baseline, score etc.
   * would already be computed and stored in the anomaly.
   */
  private void mergeChildren(MergedAnomalyResultDTO parentAnomaly, MergedAnomalyResultDTO anomalyToMerge) {
    Set<MergedAnomalyResultDTO> mergedChildren = new HashSet<>();
    Map<Long, MergedAnomalyResultDTO> tempMergeBuffer = new HashMap<>();

    Set<MergedAnomalyResultDTO> parentChildren = parentAnomaly.getChildren();
    Set<MergedAnomalyResultDTO> anomalyChildren = anomalyToMerge.getChildren();

    updateMergeChildAnomalies(tempMergeBuffer, mergedChildren, parentChildren);

    // If anomaly id clash, replace it with the child of anomalyToMerge
    updateMergeChildAnomalies(tempMergeBuffer, mergedChildren, anomalyChildren);

    mergedChildren.addAll(tempMergeBuffer.values());
    parentAnomaly.setChildren(mergedChildren);
  }

  // Helper method to merge child anomalies by updating a map
  private void updateMergeChildAnomalies(Map<Long, MergedAnomalyResultDTO> tempMergeBuffer, Set<MergedAnomalyResultDTO> mergedChildren, Set<MergedAnomalyResultDTO> anomalies) {
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (anomaly.getId() != null) {
        tempMergeBuffer.put(anomaly.getId(), anomaly);
      } else {
        mergedChildren.add(anomaly);
      }
    }
  }

  /*
    Make sure that the anomalies generated from detector is shorter than maxDuration. Otherwise, split the anomaly
    Do not split anomaly if it is existing anomaly.
   */
  private Collection<MergedAnomalyResultDTO> enforceMaxDuration(Collection<MergedAnomalyResultDTO> anomalies) {
    Set<MergedAnomalyResultDTO> result = new HashSet<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (isExistingAnomaly(anomaly)) {
        result.add(anomaly);
      } else {
        if (anomaly.getEndTime() - anomaly.getStartTime() > this.maxDuration) {
          result.addAll(splitAnomaly(anomaly, this.maxDuration));
        } else {
          result.add(anomaly);
        }
      }
    }
    return result;
  }

  /*
    Split the anomaly into multiple consecutive anomalies with duration less than the max allowed duration.
  */
  private Collection<MergedAnomalyResultDTO> splitAnomaly(MergedAnomalyResultDTO anomaly, long maxDuration) {
    int anomalyCountAfterSplit = (int) Math.ceil((anomaly.getEndTime() - anomaly.getStartTime()) / (double) maxDuration);
    if (anomalyCountAfterSplit > NUMBER_OF_SPLITED_ANOMALIES_LIMIT) {
      // if the number of anomalies after split is more than the limit, don't split
      LOG.warn("Exceeded max number of split count. maxDuration = {}, anomaly split count = {}", maxDuration, anomalyCountAfterSplit);
      return Collections.singleton(anomaly);
    }
    Set<MergedAnomalyResultDTO> result = new HashSet<>();

    long nextStartTime = anomaly.getStartTime();
    for (int i = 0; i < anomalyCountAfterSplit; i++) {
      MergedAnomalyResultDTO splitedAnomaly = copyAnomalyInfo(anomaly, new MergedAnomalyResultDTO());
      splitedAnomaly.setStartTime(nextStartTime);
      splitedAnomaly.setEndTime(Math.min(nextStartTime + maxDuration, anomaly.getEndTime()));
      nextStartTime = splitedAnomaly.getEndTime();
      result.add(splitedAnomaly);
    }
    return result;
  }


  protected long getStartTime(Iterable<MergedAnomalyResultDTO> anomalies) {
    long time = this.startTime;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      time = Math.min(anomaly.getStartTime(), time);
    }
    return time;
  }

  protected long getEndTime(Iterable<MergedAnomalyResultDTO> anomalies) {
    long time = this.endTime;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      time = Math.max(anomaly.getEndTime(), time);
    }
    return time;
  }

  protected List<String> getDetectionCompNames(Iterable<MergedAnomalyResultDTO> anomalies) {
    List<String> detCompNames = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (anomaly.getProperties() != null && anomaly.getProperties().containsKey(PROP_DETECTOR_COMPONENT_NAME)) {
        detCompNames.add(anomaly.getProperties().get(PROP_DETECTOR_COMPONENT_NAME));
      }
    }
    return detCompNames;
  }

  protected MergedAnomalyResultDTO copyAnomalyInfo(MergedAnomalyResultDTO from, MergedAnomalyResultDTO to) {
    to.setStartTime(from.getStartTime());
    to.setEndTime(from.getEndTime());
    to.setMetric(from.getMetric());
    to.setMetricUrn(from.getMetricUrn());
    to.setCollection(from.getCollection());
    to.setDimensions(from.getDimensions());
    to.setDetectionConfigId(from.getDetectionConfigId());
    to.setAnomalyResultSource(from.getAnomalyResultSource());
    to.setAvgBaselineVal(from.getAvgBaselineVal());
    to.setAvgCurrentVal(from.getAvgCurrentVal());
    to.setFeedback(from.getFeedback());
    to.setAnomalyFeedbackId(from.getAnomalyFeedbackId());
    to.setScore(from.getScore());
    to.setWeight(from.getWeight());
    to.setProperties(from.getProperties());
    to.setType(from.getType());
    to.setSeverityLabel(from.getSeverityLabel());
    return to;
  }

  protected static class AnomalyKey {
    final String metric;
    final String collection;
    final DimensionMap dimensions;
    final String mergeKey;
    final String componentKey;
    final AnomalyType type;

    public AnomalyKey(String metric, String collection, DimensionMap dimensions, String mergeKey, String componentKey,
        AnomalyType type) {
      this.metric = metric;
      this.collection = collection;
      this.dimensions = dimensions;
      this.mergeKey = mergeKey;
      this.componentKey = componentKey;
      this.type = type;
    }

    public static AnomalyKey from(MergedAnomalyResultDTO anomaly) {
      // Anomalies having the same mergeKey or groupKey should be merged
      String mergeKey = "";
      if (anomaly.getProperties().containsKey(PROP_MERGE_KEY)) {
        mergeKey = anomaly.getProperties().get(PROP_MERGE_KEY);
      } else if (anomaly.getProperties().containsKey(PROP_GROUP_KEY)) {
        mergeKey = anomaly.getProperties().get(PROP_GROUP_KEY);
      }

      String componentKey = "";
      if (anomaly.getProperties().containsKey(PROP_DETECTOR_COMPONENT_NAME)) {
        componentKey = anomaly.getProperties().get(PROP_DETECTOR_COMPONENT_NAME);
      }

      return new AnomalyKey(anomaly.getMetric(), anomaly.getCollection(), anomaly.getDimensions(), mergeKey, componentKey, anomaly.getType());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AnomalyKey)) {
        return false;
      }
      AnomalyKey that = (AnomalyKey) o;
      return Objects.equals(metric, that.metric) && Objects.equals(collection, that.collection) && Objects.equals(
          dimensions, that.dimensions) && Objects.equals(mergeKey, that.mergeKey) && Objects.equals(componentKey,
          that.componentKey) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(metric, collection, dimensions, mergeKey, componentKey, type);
    }
  }
}
