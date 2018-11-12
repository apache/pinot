/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.spi.model.AnomalySlice;
import com.linkedin.thirdeye.detection.ConfigUtils;
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
  private static final String PROP_MERGE_KEY = "mergeKey";

  protected static final Comparator<MergedAnomalyResultDTO> COMPARATOR = new Comparator<MergedAnomalyResultDTO>() {
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

  protected final List<Map<String, Object>> nestedProperties;
  protected final long maxGap; // max time gap for merge
  protected final long maxDuration; // max overall duration of merged anomaly
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
    this.slice = new AnomalySlice().withStart(startTime).withEnd(endTime);
    this.nestedProperties = ConfigUtils.getList(config.getProperties().get(PROP_NESTED));
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    Map<String, Object> diagnostics = new HashMap<>();

    // generate anomalies
    List<MergedAnomalyResultDTO> generated = new ArrayList<>();

    int i = 0;
    for (Map<String, Object> properties : this.nestedProperties) {
      DetectionConfigDTO nestedConfig = new DetectionConfigDTO();

      Preconditions.checkArgument(properties.containsKey(PROP_CLASS_NAME), "Nested missing " + PROP_CLASS_NAME);

      nestedConfig.setId(this.config.getId());
      nestedConfig.setName(this.config.getName());
      nestedConfig.setProperties(properties);
      nestedConfig.setComponents(this.config.getComponents());

      DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

      DetectionPipelineResult intermediate = pipeline.run();

      generated.addAll(intermediate.getAnomalies());
      diagnostics.put(String.valueOf(i), intermediate.getDiagnostics());

      i++;
    }

    // retrieve anomalies
    AnomalySlice effectiveSlice = this.slice
        .withStart(this.getStartTime(generated) - this.maxGap - 1)
        .withEnd(this.getEndTime(generated) + this.maxGap + 1);

    List<MergedAnomalyResultDTO> retrieved = new ArrayList<>();
    retrieved.addAll(this.provider.fetchAnomalies(Collections.singleton(effectiveSlice), this.config.getId()).get(effectiveSlice));

    // merge
    List<MergedAnomalyResultDTO> all = new ArrayList<>();
    all.addAll(retrieved);
    all.addAll(generated);

    return new DetectionPipelineResult(this.merge(all)).setDiagnostics(diagnostics);
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
        // no parent, too far away
        parents.put(key, anomaly);
        output.add(anomaly);

      } else if (anomaly.getEndTime() <= parent.getEndTime() || anomaly.getEndTime() - parent.getStartTime() <= this.maxDuration) {
        // fully merge into existing
        parent.setEndTime(Math.max(parent.getEndTime(), anomaly.getEndTime()));

      } else if (parent.getEndTime() >= anomaly.getStartTime()) {
        // partially merge, truncate new
        long truncationTimestamp = Math.max(parent.getEndTime(), parent.getStartTime() + this.maxDuration);

        parent.setEndTime(truncationTimestamp);
        anomaly.setStartTime(Math.max(truncationTimestamp, anomaly.getStartTime()));
        anomaly.setEndTime(Math.max(truncationTimestamp, anomaly.getEndTime()));

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

  protected static class AnomalyKey {
    final String metric;
    final String collection;
    final DimensionMap dimensions;
    final String mergeKey;

    public AnomalyKey(String metric, String collection, DimensionMap dimensions, String mergeKey) {
      this.metric = metric;
      this.collection = collection;
      this.dimensions = dimensions;
      this.mergeKey = mergeKey;
    }

    public static AnomalyKey from(MergedAnomalyResultDTO anomaly) {
      return new AnomalyKey(anomaly.getMetric(), anomaly.getCollection(), anomaly.getDimensions(), anomaly.getProperties().get(PROP_MERGE_KEY));
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
          dimensions, that.dimensions) && Objects.equals(mergeKey, that.mergeKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(metric, collection, dimensions, mergeKey);
    }
  }
}
