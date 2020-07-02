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

package org.apache.pinot.thirdeye.detection.wrapper;

import com.google.common.collect.Collections2;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.algorithm.MergeWrapper;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;


/**
 * The Data Quality Merge Wrapper. This merge wrapper is specifically designed keeping the sla anomalies in mind.
 * We might need to revisit this when more data quality rules are added. Fundamentally, the data sla anomalies are never
 * merged as we want to keep re-notifying users if the sla is missed after every detection. This merger will ensure no
 * duplicate sla anomalies are created if the detection runs more frequently and will serve as a placeholder for future
 * merging logic.
 */
public class DataQualityMergeWrapper extends MergeWrapper {
  private static final String PROP_GROUP_KEY = "groupKey";

  public DataQualityMergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);
  }

  @Override
  protected List<MergedAnomalyResultDTO> retrieveAnomaliesFromDatabase(List<MergedAnomalyResultDTO> generated) {
    List<MergedAnomalyResultDTO> retrieved = super.retrieveAnomaliesFromDatabase(generated);

    return new ArrayList<>(Collections2.filter(retrieved,
        anomaly -> !anomaly.isChild() &&
            anomaly.getAnomalyResultSource().equals(AnomalyResultSource.DATA_QUALITY_DETECTION)
    ));
  }

  @Override
  protected List<MergedAnomalyResultDTO> merge(Collection<MergedAnomalyResultDTO> anomalies) {
    List<MergedAnomalyResultDTO> input = new ArrayList<>(anomalies);
    input.sort(MergeWrapper.COMPARATOR);

    List<MergedAnomalyResultDTO> output = new ArrayList<>();
    Map<AnomalyKey, MergedAnomalyResultDTO> parents = new HashMap<>();
    for (MergedAnomalyResultDTO anomaly : input) {
      if (anomaly.isChild()) {
        continue;
      }

      // Prevent merging of grouped anomalies
      String groupKey = "";
      if (anomaly.getProperties().containsKey(PROP_GROUP_KEY)) {
        groupKey = anomaly.getProperties().get(PROP_GROUP_KEY);
      }
      AnomalyKey key = new AnomalyKey(anomaly.getMetric(), anomaly.getCollection(), anomaly.getDimensions(), groupKey,
          "", anomaly.getType());

      MergedAnomalyResultDTO parent = parents.get(key);
      if (parent == null) {
        parents.put(key, anomaly);
        output.add(anomaly);
      } else if (anomaly.getEndTime() <= parent.getEndTime()) {
        // Ignore as an sla anomaly already exists - we do not want a duplicate.
        // This can happen if the detection is setup to run more frequently than
        // the dataset granularity
      } else {
        // save the anomaly
        parents.put(key, anomaly);
        output.add(anomaly);
      }
    }

    return output;
  }
}
