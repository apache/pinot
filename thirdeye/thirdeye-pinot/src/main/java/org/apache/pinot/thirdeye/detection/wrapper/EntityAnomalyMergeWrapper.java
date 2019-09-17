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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;


/**
 * Merges the entity anomaly in the anomaly hierarchy with the corresponding entity anomaly in DB
 * referred to by the {@value #PROP_DETECTOR_COMPONENT_NAME}
 *
 * While merging two such entity anomalies (old, new), it merges the new anomaly into the old one.
 * 1. Merges the duration of the anomaly
 * 2. Keeping the properties of old, it concatenates by overriding the properties of new in old
 * 3. Keeping the child anomalies of old, it concatenates by overriding the children of new based
 *    on anomaly ID
 *
 * We "concatenate by overriding" because the grouping logic and scores computed in the new anomaly
 * already take into account the complete anomaly duration including the historical period. Note
 * that the anomalies detected at the detection layer is merged with historical anomalies
 * {@link BaselineFillingMergeWrapper} and then passed to the grouper.
 *
 * TODO:
 * Splitting an entity anomaly is not supported at this stage. Avoid setting MAX_DURATION for this merger.
 */
public class EntityAnomalyMergeWrapper extends BaselineFillingMergeWrapper {

  public EntityAnomalyMergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);
  }

  @Override
  protected List<MergedAnomalyResultDTO> retrieveAnomaliesFromDatabase(List<MergedAnomalyResultDTO> generated) {
    AnomalySlice effectiveSlice = this.slice
        .withStart(this.getStartTime(generated) - this.maxGap - 1)
        .withEnd(this.getEndTime(generated) + this.maxGap + 1)
        .withDetectionCompNames(this.getDetectionCompNames(generated));

    Collection<MergedAnomalyResultDTO> anomalies = this.provider.fetchAnomalies(Collections.singleton(effectiveSlice), this.config.getId()).get(effectiveSlice);

    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if(anomaly.getId() != null) this.existingAnomalies.put(anomaly.getId(), copyAnomalyInfo(anomaly, new MergedAnomalyResultDTO()));
    }

    return new ArrayList<>(anomalies);
  }


  @Override
  protected List<MergedAnomalyResultDTO> merge(Collection<MergedAnomalyResultDTO> anomalies) {
    return super.merge(anomalies);
  }
}
