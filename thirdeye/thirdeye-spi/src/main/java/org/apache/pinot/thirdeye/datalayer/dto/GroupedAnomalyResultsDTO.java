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
 *
 */

package org.apache.pinot.thirdeye.datalayer.dto;

import org.apache.pinot.thirdeye.datalayer.pojo.GroupedAnomalyResultsBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * The grouped anomaly results for alerter. Each group of anomalies should be sent through the same email.
 */
public class GroupedAnomalyResultsDTO extends GroupedAnomalyResultsBean {

  private List<MergedAnomalyResultDTO> anomalyResults = new ArrayList<>();

  public List<MergedAnomalyResultDTO> getAnomalyResults() {
    return anomalyResults;
  }

  public void setAnomalyResults(List<MergedAnomalyResultDTO> anomalyResults) {
    this.anomalyResults = anomalyResults;
  }

  @Override
  public long getEndTime() {
    if (anomalyResults == null || anomalyResults.isEmpty()) {
      return 0;
    }
    Collections.sort(anomalyResults, new Comparator<MergedAnomalyResultDTO>() {
      @Override
      public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
        return (int) (o1.getEndTime() - o2.getEndTime());
      }
    });
    return anomalyResults.get(anomalyResults.size() - 1).getEndTime();
  }
}
