/**
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
package org.apache.pinot.common.response.broker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.io.Serializable;
import java.util.List;


/**
 * This class holds the results of group by order by query which is set into the {@link BrokerResponseNative}
 */
@JsonPropertyOrder({"groupBy", "aggregationResults"})
public class OrderedGroupByResults {
  private List<String[]> _groupBy;
  private List<Serializable[]> _aggregationResults;

  @JsonCreator
  public OrderedGroupByResults(@JsonProperty("groupBy") List<String[]> groupBy,
      @JsonProperty("aggregationResults") List<Serializable[]> aggregationResults) {
    _groupBy = groupBy;
    _aggregationResults = aggregationResults;
  }

  @JsonProperty("groupBy")
  public List<String[]> getGroupBy() {
    return _groupBy;
  }

  @JsonProperty("groupBy")
  public void setGroupBy(List<String[]> groupBy) {
    _groupBy = groupBy;
  }

  @JsonProperty("aggregationResults")
  public List<Serializable[]> getAggregationResults() {
    return _aggregationResults;
  }

  @JsonProperty("aggregationResults")
  public void setAggregationResults(List<Serializable[]> aggregationResults) {
    _aggregationResults = aggregationResults;
  }
}
