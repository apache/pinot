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
@JsonPropertyOrder({"orderBy", "groupByKeys", "aggregationResults"})
public class GroupByOrderByResults {
  private List<String> _orderBy;
  private List<String[]> _groupByKeys;
  private List<Serializable[]> _aggregationResults;

  @JsonCreator
  public GroupByOrderByResults(@JsonProperty("orderBy") List<String> orderBy,
      @JsonProperty("groupByKeys") List<String[]> groupByKeys,
      @JsonProperty("aggregationResults") List<Serializable[]> aggregationResults) {
    _orderBy = orderBy;
    _groupByKeys = groupByKeys;
    _aggregationResults = aggregationResults;
  }

  @JsonProperty("orderBy")
  public List<String> getOrderBy() {
    return _orderBy;
  }

  @JsonProperty("orderBy")
  public void setOrderBy(List<String> orderBy) {
    _orderBy = orderBy;
  }

  @JsonProperty("groupByKeys")
  public List<String[]> getGroupByKeys() {
    return _groupByKeys;
  }

  @JsonProperty("groupByKeys")
  public void setGroupByKeys(List<String[]> groupByKeys) {
    _groupByKeys = groupByKeys;
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
