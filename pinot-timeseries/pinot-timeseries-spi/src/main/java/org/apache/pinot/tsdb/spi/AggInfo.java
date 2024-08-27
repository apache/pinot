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
package org.apache.pinot.tsdb.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * AggInfo is used to represent the aggregation function. Aggregation functions are simply stored as a string,
 * since time-series languages are allowed to implement their own aggregation functions.
 * TODO: We will likely be adding more parameters to this. One candidate is partial/full aggregation information or
 *   aggregation result type to allow for intermediate result types.
 */
public class AggInfo {
  private final String _aggFunction;

  @JsonCreator
  public AggInfo(
      @JsonProperty("aggFunction") String aggFunction) {
    _aggFunction = aggFunction;
  }

  public String getAggFunction() {
    return _aggFunction;
  }
}
