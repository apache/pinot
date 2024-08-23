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
 * AggInfo is used to represent the aggregation function, and whether it is a partial aggregation or not.
 * Aggregation functions are simply stored as a string, since time-series languages are allowed to implement their own
 * aggregation functions. We don't need the partial flag most likely, but keeping it here for now so we don't forget
 * to account for this.
 */
public class AggInfo {
  private final String _aggFunction;
  // TODO: This flag is unused right now. It might make more sense to have this in the physical plans.
  private final boolean _isPartial;

  @JsonCreator
  public AggInfo(
      @JsonProperty("aggFunction") String aggFunction,
      @JsonProperty("isPartial") boolean isPartial) {
    _aggFunction = aggFunction;
    _isPartial = isPartial;
  }

  public String getAggFunction() {
    return _aggFunction;
  }

  public boolean isPartial() {
    return _isPartial;
  }
}
