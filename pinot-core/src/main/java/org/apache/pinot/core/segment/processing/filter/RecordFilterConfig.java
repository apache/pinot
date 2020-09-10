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
package org.apache.pinot.core.segment.processing.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Config for RecordFilter
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RecordFilterConfig {

  private static final RecordFilterFactory.RecordFilterType DEFAULT_RECORD_FILTER_TYPE =
      RecordFilterFactory.RecordFilterType.NO_OP;

  private final RecordFilterFactory.RecordFilterType _recordFilterType;
  private final String _filterFunction;

  @JsonCreator
  private RecordFilterConfig(
      @JsonProperty(value = "recordFilterType", required = true) RecordFilterFactory.RecordFilterType recordFilterType,
      @JsonProperty(value = "filterFunction") String filterFunction) {
    _recordFilterType = recordFilterType;
    _filterFunction = filterFunction;
  }

  /**
   * The type of RecordFilter
   */
  @JsonProperty
  public RecordFilterFactory.RecordFilterType getRecordFilterType() {
    return _recordFilterType;
  }

  /**
   * Filter function to use for filtering out partitions
   */
  @JsonProperty
  public String getFilterFunction() {
    return _filterFunction;
  }

  /**
   * Builder for a RecordFilterConfig
   */
  public static class Builder {
    private RecordFilterFactory.RecordFilterType _recordFilterType = DEFAULT_RECORD_FILTER_TYPE;
    private String _filterFunction;

    public Builder setRecordFilterType(RecordFilterFactory.RecordFilterType recordFilterType) {
      _recordFilterType = recordFilterType;
      return this;
    }

    public Builder setFilterFunction(String filterFunction) {
      _filterFunction = filterFunction;
      return this;
    }

    public RecordFilterConfig build() {
      return new RecordFilterConfig(_recordFilterType, _filterFunction);
    }
  }

  @Override
  public String toString() {
    return "RecordFilterConfig{" + " _recordFilterType=" + _recordFilterType + ", _filterFunction='" + _filterFunction
        + '\'' + '}';
  }
}
