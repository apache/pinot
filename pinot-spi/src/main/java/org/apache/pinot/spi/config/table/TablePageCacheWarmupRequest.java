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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.spi.config.BaseJsonConfig;

import java.util.List;

public class TablePageCacheWarmupRequest extends BaseJsonConfig {

  private final List<String> _queries;
  private final List<String> _segments;

  @JsonCreator
  public TablePageCacheWarmupRequest(
      @JsonProperty("queries") List<String> queries,
      @JsonProperty("segments") List<String> segments) {
    _queries = queries;
    _segments = segments;
  }

  @JsonProperty("queries")
  public List<String> getQueries() {
    return _queries;
  }

  @JsonProperty("segments")
  public List<String> getSegments() {
    return _segments;
  }
}
