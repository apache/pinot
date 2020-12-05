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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;


/**
 * Information regarding the consumer of a segment
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentConsumerInfo {
  private final String _segmentName;
  private final String _consumerState;
  private final Map<String, String> _partitionToOffsetMap;

  public SegmentConsumerInfo(@JsonProperty("segmentName") String segmentName,
      @JsonProperty("consumerState") String consumerState,
      @JsonProperty("partitionToOffsetMap") Map<String, String> partitionToOffsetMap) {
    _segmentName = segmentName;
    _consumerState = consumerState;
    _partitionToOffsetMap = partitionToOffsetMap;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getConsumerState() {
    return _consumerState;
  }

  public Map<String, String> getPartitionToOffsetMap() {
    return _partitionToOffsetMap;
  }
}
