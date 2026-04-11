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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Table-level compression statistics summary, aggregated from per-column data.
 * Contains total raw and compressed forward index sizes and the overall compression ratio.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CompressionStatsSummary {
  private final long _rawForwardIndexSizePerReplicaInBytes;
  private final long _compressedForwardIndexSizePerReplicaInBytes;
  private final double _compressionRatio;

  @JsonCreator
  public CompressionStatsSummary(
      @JsonProperty("rawForwardIndexSizePerReplicaInBytes") long rawForwardIndexSizePerReplicaInBytes,
      @JsonProperty("compressedForwardIndexSizePerReplicaInBytes") long compressedForwardIndexSizePerReplicaInBytes,
      @JsonProperty("compressionRatio") double compressionRatio) {
    _rawForwardIndexSizePerReplicaInBytes = rawForwardIndexSizePerReplicaInBytes;
    _compressedForwardIndexSizePerReplicaInBytes = compressedForwardIndexSizePerReplicaInBytes;
    _compressionRatio = compressionRatio;
  }

  public long getRawForwardIndexSizePerReplicaInBytes() {
    return _rawForwardIndexSizePerReplicaInBytes;
  }

  public long getCompressedForwardIndexSizePerReplicaInBytes() {
    return _compressedForwardIndexSizePerReplicaInBytes;
  }

  public double getCompressionRatio() {
    return _compressionRatio;
  }
}
