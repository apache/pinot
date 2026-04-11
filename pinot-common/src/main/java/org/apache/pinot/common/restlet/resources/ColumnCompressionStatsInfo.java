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
import javax.annotation.Nullable;


/**
 * Per-column forward index compression statistics for a segment.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnCompressionStatsInfo {
  private final long _rawForwardIndexSizeBytes;
  private final long _compressedForwardIndexSizeBytes;
  private final String _compressionCodec;

  @JsonCreator
  public ColumnCompressionStatsInfo(
      @JsonProperty("rawForwardIndexSizeBytes") long rawForwardIndexSizeBytes,
      @JsonProperty("compressedForwardIndexSizeBytes") long compressedForwardIndexSizeBytes,
      @JsonProperty("compressionCodec") @Nullable String compressionCodec) {
    _rawForwardIndexSizeBytes = rawForwardIndexSizeBytes;
    _compressedForwardIndexSizeBytes = compressedForwardIndexSizeBytes;
    _compressionCodec = compressionCodec;
  }

  public long getRawForwardIndexSizeBytes() {
    return _rawForwardIndexSizeBytes;
  }

  public long getCompressedForwardIndexSizeBytes() {
    return _compressedForwardIndexSizeBytes;
  }

  @Nullable
  public String getCompressionCodec() {
    return _compressionCodec;
  }
}
