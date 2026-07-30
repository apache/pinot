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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.annotation.Nullable;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TableSizeInfo {
  /// Version of the server size contribution protocol emitted by this build.
  public static final int CURRENT_METADATA_VERSION = 1;

  private final String _tableName;
  private final long _diskSizeInBytes;
  private final List<SegmentSizeInfo> _segments;
  private final int _metadataVersion;

  public TableSizeInfo(String tableName, long sizeInBytes, List<SegmentSizeInfo> segments) {
    this(tableName, sizeInBytes, segments, 0);
  }

  @JsonCreator
  public TableSizeInfo(@JsonProperty("tableName") String tableName, @JsonProperty("diskSizeInBytes") long sizeInBytes,
      @JsonProperty("segments") List<SegmentSizeInfo> segments,
      @JsonProperty("metadataVersion") @Nullable Integer metadataVersion) {
    _tableName = tableName;
    _diskSizeInBytes = sizeInBytes;
    _segments = segments;
    _metadataVersion = metadataVersion != null ? metadataVersion : 0;
  }

  public String getTableName() {
    return _tableName;
  }

  public long getDiskSizeInBytes() {
    return _diskSizeInBytes;
  }

  public List<SegmentSizeInfo> getSegments() {
    return _segments;
  }

  /// Returns the server size contribution protocol version, or `0` for a legacy response.
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getMetadataVersion() {
    return _metadataVersion;
  }
}
