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
package org.apache.pinot.common.metadata.segment;

import java.util.Map;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;


/// Backend-neutral immutable input for loading published offline segment content. Authoritative object identities,
/// assignment epochs and content authentication belong to the provider and installation protocol, not this loader
/// descriptor. `published` controls whether a CRC mismatch can safely require replacement of an existing local copy.
public final class SegmentLoadMetadata {
  private final String _segmentName;
  private final long _crc;
  private final long _dataCrc;
  private final boolean _useDataCrc;
  private final String _downloadUri;
  @Nullable
  private final String _tier;
  @Nullable
  private final String _crypterName;
  private final long _creationTimeMs;
  private final long _pushTimeMs;
  private final Map<String, String> _customConfigs;
  private final boolean _published;

  public SegmentLoadMetadata(String segmentName, long crc, long dataCrc, boolean useDataCrc, String downloadUri,
      @Nullable String tier, @Nullable String crypterName, long creationTimeMs, long pushTimeMs,
      Map<String, String> customConfigs, boolean published) {
    _segmentName = requireNonNull(segmentName, "segmentName");
    _crc = crc;
    _dataCrc = dataCrc;
    _useDataCrc = useDataCrc;
    _downloadUri = requireNonNull(downloadUri, "downloadUri");
    _tier = tier;
    _crypterName = crypterName;
    _creationTimeMs = creationTimeMs;
    _pushTimeMs = pushTimeMs;
    _customConfigs = Map.copyOf(customConfigs);
    _published = published;
  }

  public String segmentName() {
    return _segmentName;
  }

  public long crc() {
    return _crc;
  }

  public long dataCrc() {
    return _dataCrc;
  }

  public boolean useDataCrc() {
    return _useDataCrc;
  }

  public String downloadUri() {
    return _downloadUri;
  }

  @Nullable
  public String tier() {
    return _tier;
  }

  @Nullable
  public String crypterName() {
    return _crypterName;
  }

  public long creationTimeMs() {
    return _creationTimeMs;
  }

  public long pushTimeMs() {
    return _pushTimeMs;
  }

  public Map<String, String> customConfigs() {
    return _customConfigs;
  }

  public boolean published() {
    return _published;
  }
}
