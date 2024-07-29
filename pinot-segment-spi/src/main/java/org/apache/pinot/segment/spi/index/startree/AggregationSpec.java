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
package org.apache.pinot.segment.spi.index.startree;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;


public class AggregationSpec {
  public static final CompressionCodec DEFAULT_COMPRESSION_CODEC = CompressionCodec.PASS_THROUGH;
  public static final AggregationSpec DEFAULT = new AggregationSpec(null, null, null, null, null);

  private final CompressionCodec _compressionCodec;
  private final boolean _deriveNumDocsPerChunk;
  private final int _indexVersion;
  private final int _targetMaxChunkSizeBytes;
  private final int _targetDocsPerChunk;

  public AggregationSpec(StarTreeAggregationConfig aggregationConfig) {
    this(aggregationConfig.getCompressionCodec(), aggregationConfig.getDeriveNumDocsPerChunk(),
        aggregationConfig.getIndexVersion(), aggregationConfig.getTargetMaxChunkSizeBytes(),
        aggregationConfig.getTargetDocsPerChunk());
  }

  public AggregationSpec(@Nullable CompressionCodec compressionCodec, @Nullable Boolean deriveNumDocsPerChunk,
      @Nullable Integer indexVersion, @Nullable Integer targetMaxChunkSizeBytes, @Nullable Integer targetDocsPerChunk) {
    _indexVersion = indexVersion != null ? indexVersion : ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION;
    _compressionCodec = compressionCodec != null ? compressionCodec : DEFAULT_COMPRESSION_CODEC;
    _deriveNumDocsPerChunk = deriveNumDocsPerChunk != null ? deriveNumDocsPerChunk : false;
    _targetMaxChunkSizeBytes = targetMaxChunkSizeBytes != null ? targetMaxChunkSizeBytes
        : ForwardIndexConfig.DEFAULT_TARGET_MAX_CHUNK_SIZE_BYTES;
    _targetDocsPerChunk =
        targetDocsPerChunk != null ? targetDocsPerChunk : ForwardIndexConfig.DEFAULT_TARGET_DOCS_PER_CHUNK;
  }

  public CompressionCodec getCompressionCodec() {
    return _compressionCodec;
  }

  public boolean isDeriveNumDocsPerChunk() {
    return _deriveNumDocsPerChunk;
  }

  public int getIndexVersion() {
    return _indexVersion;
  }

  public int getTargetMaxChunkSizeBytes() {
    return _targetMaxChunkSizeBytes;
  }

  public int getTargetDocsPerChunk() {
    return _targetDocsPerChunk;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AggregationSpec)) {
      return false;
    }
    AggregationSpec that = (AggregationSpec) o;
    return _deriveNumDocsPerChunk == that._deriveNumDocsPerChunk && _indexVersion == that._indexVersion
        && _targetMaxChunkSizeBytes == that._targetMaxChunkSizeBytes && _targetDocsPerChunk == that._targetDocsPerChunk
        && _compressionCodec == that._compressionCodec;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_compressionCodec, _deriveNumDocsPerChunk, _indexVersion, _targetMaxChunkSizeBytes,
        _targetDocsPerChunk);
  }

  @Override
  public String toString() {
    //@formatter:off
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("compressionCodec", _compressionCodec)
        .append("deriveNumDocsPerChunk", _deriveNumDocsPerChunk)
        .append("indexVersion", _indexVersion)
        .append("targetMaxChunkSizeBytes", _targetMaxChunkSizeBytes)
        .append("targetDocsPerChunk", _targetDocsPerChunk)
        .toString();
    //@formatter:on
  }
}
