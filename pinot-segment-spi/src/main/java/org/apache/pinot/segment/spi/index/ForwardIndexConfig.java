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

package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;


public class ForwardIndexConfig extends IndexConfig {
  public static final int DEFAULT_RAW_WRITER_VERSION = 2;
  public static final ForwardIndexConfig DISABLED = new ForwardIndexConfig(false, null, null, null);

  @Nullable
  private final ChunkCompressionType _chunkCompressionType;
  private final boolean _deriveNumDocsPerChunk;
  private final int _rawIndexWriterVersion;

  public static final ForwardIndexConfig DEFAULT = new Builder().build();

  @JsonCreator
  public ForwardIndexConfig(@Nullable @JsonProperty("enabled") Boolean enabled,
      @Nullable @JsonProperty("chunkCompressionType") ChunkCompressionType chunkCompressionType,
      @JsonProperty("deriveNumDocsPerChunk") Boolean deriveNumDocsPerChunk,
      @JsonProperty("rawIndexWriterVersion") Integer rawIndexWriterVersion) {
    super(enabled != null && enabled);
    _chunkCompressionType = chunkCompressionType;
    _deriveNumDocsPerChunk = deriveNumDocsPerChunk != null && deriveNumDocsPerChunk;
    _rawIndexWriterVersion = rawIndexWriterVersion == null ? DEFAULT_RAW_WRITER_VERSION : rawIndexWriterVersion;
  }

  @Nullable
  public ChunkCompressionType getChunkCompressionType() {
    return _chunkCompressionType;
  }

  public boolean isDeriveNumDocsPerChunk() {
    return _deriveNumDocsPerChunk;
  }

  public int getRawIndexWriterVersion() {
    return _rawIndexWriterVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ForwardIndexConfig that = (ForwardIndexConfig) o;
    return _deriveNumDocsPerChunk == that._deriveNumDocsPerChunk
        && _rawIndexWriterVersion == that._rawIndexWriterVersion && _chunkCompressionType == that._chunkCompressionType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_chunkCompressionType, _deriveNumDocsPerChunk, _rawIndexWriterVersion);
  }

  public static class Builder {
    @Nullable
    private ChunkCompressionType _chunkCompressionType;
    private boolean _deriveNumDocsPerChunk = false;
    private int _rawIndexWriterVersion = DEFAULT_RAW_WRITER_VERSION;

    public Builder() {
    }

    public Builder withCompressionType(ChunkCompressionType chunkCompressionType) {
      _chunkCompressionType = chunkCompressionType;
      return this;
    }

    public Builder withDeriveNumDocsPerChunk(boolean deriveNumDocsPerChunk) {
      _deriveNumDocsPerChunk = deriveNumDocsPerChunk;
      return this;
    }

    public Builder withRawIndexWriterVersion(int rawIndexWriterVersion) {
      _rawIndexWriterVersion = rawIndexWriterVersion;
      return this;
    }

    public Builder withLegacyProperties(Map<String, Map<String, String>> propertiesByCol, String colName) {
      if (propertiesByCol != null) {
        withLegacyProperties(propertiesByCol.get(colName));
      }
      return this;
    }

    public Builder withLegacyProperties(Map<String, String> properties) {
      String newDerive = properties.get(FieldConfig.DERIVE_NUM_DOCS_PER_CHUNK_RAW_INDEX_KEY);
      if (newDerive != null) {
        withDeriveNumDocsPerChunk(Boolean.parseBoolean(newDerive));
      }
      String newRawIndexVersion = properties.get(FieldConfig.RAW_INDEX_WRITER_VERSION);
      if (newRawIndexVersion != null) {
        withRawIndexWriterVersion(Integer.parseInt(newRawIndexVersion));
      }
      return this;
    }

    public ForwardIndexConfig build() {
      return new ForwardIndexConfig(true, _chunkCompressionType, _deriveNumDocsPerChunk, _rawIndexWriterVersion);
    }
  }

  @Override
  public String toString() {
    try {
      return JsonUtils.objectToString(this);
    } catch (IOException ex) {
      return "{" + "\"chunkCompressionType\":" + _chunkCompressionType
          + ", \"deriveNumDocsPerChunk\":" + _deriveNumDocsPerChunk
          + ", \"rawIndexWriterVersion\":" + _rawIndexWriterVersion + '}';
    }
  }
}
