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
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.DictIdCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.IndexConfig;


public class ForwardIndexConfig extends IndexConfig {
  public static final int DEFAULT_RAW_WRITER_VERSION = 2;
  public static final ForwardIndexConfig DISABLED = new ForwardIndexConfig(true, null, null, null, null);
  public static final ForwardIndexConfig DEFAULT = new Builder().build();

  @Nullable
  private final ChunkCompressionType _chunkCompressionType;
  private final boolean _deriveNumDocsPerChunk;
  private final int _rawIndexWriterVersion;

  @Nullable
  private final DictIdCompressionType _dictIdCompressionType;

  @JsonCreator
  public ForwardIndexConfig(@JsonProperty("disabled") @Nullable Boolean disabled,
      @JsonProperty("chunkCompressionType") @Nullable ChunkCompressionType chunkCompressionType,
      @JsonProperty("deriveNumDocsPerChunk") Boolean deriveNumDocsPerChunk,
      @JsonProperty("rawIndexWriterVersion") Integer rawIndexWriterVersion,
      @JsonProperty("dictIdCompressionType") @Nullable DictIdCompressionType dictIdCompressionType) {
    super(disabled);
    _chunkCompressionType = chunkCompressionType;
    _deriveNumDocsPerChunk = deriveNumDocsPerChunk != null && deriveNumDocsPerChunk;
    _rawIndexWriterVersion = rawIndexWriterVersion == null ? DEFAULT_RAW_WRITER_VERSION : rawIndexWriterVersion;
    _dictIdCompressionType = dictIdCompressionType;
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

  @Nullable
  public DictIdCompressionType getDictIdCompressionType() {
    return _dictIdCompressionType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ForwardIndexConfig)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ForwardIndexConfig that = (ForwardIndexConfig) o;
    return _deriveNumDocsPerChunk == that._deriveNumDocsPerChunk
        && _rawIndexWriterVersion == that._rawIndexWriterVersion && _chunkCompressionType == that._chunkCompressionType
        && Objects.equals(_dictIdCompressionType, that._dictIdCompressionType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _chunkCompressionType, _deriveNumDocsPerChunk, _rawIndexWriterVersion,
        _dictIdCompressionType);
  }

  public static class Builder {
    @Nullable
    private ChunkCompressionType _chunkCompressionType;
    private boolean _deriveNumDocsPerChunk = false;
    private int _rawIndexWriterVersion = DEFAULT_RAW_WRITER_VERSION;

    @Nullable
    private DictIdCompressionType _dictIdCompressionType;

    public Builder() {
    }

    public Builder(ForwardIndexConfig other) {
      _chunkCompressionType = other.getChunkCompressionType();
      _deriveNumDocsPerChunk = other._deriveNumDocsPerChunk;
      _rawIndexWriterVersion = other._rawIndexWriterVersion;
      _dictIdCompressionType = other._dictIdCompressionType;
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

    public Builder withDictIdCompressionType(DictIdCompressionType dictIdCompressionType) {
      _dictIdCompressionType = dictIdCompressionType;
      return this;
    }

    public Builder withCompressionCodec(CompressionCodec compressionCodec) {
      if (compressionCodec == null) {
        _chunkCompressionType = null;
        _dictIdCompressionType = null;
        return this;
      }
      switch (compressionCodec) {
        case PASS_THROUGH:
          _chunkCompressionType = ChunkCompressionType.PASS_THROUGH;
          break;
        case SNAPPY:
          _chunkCompressionType = ChunkCompressionType.SNAPPY;
          break;
        case ZSTANDARD:
          _chunkCompressionType = ChunkCompressionType.ZSTANDARD;
          break;
        case LZ4:
          _chunkCompressionType = ChunkCompressionType.LZ4;
          break;
        case CLP:
          _chunkCompressionType = ChunkCompressionType.CLP;
          break;
        case MV_ENTRY_DICT:
          _dictIdCompressionType = DictIdCompressionType.MV_ENTRY_DICT;
          break;
        default:
          throw new IllegalStateException("Unsupported compression codec: " + compressionCodec);
      }
      return this;
    }

    public Builder withLegacyProperties(Map<String, Map<String, String>> propertiesByCol, String colName) {
      if (propertiesByCol != null) {
        Map<String, String> colProps = propertiesByCol.get(colName);
        if (colProps != null) {
          withLegacyProperties(colProps);
        }
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
      return new ForwardIndexConfig(false, _chunkCompressionType, _deriveNumDocsPerChunk, _rawIndexWriterVersion,
          _dictIdCompressionType);
    }
  }
}
