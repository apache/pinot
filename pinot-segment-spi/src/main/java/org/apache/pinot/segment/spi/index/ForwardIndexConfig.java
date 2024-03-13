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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
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
  public static final ForwardIndexConfig DISABLED = new ForwardIndexConfig(true, null, null, null, null, null);
  public static final ForwardIndexConfig DEFAULT = new Builder().build();

  @Nullable
  private final CompressionCodec _compressionCodec;
  private final boolean _deriveNumDocsPerChunk;
  private final int _rawIndexWriterVersion;

  @Nullable
  private final ChunkCompressionType _chunkCompressionType;
  @Nullable
  private final DictIdCompressionType _dictIdCompressionType;

  public ForwardIndexConfig(@Nullable Boolean disabled, @Nullable CompressionCodec compressionCodec,
      @Nullable Boolean deriveNumDocsPerChunk, @Nullable Integer rawIndexWriterVersion) {
    super(disabled);
    _deriveNumDocsPerChunk = Boolean.TRUE.equals(deriveNumDocsPerChunk);
    _rawIndexWriterVersion = rawIndexWriterVersion == null ? DEFAULT_RAW_WRITER_VERSION : rawIndexWriterVersion;
    _compressionCodec = compressionCodec;

    if (compressionCodec != null) {
      switch (compressionCodec) {
        case PASS_THROUGH:
        case CLP:
          _chunkCompressionType = ChunkCompressionType.PASS_THROUGH;
          _dictIdCompressionType = null;
          break;
        case SNAPPY:
          _chunkCompressionType = ChunkCompressionType.SNAPPY;
          _dictIdCompressionType = null;
          break;
        case ZSTANDARD:
          _chunkCompressionType = ChunkCompressionType.ZSTANDARD;
          _dictIdCompressionType = null;
          break;
        case LZ4:
          _chunkCompressionType = ChunkCompressionType.LZ4;
          _dictIdCompressionType = null;
          break;
        case MV_ENTRY_DICT:
          _dictIdCompressionType = DictIdCompressionType.MV_ENTRY_DICT;
          _chunkCompressionType = null;
          break;
        default:
          throw new IllegalStateException("Unsupported compression codec: " + compressionCodec);
      }
    } else {
      _dictIdCompressionType = null;
      _chunkCompressionType = null;
    }
  }

  @JsonCreator
  public ForwardIndexConfig(@JsonProperty("disabled") @Nullable Boolean disabled,
      @JsonProperty("compressionCodec") @Nullable CompressionCodec compressionCodec,
      @Deprecated @JsonProperty("chunkCompressionType") @Nullable ChunkCompressionType chunkCompressionType,
      @Deprecated @JsonProperty("dictIdCompressionType") @Nullable DictIdCompressionType dictIdCompressionType,
      @JsonProperty("deriveNumDocsPerChunk") @Nullable Boolean deriveNumDocsPerChunk,
      @JsonProperty("rawIndexWriterVersion") @Nullable Integer rawIndexWriterVersion) {
    this(disabled, getActualCompressionCodec(compressionCodec, chunkCompressionType, dictIdCompressionType),
        deriveNumDocsPerChunk, rawIndexWriterVersion);
  }

  public static CompressionCodec getActualCompressionCodec(@Nullable CompressionCodec compressionCodec,
      @Nullable ChunkCompressionType chunkCompressionType, @Nullable DictIdCompressionType dictIdCompressionType) {
    if (compressionCodec != null) {
      return compressionCodec;
    }
    if (chunkCompressionType != null && dictIdCompressionType != null) {
      throw new IllegalArgumentException("chunkCompressionType and dictIdCompressionType should not be used together");
    }
    if (chunkCompressionType != null) {
      switch (chunkCompressionType) {
        case PASS_THROUGH:
          return CompressionCodec.PASS_THROUGH;
        case SNAPPY:
          return CompressionCodec.SNAPPY;
        case ZSTANDARD:
          return CompressionCodec.ZSTANDARD;
        case LZ4:
          return CompressionCodec.LZ4;
        default:
          throw new IllegalStateException("Unsupported chunk compression type: " + chunkCompressionType);
      }
    } else if (dictIdCompressionType != null) {
      switch (dictIdCompressionType) {
        case MV_ENTRY_DICT:
          return CompressionCodec.MV_ENTRY_DICT;
        default:
          throw new IllegalStateException("Unsupported dictionary compression type: " + dictIdCompressionType);
      }
    } else {
      return null;
    }
  }

  @Nullable
  public CompressionCodec getCompressionCodec() {
    return _compressionCodec;
  }

  public boolean isDeriveNumDocsPerChunk() {
    return _deriveNumDocsPerChunk;
  }

  public int getRawIndexWriterVersion() {
    return _rawIndexWriterVersion;
  }

  @JsonIgnore
  @Nullable
  public ChunkCompressionType getChunkCompressionType() {
    return _chunkCompressionType;
  }

  @JsonIgnore
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
    return _compressionCodec == that._compressionCodec && _deriveNumDocsPerChunk == that._deriveNumDocsPerChunk
        && _rawIndexWriterVersion == that._rawIndexWriterVersion;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _compressionCodec, _deriveNumDocsPerChunk, _rawIndexWriterVersion);
  }

  public static class Builder {
    @Nullable
    private CompressionCodec _compressionCodec;
    private boolean _deriveNumDocsPerChunk = false;
    private int _rawIndexWriterVersion = DEFAULT_RAW_WRITER_VERSION;

    public Builder() {
    }

    public Builder(ForwardIndexConfig other) {
      _compressionCodec = other._compressionCodec;
      _deriveNumDocsPerChunk = other._deriveNumDocsPerChunk;
      _rawIndexWriterVersion = other._rawIndexWriterVersion;
    }

    public Builder withCompressionCodec(CompressionCodec compressionCodec) {
      _compressionCodec = compressionCodec;
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

    @Deprecated
    public Builder withCompressionType(ChunkCompressionType chunkCompressionType) {
      if (chunkCompressionType == null) {
        return this;
      }
      switch (chunkCompressionType) {
        case LZ4:
        case LZ4_LENGTH_PREFIXED:
          _compressionCodec = CompressionCodec.LZ4;
          break;
        case PASS_THROUGH:
          _compressionCodec = CompressionCodec.PASS_THROUGH;
          break;
        case SNAPPY:
          _compressionCodec = CompressionCodec.SNAPPY;
          break;
        case ZSTANDARD:
          _compressionCodec = CompressionCodec.ZSTANDARD;
          break;
        default:
          throw new IllegalArgumentException("Unsupported chunk compression type: " + chunkCompressionType);
      }
      return this;
    }

    @Deprecated
    public Builder withDictIdCompressionType(DictIdCompressionType dictIdCompressionType) {
      if (dictIdCompressionType == null) {
        return this;
      }
      Preconditions.checkArgument(dictIdCompressionType == DictIdCompressionType.MV_ENTRY_DICT,
          "Unsupported dictionary compression type: " + dictIdCompressionType);
      _compressionCodec = CompressionCodec.MV_ENTRY_DICT;
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
      return new ForwardIndexConfig(false, _compressionCodec, _deriveNumDocsPerChunk, _rawIndexWriterVersion);
    }
  }
}
