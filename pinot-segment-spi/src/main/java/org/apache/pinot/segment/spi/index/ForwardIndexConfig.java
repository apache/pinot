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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.compression.DictIdCompressionType;
import org.apache.pinot.spi.config.table.CompressionCodec;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.utils.DataSizeUtils;


public class ForwardIndexConfig extends IndexConfig {
  private static int _defaultRawIndexWriterVersion = 4;
  private static String _defaultTargetMaxChunkSize = "1MB";
  private static int _defaultTargetMaxChunkSizeBytes = 1024 * 1024;
  private static int _defaultTargetDocsPerChunk = 1000;

  public static int getDefaultRawWriterVersion() {
    return _defaultRawIndexWriterVersion;
  }

  public static void setDefaultRawIndexWriterVersion(int defaultRawIndexWriterVersion) {
    _defaultRawIndexWriterVersion = defaultRawIndexWriterVersion;
  }

  public static String getDefaultTargetMaxChunkSize() {
    return _defaultTargetMaxChunkSize;
  }

  public static int getDefaultTargetMaxChunkSizeBytes() {
    return _defaultTargetMaxChunkSizeBytes;
  }

  public static void setDefaultTargetMaxChunkSize(String defaultTargetMaxChunkSize) {
    _defaultTargetMaxChunkSize = defaultTargetMaxChunkSize;
    _defaultTargetMaxChunkSizeBytes = (int) DataSizeUtils.toBytes(defaultTargetMaxChunkSize);
  }

  public static int getDefaultTargetDocsPerChunk() {
    return _defaultTargetDocsPerChunk;
  }

  public static void setDefaultTargetDocsPerChunk(int defaultTargetDocsPerChunk) {
    _defaultTargetDocsPerChunk = defaultTargetDocsPerChunk;
  }

  public static ForwardIndexConfig getDefault() {
    return new Builder().build();
  }

  public static ForwardIndexConfig getDisabled() {
    return new ForwardIndexConfig(true, null, null, null, null, null, null, null, null);
  }

  @Nullable
  private final CompressionCodec _compressionCodec;
  private final boolean _deriveNumDocsPerChunk;
  private final int _rawIndexWriterVersion;
  private final String _targetMaxChunkSize;
  private final int _targetMaxChunkSizeBytes;
  private final int _targetDocsPerChunk;

  @Nullable
  private final DictIdCompressionType _dictIdCompressionType;
  @Nullable
  private final Map<String, Object> _configs;

  public ForwardIndexConfig(@Nullable Boolean disabled, @Nullable CompressionCodec compressionCodec,
      @Nullable Boolean deriveNumDocsPerChunk, @Nullable Integer rawIndexWriterVersion,
      @Nullable String targetMaxChunkSize, @Nullable Integer targetDocsPerChunk,
      @Nullable Map<String, Object> configs) {
    super(disabled);
    _compressionCodec = compressionCodec;
    _deriveNumDocsPerChunk = Boolean.TRUE.equals(deriveNumDocsPerChunk);

    _rawIndexWriterVersion = rawIndexWriterVersion == null ? _defaultRawIndexWriterVersion : rawIndexWriterVersion;
    _targetMaxChunkSize = targetMaxChunkSize == null ? _defaultTargetMaxChunkSize : targetMaxChunkSize;
    _targetMaxChunkSizeBytes =
        targetMaxChunkSize == null ? _defaultTargetMaxChunkSizeBytes : (int) DataSizeUtils.toBytes(targetMaxChunkSize);
    _targetDocsPerChunk = targetDocsPerChunk == null ? _defaultTargetDocsPerChunk : targetDocsPerChunk;
    _configs = configs != null ? configs : new HashMap<>();
    if (compressionCodec != null && compressionCodec.equals(CompressionCodec.MV_ENTRY_DICT)) {
      _dictIdCompressionType = DictIdCompressionType.MV_ENTRY_DICT;
    } else {
      _dictIdCompressionType = null;
    }
  }

  @JsonCreator
  public ForwardIndexConfig(@JsonProperty("disabled") @Nullable Boolean disabled,
      @JsonProperty("compressionCodec") @Nullable String compressionCodec,
      @Deprecated @JsonProperty("chunkCompressionType") @Nullable String chunkCompressionType,
      @Deprecated @JsonProperty("dictIdCompressionType") @Nullable DictIdCompressionType dictIdCompressionType,
      @JsonProperty("deriveNumDocsPerChunk") @Nullable Boolean deriveNumDocsPerChunk,
      @JsonProperty("rawIndexWriterVersion") @Nullable Integer rawIndexWriterVersion,
      @JsonProperty("targetMaxChunkSize") @Nullable String targetMaxChunkSize,
      @JsonProperty("targetDocsPerChunk") @Nullable Integer targetDocsPerChunk,
      @JsonProperty("configs") @Nullable Map<String, Object> configs) {
    this(disabled, resolveCompressionCodec(compressionCodec, chunkCompressionType, dictIdCompressionType),
        deriveNumDocsPerChunk, rawIndexWriterVersion, targetMaxChunkSize, targetDocsPerChunk, configs);
  }

  private static CompressionCodec resolveCompressionCodec(@Nullable String compressionCodec,
      @Nullable String chunkCompressionType, @Nullable DictIdCompressionType dictIdCompressionType) {
    if (compressionCodec != null) {
      return CompressionCodec.fromString(compressionCodec);
    }
    if (chunkCompressionType != null) {
      if (dictIdCompressionType != null) {
        throw new IllegalArgumentException(
            "chunkCompressionType and dictIdCompressionType should not be used together");
      }
      return CompressionCodec.fromString(chunkCompressionType);
    }
    if (dictIdCompressionType != null) {
      Preconditions.checkArgument(dictIdCompressionType == DictIdCompressionType.MV_ENTRY_DICT,
          "Unsupported dictionary compression type: " + dictIdCompressionType);
      return CompressionCodec.MV_ENTRY_DICT;
    }
    return null;
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

  public String getTargetMaxChunkSize() {
    return _targetMaxChunkSize;
  }

  public int getTargetDocsPerChunk() {
    return _targetDocsPerChunk;
  }

  @JsonIgnore
  public int getTargetMaxChunkSizeBytes() {
    return _targetMaxChunkSizeBytes;
  }

  @JsonIgnore
  @Nullable
  public DictIdCompressionType getDictIdCompressionType() {
    return _dictIdCompressionType;
  }

  @JsonIgnore
  @Nullable
  public Map<String, Object> getConfigs() {
    return _configs;
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
    return Objects.equals(_compressionCodec, that._compressionCodec)
        && _deriveNumDocsPerChunk == that._deriveNumDocsPerChunk
        && _rawIndexWriterVersion == that._rawIndexWriterVersion
        && Objects.equals(_targetMaxChunkSize, that._targetMaxChunkSize)
        && _targetDocsPerChunk == that._targetDocsPerChunk;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _compressionCodec, _deriveNumDocsPerChunk, _rawIndexWriterVersion,
        _targetMaxChunkSize, _targetDocsPerChunk);
  }

  public static class Builder {
    @Nullable
    private CompressionCodec _compressionCodec;
    private boolean _deriveNumDocsPerChunk = false;
    private int _rawIndexWriterVersion = _defaultRawIndexWriterVersion;
    private String _targetMaxChunkSize = _defaultTargetMaxChunkSize;
    private int _targetDocsPerChunk = _defaultTargetDocsPerChunk;
    private Map<String, Object> _configs = new HashMap<>();

    public Builder() {
    }

    public Builder(ForwardIndexConfig other) {
      _compressionCodec = other._compressionCodec;
      _deriveNumDocsPerChunk = other._deriveNumDocsPerChunk;
      _rawIndexWriterVersion = other._rawIndexWriterVersion;
      _targetMaxChunkSize = other._targetMaxChunkSize;
      _targetDocsPerChunk = other._targetDocsPerChunk;
      _configs = other._configs;
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

    public Builder withTargetMaxChunkSize(int targetMaxChunkSize) {
      _targetMaxChunkSize = DataSizeUtils.fromBytes(targetMaxChunkSize);
      return this;
    }

    public Builder withTargetDocsPerChunk(int targetDocsPerChunk) {
      _targetDocsPerChunk = targetDocsPerChunk;
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
      return new ForwardIndexConfig(false, _compressionCodec, _deriveNumDocsPerChunk,
          _rawIndexWriterVersion, _targetMaxChunkSize, _targetDocsPerChunk, _configs);
    }
  }
}
