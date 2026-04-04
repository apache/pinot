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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.codec.ChunkCodecPipeline;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.DictIdCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
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
    return new ForwardIndexConfig(true, null, null, null, null, null, null, null, null, null);
  }

  @Nullable
  private final CompressionCodec _compressionCodec;
  @Nullable
  private final ChunkCodecPipeline _codecPipeline;
  private final boolean _deriveNumDocsPerChunk;
  private final int _rawIndexWriterVersion;
  private final String _targetMaxChunkSize;
  private final int _targetMaxChunkSizeBytes;
  private final int _targetDocsPerChunk;

  @Nullable
  private final ChunkCompressionType _chunkCompressionType;
  @Nullable
  private final DictIdCompressionType _dictIdCompressionType;
  @Nullable
  private final Map<String, Object> _configs;

  public ForwardIndexConfig(@Nullable Boolean disabled, @Nullable CompressionCodec compressionCodec,
      @Nullable Boolean deriveNumDocsPerChunk, @Nullable Integer rawIndexWriterVersion,
      @Nullable String targetMaxChunkSize, @Nullable Integer targetDocsPerChunk,
      @Nullable Map<String, Object> configs) {
    this(disabled, compressionCodec, null, deriveNumDocsPerChunk, rawIndexWriterVersion, targetMaxChunkSize,
        targetDocsPerChunk, configs);
  }

  /**
   * Primary constructor with codec pipeline support.
   *
   * <p>When {@code codecPipeline} is null and {@code compressionCodec} is a standard RAW compression codec
   * (i.e., not CLP/MV_ENTRY_DICT), the pipeline is automatically derived from the compression codec. This
   * ensures all downstream code can operate through a single pipeline-based code path.</p>
   */
  public ForwardIndexConfig(@Nullable Boolean disabled, @Nullable CompressionCodec compressionCodec,
      @Nullable ChunkCodecPipeline codecPipeline, @Nullable Boolean deriveNumDocsPerChunk,
      @Nullable Integer rawIndexWriterVersion, @Nullable String targetMaxChunkSize,
      @Nullable Integer targetDocsPerChunk, @Nullable Map<String, Object> configs) {
    super(disabled);
    _compressionCodec = compressionCodec;
    _deriveNumDocsPerChunk = Boolean.TRUE.equals(deriveNumDocsPerChunk);

    _rawIndexWriterVersion = rawIndexWriterVersion == null ? _defaultRawIndexWriterVersion : rawIndexWriterVersion;
    _targetMaxChunkSize = targetMaxChunkSize == null ? _defaultTargetMaxChunkSize : targetMaxChunkSize;
    _targetMaxChunkSizeBytes =
        targetMaxChunkSize == null ? _defaultTargetMaxChunkSizeBytes : (int) DataSizeUtils.toBytes(targetMaxChunkSize);
    _targetDocsPerChunk = targetDocsPerChunk == null ? _defaultTargetDocsPerChunk : targetDocsPerChunk;
    _configs = configs != null ? configs : new HashMap<>();
    if (compressionCodec != null) {
      switch (compressionCodec) {
        case PASS_THROUGH:
        case CLP:
        case CLPV2:
        case CLPV2_ZSTD:
        case CLPV2_LZ4:
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
        case GZIP:
          _chunkCompressionType = ChunkCompressionType.GZIP;
          _dictIdCompressionType = null;
          break;
        case DELTA:
          _chunkCompressionType = ChunkCompressionType.DELTA;
          _dictIdCompressionType = null;
          break;
        case DELTADELTA:
          _chunkCompressionType = ChunkCompressionType.DELTADELTA;
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

    // Validate that compressionCodec and codecPipeline are not both set.
    Preconditions.checkArgument(compressionCodec == null || codecPipeline == null,
        "'compressionCodec' and 'codecPipeline' cannot both be set. "
            + "Use 'codecPipeline' for new configs; 'compressionCodec' is deprecated.");

    // Auto-derive codec pipeline from compressionCodec when not explicitly set.
    // This ensures all RAW forward index code paths operate through a unified pipeline.
    // CLP codecs are excluded because they use completely separate forward index creators/readers.
    // MV_ENTRY_DICT is excluded because it operates on dictionary-encoded columns, not RAW.
    // Legacy DELTA/DELTADELTA are mapped to DELTA_LZ4/DOUBLE_DELTA_LZ4 single-stage compressor pipelines
    // to preserve byte-compatible reads of existing segments.
    if (codecPipeline != null) {
      _codecPipeline = codecPipeline;
    } else if (_chunkCompressionType != null && !isCLPCodec(compressionCodec)) {
      _codecPipeline = ChunkCodecPipeline.fromCompressionType(_chunkCompressionType);
    } else {
      _codecPipeline = null;
    }
  }

  /**
   * Returns {@code true} if the given compression codec is a CLP variant.
   * CLP codecs use completely separate forward index creators/readers and cannot be
   * represented as a standard codec pipeline.
   */
  private static boolean isCLPCodec(@Nullable CompressionCodec codec) {
    return codec == CompressionCodec.CLP || codec == CompressionCodec.CLPV2
        || codec == CompressionCodec.CLPV2_ZSTD || codec == CompressionCodec.CLPV2_LZ4;
  }

  /**
   * Backward-compatible constructor without codecPipeline parameter. Retained for binary
   * compatibility with existing compiled code.
   */
  public ForwardIndexConfig(@Nullable Boolean disabled, @Nullable CompressionCodec compressionCodec,
      @Nullable ChunkCompressionType chunkCompressionType, @Nullable DictIdCompressionType dictIdCompressionType,
      @Nullable Boolean deriveNumDocsPerChunk, @Nullable Integer rawIndexWriterVersion,
      @Nullable String targetMaxChunkSize, @Nullable Integer targetDocsPerChunk,
      @Nullable Map<String, Object> configs) {
    this(disabled, compressionCodec, chunkCompressionType, dictIdCompressionType, deriveNumDocsPerChunk,
        rawIndexWriterVersion, targetMaxChunkSize, targetDocsPerChunk, null, configs);
  }

  @JsonCreator
  public ForwardIndexConfig(@JsonProperty("disabled") @Nullable Boolean disabled,
      @JsonProperty("compressionCodec") @Nullable CompressionCodec compressionCodec,
      @Deprecated @JsonProperty("chunkCompressionType") @Nullable ChunkCompressionType chunkCompressionType,
      @Deprecated @JsonProperty("dictIdCompressionType") @Nullable DictIdCompressionType dictIdCompressionType,
      @JsonProperty("deriveNumDocsPerChunk") @Nullable Boolean deriveNumDocsPerChunk,
      @JsonProperty("rawIndexWriterVersion") @Nullable Integer rawIndexWriterVersion,
      @JsonProperty("targetMaxChunkSize") @Nullable String targetMaxChunkSize,
      @JsonProperty("targetDocsPerChunk") @Nullable Integer targetDocsPerChunk,
      @JsonProperty("codecPipeline") @Nullable List<String> codecPipelineNames,
      @JsonProperty("configs") @Nullable Map<String, Object> configs) {
    this(disabled, getActualCompressionCodec(compressionCodec, chunkCompressionType, dictIdCompressionType),
        validateAndParseCodecPipeline(codecPipelineNames),
        deriveNumDocsPerChunk, rawIndexWriterVersion, targetMaxChunkSize, targetDocsPerChunk, configs);
  }

  @Nullable
  private static ChunkCodecPipeline validateAndParseCodecPipeline(@Nullable List<String> codecPipelineNames) {
    if (codecPipelineNames == null) {
      return null;
    }
    Preconditions.checkArgument(!codecPipelineNames.isEmpty(), "'codecPipeline' must be non-empty if specified");
    return ChunkCodecPipeline.fromNames(codecPipelineNames);
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
        case DELTA:
          return CompressionCodec.DELTA;
        case DELTADELTA:
          return CompressionCodec.DELTADELTA;
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

  /**
   * @deprecated Use {@link #getCodecPipeline()} instead. The codec pipeline is now the canonical
   *     representation for RAW forward index compression. This getter is retained for JSON
   *     serialization backward compatibility.
   */
  @Deprecated
  @Nullable
  public CompressionCodec getCompressionCodec() {
    return _compressionCodec;
  }

  /**
   * Returns the codec pipeline for this forward index. For all RAW forward indexes (except CLP
   * codecs), this is always non-null — auto-derived from {@code compressionCodec} when not
   * explicitly configured.
   *
   * <p>Legacy compound codecs are mapped to single-stage compressor pipelines:
   * {@code compressionCodec: DELTA} → {@code [DELTA_LZ4]},
   * {@code compressionCodec: DELTADELTA} → {@code [DOUBLE_DELTA_LZ4]}.
   * These preserve byte-compatible reads of existing segments. For the pure delta transform,
   * use an explicit pipeline like {@code ["DELTA", "ZSTANDARD"]}.</p>
   *
   * <p>Returns {@code null} only for CLP codecs, MV_ENTRY_DICT, or when no compression
   * is configured.</p>
   */
  @JsonIgnore
  @Nullable
  public ChunkCodecPipeline getCodecPipeline() {
    return _codecPipeline;
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
  public ChunkCompressionType getChunkCompressionType() {
    return _chunkCompressionType;
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
    return _compressionCodec == that._compressionCodec && Objects.equals(_codecPipeline, that._codecPipeline)
        && _deriveNumDocsPerChunk == that._deriveNumDocsPerChunk
        && _rawIndexWriterVersion == that._rawIndexWriterVersion && Objects.equals(_targetMaxChunkSize,
        that._targetMaxChunkSize) && _targetDocsPerChunk == that._targetDocsPerChunk;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _compressionCodec, _codecPipeline, _deriveNumDocsPerChunk,
        _rawIndexWriterVersion, _targetMaxChunkSize, _targetDocsPerChunk);
  }

  public static class Builder {
    @Nullable
    private CompressionCodec _compressionCodec;
    @Nullable
    private ChunkCodecPipeline _codecPipeline;
    private boolean _deriveNumDocsPerChunk = false;
    private int _rawIndexWriterVersion = _defaultRawIndexWriterVersion;
    private String _targetMaxChunkSize = _defaultTargetMaxChunkSize;
    private int _targetDocsPerChunk = _defaultTargetDocsPerChunk;
    private Map<String, Object> _configs = new HashMap<>();

    public Builder() {
    }

    public Builder(ForwardIndexConfig other) {
      _compressionCodec = other._compressionCodec;
      _codecPipeline = other._codecPipeline;
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

    public Builder withCodecPipeline(ChunkCodecPipeline codecPipeline) {
      _codecPipeline = codecPipeline;
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
      return new ForwardIndexConfig(false, _compressionCodec, _codecPipeline, _deriveNumDocsPerChunk,
          _rawIndexWriterVersion, _targetMaxChunkSize, _targetDocsPerChunk, _configs);
    }
  }
}
