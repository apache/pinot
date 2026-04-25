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
import org.apache.pinot.segment.spi.codec.CodecSpecParser;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.DictIdCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
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

  public static ForwardIndexConfig getDefault(EncodingType encodingType) {
    return new Builder(encodingType).build();
  }

  public static ForwardIndexConfig getDisabled() {
    return new Builder(EncodingType.DICTIONARY).withDisabled(true).build();
  }

  /// Writer version used for transform codec-pipeline forward indexes.
  /// Versions 1-6 are assigned to existing fixed-byte and var-byte chunk formats;
  /// version 7 is the first version that embeds a canonical codec spec in the header.
  ///
  /// **This integer is a frozen on-disk identifier.** Once a release ships with this
  /// constant, the value 7 is permanently bound to the codec-pipeline format defined by
  /// `FixedByteChunkForwardIndexWriterV7`. It must never be reassigned to a different
  /// format; any new format must take a new (unused) version number.
  public static final int CODEC_PIPELINE_WRITER_VERSION = 7;

  private final EncodingType _encodingType;
  @Nullable
  private final CompressionCodec _compressionCodec;
  @Nullable
  private final String _codecSpec;
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
    this(disabled, EncodingType.DICTIONARY, compressionCodec, null, deriveNumDocsPerChunk, rawIndexWriterVersion,
        targetMaxChunkSize, targetDocsPerChunk, configs);
  }

  public ForwardIndexConfig(@Nullable Boolean disabled, @Nullable CompressionCodec compressionCodec,
      @Nullable String codecSpec, @Nullable Boolean deriveNumDocsPerChunk, @Nullable Integer rawIndexWriterVersion,
      @Nullable String targetMaxChunkSize, @Nullable Integer targetDocsPerChunk,
      @Nullable Map<String, Object> configs) {
    this(disabled, EncodingType.DICTIONARY, compressionCodec, codecSpec, deriveNumDocsPerChunk,
        rawIndexWriterVersion, targetMaxChunkSize, targetDocsPerChunk, configs);
  }

  private ForwardIndexConfig(@Nullable Boolean disabled, @Nullable EncodingType encodingType,
      @Nullable CompressionCodec compressionCodec, @Nullable String codecSpec,
      @Nullable Boolean deriveNumDocsPerChunk, @Nullable Integer rawIndexWriterVersion,
      @Nullable String targetMaxChunkSize, @Nullable Integer targetDocsPerChunk,
      @Nullable Map<String, Object> configs) {
    super(disabled);
    // Backward-compat for legacy JSON that lacks `encodingType`: default to DICTIONARY (matches the historical
    // implicit behavior where ForwardIndexConfig had no encoding distinction). Programmatic callers must use
    // Builder(EncodingType) and pass an explicit value, typically from FieldConfig.getEncodingType().
    _encodingType = encodingType == null ? EncodingType.DICTIONARY : encodingType;
    if (compressionCodec != null && codecSpec != null) {
      throw new IllegalArgumentException("compressionCodec and codecSpec are mutually exclusive");
    }
    _compressionCodec = compressionCodec;
    // Parser-canonicalize the codecSpec so syntactic variations (case, whitespace, redundant CODEC wrapper)
    // compare equal. Full canonicalization (e.g. "ZSTD" → "ZSTD(3)") requires the runtime executor with a
    // CodecContext (data type), which ForwardIndexConfig does not have — that level of canonicalization is
    // applied by ForwardIndexHandler.shouldRewriteRawForwardIndex when comparing against the on-disk spec.
    _codecSpec = codecSpec == null ? null : CodecSpecParser.parse(codecSpec).toDslString();
    _deriveNumDocsPerChunk = Boolean.TRUE.equals(deriveNumDocsPerChunk);

    // Version 7 is reserved for the transform codec-pipeline path; setting it without a codecSpec would create
    // a config that the legacy creator factory cannot honor and fails late and confusingly at segment build.
    // Reject early. Compression-only codecSpec values use existing raw writers and should keep the normal raw writer
    // version unless callers explicitly override it.
    if (codecSpec == null && rawIndexWriterVersion != null
        && rawIndexWriterVersion == CODEC_PIPELINE_WRITER_VERSION) {
      throw new IllegalArgumentException(
          "rawIndexWriterVersion=" + CODEC_PIPELINE_WRITER_VERSION + " is reserved for the codec-pipeline path"
              + " and requires a non-null codecSpec.");
    }
    _rawIndexWriterVersion = rawIndexWriterVersion == null ? _defaultRawIndexWriterVersion : rawIndexWriterVersion;
    _targetMaxChunkSize = targetMaxChunkSize == null ? _defaultTargetMaxChunkSize : targetMaxChunkSize;
    _targetMaxChunkSizeBytes =
        targetMaxChunkSize == null ? _defaultTargetMaxChunkSizeBytes : (int) DataSizeUtils.toBytes(targetMaxChunkSize);
    _targetDocsPerChunk = targetDocsPerChunk == null ? _defaultTargetDocsPerChunk : targetDocsPerChunk;
    _configs = configs != null ? configs : new HashMap<>();
    if (_compressionCodec != null) {
      switch (_compressionCodec) {
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
          throw new IllegalStateException("Unsupported compression codec: " + _compressionCodec);
      }
    } else {
      _dictIdCompressionType = null;
      _chunkCompressionType = null;
    }
  }

  @JsonCreator
  public ForwardIndexConfig(@JsonProperty("disabled") @Nullable Boolean disabled,
      @JsonProperty("encodingType") @Nullable EncodingType encodingType,
      @JsonProperty("compressionCodec") @Nullable CompressionCodec compressionCodec,
      @Deprecated @JsonProperty("chunkCompressionType") @Nullable ChunkCompressionType chunkCompressionType,
      @Deprecated @JsonProperty("dictIdCompressionType") @Nullable DictIdCompressionType dictIdCompressionType,
      @JsonProperty("codecSpec") @Nullable String codecSpec,
      @JsonProperty("deriveNumDocsPerChunk") @Nullable Boolean deriveNumDocsPerChunk,
      @JsonProperty("rawIndexWriterVersion") @Nullable Integer rawIndexWriterVersion,
      @JsonProperty("targetMaxChunkSize") @Nullable String targetMaxChunkSize,
      @JsonProperty("targetDocsPerChunk") @Nullable Integer targetDocsPerChunk,
      @JsonProperty("configs") @Nullable Map<String, Object> configs) {
    this(disabled, encodingType,
        getActualCompressionCodec(compressionCodec, chunkCompressionType, dictIdCompressionType), codecSpec,
        deriveNumDocsPerChunk, rawIndexWriterVersion, targetMaxChunkSize, targetDocsPerChunk, configs);
  }

  /// Binary-compatibility bridge for callers compiled against the pre-codecSpec constructor signature.
  /// Delegates to the current 10-argument constructor with `codecSpec=null`.
  ///
  /// @deprecated Use the 10-argument constructor that includes `codecSpec`.
  ///             Scheduled for removal in Pinot 2.0 once all callers have migrated.
  @Deprecated(forRemoval = true)
  public ForwardIndexConfig(@Nullable Boolean disabled, @Nullable CompressionCodec compressionCodec,
      @Nullable ChunkCompressionType chunkCompressionType,
      @Nullable DictIdCompressionType dictIdCompressionType, @Nullable Boolean deriveNumDocsPerChunk,
      @Nullable Integer rawIndexWriterVersion, @Nullable String targetMaxChunkSize,
      @Nullable Integer targetDocsPerChunk, @Nullable Map<String, Object> configs) {
    this(disabled, EncodingType.DICTIONARY,
        getActualCompressionCodec(compressionCodec, chunkCompressionType, dictIdCompressionType),
        null, deriveNumDocsPerChunk, rawIndexWriterVersion, targetMaxChunkSize, targetDocsPerChunk, configs);
  }

  @SuppressWarnings("deprecation") // intentional: translating legacy ChunkCompressionType to its CompressionCodec shim
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

  /// Legacy compression codec accessor. Still load-bearing for `MV_ENTRY_DICT`, the CLP
  /// family, and existing configs. New raw forward-index compression settings should prefer
  /// [#getCodecSpec()].
  @Nullable
  public CompressionCodec getCompressionCodec() {
    return _compressionCodec;
  }

  /// Returns the codec DSL spec string (e.g. `"CODEC(DELTA,ZSTD(3))"`) when this
  /// forward index uses a codec pipeline, or `null` when using the legacy
  /// `compressionCodec` path.
  @Nullable
  public String getCodecSpec() {
    return _codecSpec;
  }

  /// Returns `true` when this config uses a codec pipeline spec rather than a legacy compression codec.
  @JsonIgnore
  public boolean hasCodecSpec() {
    return _codecSpec != null;
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

  public EncodingType getEncodingType() {
    return _encodingType;
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
    return _compressionCodec == that._compressionCodec && Objects.equals(_codecSpec, that._codecSpec)
        && _deriveNumDocsPerChunk == that._deriveNumDocsPerChunk
        && _rawIndexWriterVersion == that._rawIndexWriterVersion && Objects.equals(_targetMaxChunkSize,
        that._targetMaxChunkSize) && _targetDocsPerChunk == that._targetDocsPerChunk
        && _encodingType == that._encodingType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _compressionCodec, _codecSpec, _deriveNumDocsPerChunk,
        _rawIndexWriterVersion, _targetMaxChunkSize, _targetDocsPerChunk, _encodingType);
  }

  public static class Builder {
    private boolean _disabled = false;
    private final EncodingType _encodingType;
    @Nullable
    private CompressionCodec _compressionCodec;
    @Nullable
    private String _codecSpec;
    private boolean _deriveNumDocsPerChunk = false;
    private int _rawIndexWriterVersion = _defaultRawIndexWriterVersion;
    private boolean _rawIndexWriterVersionExplicit = false;
    private String _targetMaxChunkSize = _defaultTargetMaxChunkSize;
    private int _targetDocsPerChunk = _defaultTargetDocsPerChunk;
    private Map<String, Object> _configs = new HashMap<>();

    /// Constructs a builder with the given forward-index encoding. Callers should pass `FieldConfig.getEncodingType()`
    /// so the forward-index encoding stays consistent with the column-level encoding.
    public Builder(EncodingType encodingType) {
      _encodingType = Preconditions.checkNotNull(encodingType, "encodingType must not be null");
    }

    public Builder(ForwardIndexConfig other) {
      this(other, other._encodingType);
    }

    /// Copies all fields from `other` but overrides the forward-index encoding to `encodingType`.
    /// Used at segment creation when an optimizer flips the column to RAW after the configured encoding was
    /// `DICTIONARY`.
    public Builder(ForwardIndexConfig other, EncodingType encodingType) {
      _disabled = other.isDisabled();
      _encodingType = Preconditions.checkNotNull(encodingType, "encodingType must not be null");
      _compressionCodec = other._compressionCodec;
      _codecSpec = other._codecSpec;
      _deriveNumDocsPerChunk = other._deriveNumDocsPerChunk;
      _rawIndexWriterVersion = other._rawIndexWriterVersion;
      _rawIndexWriterVersionExplicit = other._rawIndexWriterVersion != _defaultRawIndexWriterVersion;
      _targetMaxChunkSize = other._targetMaxChunkSize;
      _targetDocsPerChunk = other._targetDocsPerChunk;
      // Defensive copy: getConfigs() exposes this map, so aliasing it would let a mutation on one
      // ForwardIndexConfig instance leak into the copied instance (and vice versa).
      _configs = new HashMap<>(other._configs);
    }

    public Builder withDisabled(boolean disabled) {
      _disabled = disabled;
      return this;
    }

    public Builder withCompressionCodec(CompressionCodec compressionCodec) {
      _compressionCodec = compressionCodec;
      // symmetric with withCodecSpec: setting one path clears the other so build() never hits the
      // mutually-exclusive guard at construction time.
      _codecSpec = null;
      resetCodecPipelineWriterVersionIfNeeded();
      return this;
    }

    public Builder withCodecSpec(String codecSpec) {
      _codecSpec = codecSpec;
      _compressionCodec = null;
      return this;
    }

    public Builder withDeriveNumDocsPerChunk(boolean deriveNumDocsPerChunk) {
      _deriveNumDocsPerChunk = deriveNumDocsPerChunk;
      return this;
    }

    public Builder withRawIndexWriterVersion(int rawIndexWriterVersion) {
      _rawIndexWriterVersion = rawIndexWriterVersion;
      _rawIndexWriterVersionExplicit = true;
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
    @SuppressWarnings("deprecation") // intentional: this deprecated shim translates legacy types to their enum values
    public Builder withCompressionType(ChunkCompressionType chunkCompressionType) {
      if (chunkCompressionType == null) {
        return this;
      }
      _codecSpec = null;
      resetCodecPipelineWriterVersionIfNeeded();
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
      _codecSpec = null;
      resetCodecPipelineWriterVersionIfNeeded();
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
      Integer versionArg = _rawIndexWriterVersionExplicit ? _rawIndexWriterVersion : null;
      return new ForwardIndexConfig(_disabled, _encodingType, _compressionCodec, _codecSpec, _deriveNumDocsPerChunk,
          versionArg, _targetMaxChunkSize, _targetDocsPerChunk, _configs);
    }

    private void resetCodecPipelineWriterVersionIfNeeded() {
      if (_rawIndexWriterVersion == CODEC_PIPELINE_WRITER_VERSION) {
        _rawIndexWriterVersion = _defaultRawIndexWriterVersion;
        _rawIndexWriterVersionExplicit = false;
      }
    }
  }
}
