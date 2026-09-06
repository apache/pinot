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
package org.apache.pinot.segment.spi.index.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Segment;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;

import static com.google.common.base.Preconditions.checkElementIndex;


/// Column metadata parsed from `metadata.properties` (or built through [Builder]).
///
/// A server retains one instance per (segment, column) for as long as the segment is loaded, so the parse path keeps
/// the per-column footprint small: column names, parent-column names, date-time formats/granularities and custom
/// default-null literals are interned (they recur in every segment of a table), and a `defaultNullValue` that equals
/// the type default is not handed to the [FieldSpec] at all, so the spec carries the shared static
/// `FieldSpec.DEFAULT_*` constant and never retains the literal. The [FieldSpec] itself is then interned through
/// [#FIELD_SPEC_INTERNER], so every segment of a table (and every table with an identical column definition) shares
/// one instance per distinct spec instead of retaining its own. Segment-derived [FieldSpec]s must therefore be treated
/// as immutable: a setter call on one would bleed into every other segment and table that shares it, and would
/// corrupt the interner's hash bucket (nothing ever mutated one; copy via a JSON round-trip before mutating).
///
/// The object layout is kept at 64 bytes for an ordinary column for the same reason: the six booleans, the
/// forward-index encoding and the two min/max representation bits are packed into one [#_flags] short; the refs only
/// a partitioned column or an OPEN_STRUCT parent/child carries (partition function and partitions, parent column,
/// sparse keys) live in a lazily allocated [Extras] holder that stays `null` for every other column; the four ints
/// that are not column-distinguishing live in a shared [SharedShape]; and the three element-length ints share their
/// two words with the numeric min/max (see [#_minWord]). The compression stats stay a direct ref because the segment
/// creator writes them for every raw column. None of this is visible through the public getters, so the
/// `/tables/{table}/segments/{segment}/metadata` payload (bean-serialized from the getters) is unchanged.
///
/// | bytes | field(s) |
/// |------:|----------|
/// |    12 | object header |
/// |     4 | `_cardinality` |
/// |    16 | `_minWord`, `_maxWord` |
/// |   2+2 | `_flags` plus alignment padding |
/// |    28 | `_fieldSpec`, `_shape`, `_minValue`, `_maxValue`, `_extras`, `_compressionMetadata`, `_indexTypeSizes` |
/// |    64 | total (was 72: eight ints, six refs and a flags byte) |
///
/// On top of the eight bytes this saves directly, a fixed-width column no longer retains a box per min/max value
/// (~30 bytes and two objects per column for a nullable INT column), and the [SharedShape] is amortized over every
/// column of the segment that has the same shape.
@SuppressWarnings({"rawtypes", "unchecked"})
public class ColumnMetadataImpl implements ColumnMetadata {
  private static final long SIZE_MASK = 0xffffffffffffL;

  // Bits of _flags
  private static final short HAS_DICTIONARY = 1;
  private static final short DICTIONARY_ENCODED_FORWARD_INDEX = 1 << 1;
  private static final short SORTED = 1 << 2;
  private static final short NON_NULL = 1 << 3;
  private static final short MIN_MAX_VALUE_INVALID = 1 << 4;
  private static final short ASCII = 1 << 5;
  private static final short AUTO_GENERATED = 1 << 6;
  /// Set when the min (max) value is held as raw bits in [#_minWord] ([#_maxWord]) rather than as an object in
  /// [#_minValue] ([#_maxValue]); an absent value sets neither.
  private static final short MIN_VALUE_IN_WORD = 1 << 7;
  private static final short MAX_VALUE_IN_WORD = 1 << 8;

  /// Canonical instances of the [FieldSpec]s parsed from `metadata.properties`, keyed by [FieldSpec#equals] /
  /// [FieldSpec#hashCode] (name, data type, single-value, default null value, max length, date-time format and
  /// granularity, ...), so schema evolution yields a distinct canonical instance per version of a column. The specs
  /// are held weakly: the canonical instance is exactly the one the loaded segments retain, so it lives as long as
  /// any of them and is released once the last one is unloaded. Thread-safe.
  private static final Interner<FieldSpec> FIELD_SPEC_INTERNER = Interners.newWeakInterner();

  /// Canonical instances of the [SharedShape]s, held weakly exactly like [#FIELD_SPEC_INTERNER]: the canonical
  /// instance is one of the instances the loaded segments retain, so it lives as long as any of them.
  private static final Interner<SharedShape> SHAPE_INTERNER = Interners.newWeakInterner();

  private final FieldSpec _fieldSpec;
  /// The ints that are not column-distinguishing, shared with every other column that has the same shape.
  private final SharedShape _shape;
  private final int _cardinality;
  /// Two words with a use that depends on whether the stored type is fixed width, which is exactly the condition
  /// under which the other use is dead:
  /// - fixed-width stored type (INT, LONG, FLOAT, DOUBLE, and BOOLEAN/TIMESTAMP through their stored type): the raw
  ///   bits of the min and max value, boxed on demand by [#getMinValue()] / [#getMaxValue()], with presence carried
  ///   by [#MIN_VALUE_IN_WORD] / [#MAX_VALUE_IN_WORD]. The element lengths are dead here because [Builder#build()]
  ///   pins them to `storedType.size()`.
  /// - otherwise: `_minWord` packs `lengthOfShortestElement` (high half) and `lengthOfLongestElement` (low half),
  ///   `_maxWord` holds `maxRowLengthInBytes`. The value words are dead here because a STRING, BYTES, BIG_DECIMAL or
  ///   COMPLEX min/max is an object, kept in [#_minValue] / [#_maxValue].
  ///
  /// A fixed-width column whose builder was handed a min/max that is not the box class of its stored type falls back
  /// to [#_minValue] / [#_maxValue] as well, so no caller can lose a value by handing over an unexpected type.
  private final long _minWord;
  private final long _maxWord;
  @Nullable
  private final Comparable _minValue;
  @Nullable
  private final Comparable _maxValue;
  /// hasDictionary, forward-index encoding, sorted, nonNull, minMaxValueInvalid, ascii, autoGenerated and the two
  /// min/max representation bits, see the bit constants above.
  private final short _flags;
  @Nullable
  private final Extras _extras;
  @Nullable
  private final CompressionMetadata _compressionMetadata;

  /// Packed index sizes: the high 16 bits identify the index type and the low 48 bits hold its size.
  /// Allocated on the first valid append and populated before publication. Not thread-safe.
  @Nullable
  private LongArrayList _indexTypeSizes;

  private ColumnMetadataImpl(FieldSpec fieldSpec, SharedShape shape, int cardinality, long minWord, long maxWord,
      @Nullable Comparable minValue, @Nullable Comparable maxValue, short flags, @Nullable Extras extras,
      @Nullable CompressionMetadata compressionMetadata) {
    _fieldSpec = fieldSpec;
    _shape = shape;
    _cardinality = cardinality;
    _minWord = minWord;
    _maxWord = maxWord;
    _minValue = minValue;
    _maxValue = maxValue;
    _flags = flags;
    _extras = extras;
    _compressionMetadata = compressionMetadata;
  }

  private boolean hasFlag(short flag) {
    return (_flags & flag) != 0;
  }

  private boolean isFixedWidth() {
    return _fieldSpec.getDataType().getStoredType().isFixedWidth();
  }

  @Override
  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  @Override
  public int getTotalDocs() {
    return _shape._totalDocs;
  }

  @Override
  public int getCardinality() {
    return _cardinality;
  }

  @Override
  public boolean hasDictionary() {
    return hasFlag(HAS_DICTIONARY);
  }

  @Override
  public EncodingType getForwardIndexEncoding() {
    return hasFlag(DICTIONARY_ENCODED_FORWARD_INDEX) ? EncodingType.DICTIONARY : EncodingType.RAW;
  }

  @Override
  public boolean isSorted() {
    return hasFlag(SORTED);
  }

  @Override
  public boolean isNonNull() {
    return hasFlag(NON_NULL);
  }

  /// Returns the value equal to the one the builder was handed. A fixed-width min/max is boxed on every call rather
  /// than retained: a server keeps one instance of this class per (segment, column) for the segment lifetime, while
  /// the callers (segment pruners, aggregation rewrites, range-index construction) read it a handful of times per
  /// query and let the box die in the young generation.
  @Nullable
  @Override
  public Comparable<?> getMinValue() {
    return hasFlag(MIN_VALUE_IN_WORD) ? boxValueWord(_minWord) : _minValue;
  }

  @Nullable
  @Override
  public Comparable<?> getMaxValue() {
    return hasFlag(MAX_VALUE_IN_WORD) ? boxValueWord(_maxWord) : _maxValue;
  }

  /// Boxes a value word written by [Builder#toValueWord]; only reached for a fixed-width stored type.
  private Comparable<?> boxValueWord(long word) {
    DataType storedType = _fieldSpec.getDataType().getStoredType();
    switch (storedType) {
      case INT:
        return (int) word;
      case LONG:
        return word;
      case FLOAT:
        return Float.intBitsToFloat((int) word);
      case DOUBLE:
        return Double.longBitsToDouble(word);
      default:
        throw new IllegalStateException("Unsupported stored type for a packed min/max value: " + storedType);
    }
  }

  @Override
  public boolean isMinMaxValueInvalid() {
    return hasFlag(MIN_MAX_VALUE_INVALID);
  }

  @Override
  public int getLengthOfShortestElement() {
    return isFixedWidth() ? _fieldSpec.getDataType().getStoredType().size() : (int) (_minWord >> 32);
  }

  @Override
  public int getLengthOfLongestElement() {
    return isFixedWidth() ? _fieldSpec.getDataType().getStoredType().size() : (int) _minWord;
  }

  @Override
  public boolean isAscii() {
    return hasFlag(ASCII);
  }

  @Override
  public int getBitsPerElement() {
    return _shape._bitsPerElement;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _shape._totalNumberOfEntries;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _shape._maxNumberOfMultiValues;
  }

  /// [Builder#build()] pins this to `lengthOfLongestElement` for an SV column and to
  /// `maxNumberOfMultiValues * storedType.size()` for a fixed-width MV column, so only a var-width MV column needs
  /// the value stored (in [#_maxWord]).
  @Override
  public int getMaxRowLengthInBytes() {
    if (isFixedWidth()) {
      int size = _fieldSpec.getDataType().getStoredType().size();
      return _fieldSpec.isSingleValueField() ? size : _shape._maxNumberOfMultiValues * size;
    }
    return (int) _maxWord;
  }

  @Nullable
  @Override
  public PartitionFunction getPartitionFunction() {
    return _extras != null ? _extras._partitionFunction : null;
  }

  @Nullable
  @Override
  public Set<Integer> getPartitions() {
    return _extras != null ? _extras._partitions : null;
  }

  @Override
  public boolean isAutoGenerated() {
    return hasFlag(AUTO_GENERATED);
  }

  /// Returns `true` if this column is a materialized column produced from an OPEN_STRUCT parent column.
  public boolean isMaterializedChild() {
    return getParentColumn() != null;
  }

  /// Returns the name of the parent OPEN_STRUCT column, or `null` if this is not a materialized column.
  @Nullable
  public String getParentColumn() {
    return _extras != null ? _extras._parentColumn : null;
  }

  /// Names of the keys in this OPEN_STRUCT column's sparse blob, or null when unknown
  /// (segment predates the manifest). Only set on OPEN_STRUCT parent columns.
  @Nullable
  public List<String> getSparseKeys() {
    return _extras != null ? _extras._sparseKeys : null;
  }

  @Override
  public long getIndexSizeFor(IndexType type) {
    if (_indexTypeSizes == null) {
      return UNAVAILABLE;
    }
    short indexId = IndexService.getInstance().getNumericId(type);
    for (int i = 0; i < _indexTypeSizes.size(); i++) {
      long typeAndSize = _indexTypeSizes.getLong(i);
      if (indexId == unpackIndexType(typeAndSize)) {
        return unpackIndexSize(typeAndSize);
      }
    }
    return UNAVAILABLE;
  }

  // size should be non-negative 48-bit value
  public void addIndexSize(short indexType, long size) {
    if (size < 0 || size > SIZE_MASK) {
      throw new IllegalArgumentException(
          "Index size should be a non-negative integer value between 0 and " + SIZE_MASK);
    }
    long typeAndSize = ((long) indexType) << 48 | (size & SIZE_MASK);
    if (_indexTypeSizes == null) {
      _indexTypeSizes = new LongArrayList(2);
    }
    _indexTypeSizes.add(typeAndSize);
  }

  @Override
  public int getNumIndexes() {
    return _indexTypeSizes == null ? 0 : _indexTypeSizes.size();
  }

  @Override
  public short getIndexType(int position) {
    checkElementIndex(position, getNumIndexes());
    return unpackIndexType(_indexTypeSizes.getLong(position));
  }

  private static short unpackIndexType(long typeAndSize) {
    return (short) ((typeAndSize >>> 48));
  }

  @Override
  public long getIndexSize(int position) {
    checkElementIndex(position, getNumIndexes());
    return unpackIndexSize(_indexTypeSizes.getLong(position));
  }

  private static long unpackIndexSize(long typeAndSize) {
    return typeAndSize & SIZE_MASK;
  }

  @Override
  public long getRawForwardIndexUncompressedValueSizeInBytes() {
    return _compressionMetadata != null ? _compressionMetadata._uncompressedValueSizeInBytes : UNAVAILABLE;
  }

  @Nullable
  @Override
  public ChunkCompressionType getRawForwardIndexChunkCompressionType() {
    return _compressionMetadata != null ? _compressionMetadata._forwardIndexChunkCompressionType : null;
  }

  @Override
  public long getDictionaryEncodedUncompressedValueSizeInBytes() {
    return _compressionMetadata != null ? _compressionMetadata._dictionaryUncompressedValueSizeInBytes : UNAVAILABLE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    // The representation two equal columns pick is a function of their field spec and their values, so comparing the
    // raw words and flags is equivalent to comparing the boxed min/max and the unpacked lengths.
    ColumnMetadataImpl that = (ColumnMetadataImpl) o;
    return _cardinality == that._cardinality
        && _flags == that._flags
        && _minWord == that._minWord
        && _maxWord == that._maxWord
        && Objects.equals(_shape, that._shape)
        && Objects.equals(_fieldSpec, that._fieldSpec)
        && Objects.equals(_minValue, that._minValue)
        && Objects.equals(_maxValue, that._maxValue)
        && Objects.equals(_extras, that._extras)
        && Objects.equals(_compressionMetadata, that._compressionMetadata)
        && Objects.equals(_indexTypeSizes, that._indexTypeSizes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_fieldSpec, _shape, _cardinality, _flags, _minWord, _maxWord, _minValue, _maxValue, _extras,
        _compressionMetadata, _indexTypeSizes);
  }

  // Keeps the pre-packing field names and order, which tests and log consumers match on
  @Override
  public String toString() {
    return "ColumnMetadataImpl{"
        + "_fieldSpec=" + _fieldSpec
        + ", _totalDocs=" + getTotalDocs()
        + ", _cardinality=" + _cardinality
        + ", _hasDictionary=" + hasDictionary()
        + ", _forwardIndexEncoding=" + getForwardIndexEncoding()
        + ", _sorted=" + isSorted() + ", _nonNull=" + isNonNull()
        + ", _minValue=" + getMinValue()
        + ", _maxValue=" + getMaxValue()
        + ", _minMaxValueInvalid=" + isMinMaxValueInvalid()
        + ", _lengthOfShortestElement=" + getLengthOfShortestElement()
        + ", _lengthOfLongestElement=" + getLengthOfLongestElement()
        + ", _isAscii=" + isAscii()
        + ", _totalNumberOfEntries=" + getTotalNumberOfEntries()
        + ", _maxNumberOfMultiValues=" + getMaxNumberOfMultiValues()
        + ", _maxRowLengthInBytes=" + getMaxRowLengthInBytes()
        + ", _bitsPerElement=" + getBitsPerElement()
        + ", _partitionFunction=" + getPartitionFunction()
        + ", _partitions=" + getPartitions()
        + ", _autoGenerated=" + isAutoGenerated()
        + ", _parentColumn=" + getParentColumn()
        + ", _sparseKeys=" + getSparseKeys()
        + ", _compressionMetadata=" + _compressionMetadata
        + ", _indexTypeSizes=" + _indexTypeSizes
        + '}';
  }

  public static ColumnMetadataImpl fromPropertiesConfiguration(PropertiesConfiguration config, int totalDocs,
      String column) {
    FieldSpec fieldSpec = extractFieldSpec(column, config);
    Builder builder = new Builder()
        .setFieldSpec(fieldSpec)
        .setTotalDocs(totalDocs)
        .setCardinality(config.getInt(Column.getKeyFor(column, Column.CARDINALITY)))
        .setHasDictionary(config.getBoolean(Column.getKeyFor(column, Column.HAS_DICTIONARY), true))
        .setForwardIndexEncoding(
            config.getEnum(Column.getKeyFor(column, Column.FORWARD_INDEX_ENCODING), EncodingType.class, null))
        .setSorted(config.getBoolean(Column.getKeyFor(column, Column.IS_SORTED), false))
        .setNonNull(config.getBoolean(Column.getKeyFor(column, Column.IS_NON_NULL), false))
        .setLengthOfShortestElement(
            config.getInt(Column.getKeyFor(column, Column.LENGTH_OF_SHORTEST_ELEMENT), UNAVAILABLE))
        .setLengthOfLongestElement(
            config.getInt(Column.getKeyFor(column, Column.LENGTH_OF_LONGEST_ELEMENT), UNAVAILABLE))
        .setDictionaryElementSize(config.getInt(Column.getKeyFor(column, Column.DICTIONARY_ELEMENT_SIZE), UNAVAILABLE))
        .setAscii(config.getBoolean(Column.getKeyFor(column, Column.IS_ASCII), false))
        .setTotalNumberOfEntries(config.getInt(Column.getKeyFor(column, Column.TOTAL_NUMBER_OF_ENTRIES), UNAVAILABLE))
        .setMaxNumberOfMultiValues(
            config.getInt(Column.getKeyFor(column, Column.MAX_MULTI_VALUE_ELEMENTS), UNAVAILABLE))
        .setMaxRowLengthInBytes(config.getInt(Column.getKeyFor(column, Column.MAX_ROW_LENGTH_IN_BYTES), UNAVAILABLE))
        .setBitsPerElement(config.getInt(Column.getKeyFor(column, Column.BITS_PER_ELEMENT), UNAVAILABLE))
        .setAutoGenerated(config.getBoolean(Column.getKeyFor(column, Column.IS_AUTO_GENERATED), false))
        .setParentColumn(intern(config.getString(Column.getKeyFor(column, Column.PARENT_COLUMN), null)));

    Object rawSparseKeys = config.getProperty(Column.getKeyFor(column, Column.SPARSE_KEYS));
    if (rawSparseKeys != null) {
      // The value is a JSON array string, but LegacyListDelimiterHandler splits on commas,
      // fragmenting it into a List. Rejoin before parsing.
      String jsonStr = rawSparseKeys instanceof List
          ? ((List<?>) rawSparseKeys).stream().map(String::valueOf).collect(Collectors.joining(","))
          : rawSparseKeys.toString();
      try {
        List<String> sparseKeys = JsonUtils.stringToObject(jsonStr, new TypeReference<List<String>>() { });
        builder.setSparseKeys(sparseKeys);
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse sparse-key manifest: " + jsonStr, e);
      }
    }

    // Set min/max value
    DataType storedType = fieldSpec.getDataType().getStoredType();
    if (fieldSpec instanceof ComplexFieldSpec) {
      // Complex field does not have min/max value
      builder.setMinMaxValueInvalid(true);
    } else {
      // Set min/max value if available
      // NOTE: Use getProperty() instead of getString() to avoid variable substitution ('${anotherKey}'), which can
      //       cause problem for special values such as '$${' where the first '$' is identified as escape character.
      // TODO: Use getProperty() for other properties as well to avoid the overhead of variable substitution
      String minString = (String) config.getProperty(Column.getKeyFor(column, Column.MIN_VALUE));
      String maxString = (String) config.getProperty(Column.getKeyFor(column, Column.MAX_VALUE));
      // Set min/max value if available
      if (minString != null) {
        builder.setMinValue(parseValue(storedType, column, minString));
      }
      if (maxString != null) {
        builder.setMaxValue(parseValue(storedType, column, maxString));
      }
      if (minString == null && maxString == null) {
        builder.setMinMaxValueInvalid(config.getBoolean(Column.getKeyFor(column, Column.MIN_MAX_VALUE_INVALID), false));
      }
    }

    // Set partition function
    PartitionFunction partitionFunction = extractPartitionFunction(column, config);
    if (partitionFunction != null) {
      builder.setPartitionFunction(partitionFunction);
      builder.setPartitions(extractPartitions(column, config));
    }

    // Read compression stats if available
    builder.setRawForwardIndexUncompressedValueSizeInBytes(
        config.getLong(Column.getKeyFor(column, Column.FORWARD_INDEX_RAW_UNCOMPRESSED_VALUE_SIZE_IN_BYTES),
            UNAVAILABLE));
    builder.setRawForwardIndexChunkCompressionType(
        parseCompressionType(column,
            config.getString(Column.getKeyFor(column, Column.FORWARD_INDEX_RAW_CHUNK_COMPRESSION_TYPE), null)));
    builder.setDictionaryEncodedUncompressedValueSizeInBytes(
        config.getLong(
            Column.getKeyFor(column, Column.FORWARD_INDEX_DICTIONARY_ENCODED_UNCOMPRESSED_VALUE_SIZE_IN_BYTES),
            UNAVAILABLE));

    return builder.build();
  }

  @Nullable
  private static ChunkCompressionType parseCompressionType(String column, @Nullable String value) {
    if (value == null) {
      return null;
    }
    try {
      return ChunkCompressionType.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("Invalid forward-index chunk compression type '" + value
          + "' in metadata for column '" + column + "'", e);
    }
  }

  /// Parses the [FieldSpec] of the given column. DIMENSION, METRIC, TIME and DATE_TIME specs are returned from
  /// [#FIELD_SPEC_INTERNER], so the instance is shared with every other segment whose column parses to an equal spec
  /// and must not be mutated. A COMPLEX spec is not interned: [ComplexFieldSpec] does not override
  /// [FieldSpec#equals], so two structs with different children would alias; its children are parsed through this
  /// method and are interned.
  public static FieldSpec extractFieldSpec(String column, PropertiesConfiguration config) {
    // The name is retained by the FieldSpec, the segment Schema and every per-segment column map, and it recurs in
    // every segment of the table: intern it so all of them alias one JVM-wide instance. When COLUMN_NAME is absent
    // (the segment creator only writes it when it differs from the key) this is the key parsed by SegmentMetadataImpl,
    // which is already interned, so the lookup just returns it.
    String fieldName = config.getString(Column.getKeyFor(column, Column.COLUMN_NAME), column).intern();
    FieldType fieldType = config.getEnum(Column.getKeyFor(column, Column.COLUMN_TYPE), FieldType.class);
    DataType dataType = config.getEnum(Column.getKeyFor(column, Column.DATA_TYPE), DataType.class);
    boolean isSingleValue = config.getBoolean(Column.getKeyFor(column, Column.IS_SINGLE_VALUED), true);
    String defaultNullValueString = config.getString(Column.getKeyFor(column, Column.DEFAULT_NULL_VALUE), null);
    if (defaultNullValueString != null && dataType.getStoredType() == DataType.STRING) {
      defaultNullValueString = CommonsConfigurationUtils.recoverSpecialCharacterInPropertyValue(defaultNullValueString);
    }
    Integer maxLength = config.getInteger(Column.getKeyFor(column, Column.SCHEMA_MAX_LENGTH), null);
    String maxLengthExceedStrategyString =
        config.getString(Column.getKeyFor(column, Column.SCHEMA_MAX_LENGTH_EXCEED_STRATEGY), null);
    FieldSpec.MaxLengthExceedStrategy maxLengthExceedStrategy = maxLengthExceedStrategyString != null
        ? FieldSpec.MaxLengthExceedStrategy.valueOf(maxLengthExceedStrategyString) : null;
    switch (fieldType) {
      case DIMENSION:
        return FIELD_SPEC_INTERNER.intern(new DimensionFieldSpec(fieldName, dataType, isSingleValue, maxLength,
            canonicalDefaultNullValue(fieldType, dataType, defaultNullValueString), maxLengthExceedStrategy));
      case METRIC:
        return FIELD_SPEC_INTERNER.intern(new MetricFieldSpec(fieldName, dataType,
            canonicalDefaultNullValue(fieldType, dataType, defaultNullValueString), maxLength,
            maxLengthExceedStrategy));
      case TIME:
        TimeUnit timeUnit = TimeUnit.valueOf(config.getString(Segment.TIME_UNIT, "DAYS").toUpperCase());
        return FIELD_SPEC_INTERNER.intern(new TimeFieldSpec(new TimeGranularitySpec(dataType, timeUnit, fieldName)));
      case DATE_TIME:
        String format = intern(config.getString(Column.getKeyFor(column, Column.DATETIME_FORMAT)));
        String granularity = intern(config.getString(Column.getKeyFor(column, Column.DATETIME_GRANULARITY)));
        return FIELD_SPEC_INTERNER.intern(new DateTimeFieldSpec(fieldName, dataType, format, granularity,
            canonicalDefaultNullValue(fieldType, dataType, defaultNullValueString), null));
      case COMPLEX:
        List<String> childFieldNames =
            config.getList(String.class, Column.getKeyFor(column, Column.COMPLEX_CHILD_FIELD_NAMES));
        Map<String, FieldSpec> childFieldSpecs = new HashMap<>();
        if (childFieldNames != null) {
          for (String childField : childFieldNames) {
            childFieldSpecs.put(childField.intern(),
                extractFieldSpec(ComplexFieldSpec.getFullChildName(column, childField), config));
          }
        }
        // Deliberately not interned (see the method doc): only the children above are shared.
        return new ComplexFieldSpec(fieldName, dataType, true, childFieldSpecs);
      default:
        throw new IllegalStateException("Unsupported field type: " + fieldType);
    }
  }

  /// Returns the `defaultNullValue` literal to hand to the [FieldSpec] constructor: `null` when the literal parses to
  /// the type default, so the spec ends up holding the shared static `FieldSpec.DEFAULT_*` constant instead of a
  /// per-segment box plus the literal (the segment creator writes the literal for every column, so without this every
  /// column of every segment paid for it); otherwise the interned literal, so a custom default is shared across the
  /// segments of the table. Equality is [DataType#equals(Object, Object)], the predicate [FieldSpec#equals] applies to
  /// default null values, so the canonical spec equals one built from the literal and
  /// [FieldSpec#getDefaultNullValueString()] (derived from the value) is unchanged; a BIG_DECIMAL literal with a
  /// different scale or a negative-zero FLOAT/DOUBLE is not equal and stays verbatim.
  @VisibleForTesting
  @Nullable
  static String canonicalDefaultNullValue(FieldType fieldType, DataType dataType, @Nullable String literal) {
    if (literal == null) {
      return null;
    }
    Object typeDefault;
    try {
      typeDefault = FieldSpec.getDefaultNullValue(fieldType, dataType, null);
    } catch (IllegalStateException e) {
      // No type default for this combination (e.g. a METRIC BOOLEAN): the literal is the only valid value, exactly as
      // the FieldSpec constructor treats it.
      return literal.intern();
    }
    return dataType.equals(FieldSpec.getDefaultNullValue(fieldType, dataType, literal), typeDefault) ? null
        : literal.intern();
  }

  @Nullable
  private static String intern(@Nullable String value) {
    return value != null ? value.intern() : null;
  }

  @Nullable
  public static PartitionFunction extractPartitionFunction(String column, PropertiesConfiguration config) {
    String partitionFunctionName = config.getString(Column.getKeyFor(column, Column.PARTITION_FUNCTION), null);
    if (partitionFunctionName == null) {
      return null;
    }
    int numPartitions = config.getInt(Column.getKeyFor(column, Column.NUM_PARTITIONS));
    Configuration partitionFunctionConfig = config.subset(Column.getKeyFor(column, Column.PARTITION_FUNCTION_CONFIG));
    Map<String, String> partitionFunctionConfigMap;
    if (!partitionFunctionConfig.isEmpty()) {
      partitionFunctionConfigMap = new HashMap<>();
      partitionFunctionConfig.forEach((k, v) -> {
        // NOTE:
        // A partition function config value can have comma and this value is read as a List from
        // PropertiesConfiguration.getProperty, hence we need to rebuild original comma separated string value from
        // this list of values.
        partitionFunctionConfigMap.put(k, v instanceof List ? String.join(",", (List) v) : v.toString());
      });
    } else {
      partitionFunctionConfigMap = null;
    }
    return PartitionFunctionFactory.getPartitionFunction(partitionFunctionName, numPartitions,
        partitionFunctionConfigMap);
  }

  public static IntSet extractPartitions(String column, PropertiesConfiguration config) {
    return ColumnPartitionMetadata.extractPartitions(config.getList(Column.getKeyFor(column, Column.PARTITION_VALUES)));
  }

  private static Comparable parseValue(DataType storedType, String column, String valueString) {
    switch (storedType) {
      case INT:
        return Integer.valueOf(valueString);
      case LONG:
        return Long.valueOf(valueString);
      case FLOAT:
        return Float.valueOf(valueString);
      case DOUBLE:
        return Double.valueOf(valueString);
      case BIG_DECIMAL:
        return new BigDecimal(valueString);
      case STRING:
        return CommonsConfigurationUtils.recoverSpecialCharacterInPropertyValue(valueString);
      case BYTES:
        return BytesUtils.toByteArray(valueString);
      default:
        throw new IllegalStateException("Unsupported data type: " + storedType + " for column: " + column);
    }
  }

  // NOTE: This method is only meant to retain compatibility of serialization for endpoint:
  //       `/tables/{tableName}/segments/{segmentName}/metadata`
  @SuppressWarnings("unused")
  public Map<IndexType<?, ?, ?>, Long> getIndexSizeMap() {
    if (_indexTypeSizes == null) {
      return new HashMap<>();
    }
    IndexService service = IndexService.getInstance();
    Map<IndexType<?, ?, ?>, Long> result = Maps.newHashMapWithExpectedSize(_indexTypeSizes.size());
    for (int i = 0; i < _indexTypeSizes.size(); i++) {
      long typeAndSize = _indexTypeSizes.getLong(i);
      short type = unpackIndexType(typeAndSize);
      long size = unpackIndexSize(typeAndSize);
      result.put(service.get(type), size);
    }
    return result;
  }

  public static Builder builder() {
    return new Builder();
  }

  /// The ints of a column that are not column-distinguishing, interned through [#SHAPE_INTERNER] so the columns of a
  /// segment that have the same shape hold one instance instead of four ints each.
  ///
  /// `totalDocs` is identical for every column of a segment; `totalNumberOfEntries` and `maxNumberOfMultiValues` are
  /// pinned by [Builder#build()] to `totalDocs` and `0` for every SV column; and `bitsPerElement` is `UNAVAILABLE`
  /// for every raw column and takes one of at most 33 values (`1..32`) for a dictionary-encoded one. So the SV
  /// columns of a segment share at most ~34 instances however wide the segment is, and a wide external table of raw
  /// columns shares exactly one. The remaining four ints are not held here: `cardinality` is distinguishing, and the
  /// three element lengths are derived or packed into [#_minWord] / [#_maxWord].
  ///
  /// Immutable and thread-safe.
  private static final class SharedShape {
    private final int _totalDocs;
    private final int _totalNumberOfEntries;
    private final int _maxNumberOfMultiValues;
    private final int _bitsPerElement;

    private SharedShape(int totalDocs, int totalNumberOfEntries, int maxNumberOfMultiValues, int bitsPerElement) {
      _totalDocs = totalDocs;
      _totalNumberOfEntries = totalNumberOfEntries;
      _maxNumberOfMultiValues = maxNumberOfMultiValues;
      _bitsPerElement = bitsPerElement;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SharedShape that = (SharedShape) o;
      return _totalDocs == that._totalDocs
          && _totalNumberOfEntries == that._totalNumberOfEntries
          && _maxNumberOfMultiValues == that._maxNumberOfMultiValues
          && _bitsPerElement == that._bitsPerElement;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_totalDocs, _totalNumberOfEntries, _maxNumberOfMultiValues, _bitsPerElement);
    }
  }

  /// The refs that only a partitioned column (partition function and partitions) or an OPEN_STRUCT parent/child
  /// (sparse keys / parent column) carries. Ordinary columns hold no holder at all, so they never pay for the four
  /// slots; a column that has any of them pays one extra object.
  private static final class Extras {
    @Nullable
    private final PartitionFunction _partitionFunction;
    @Nullable
    private final Set<Integer> _partitions;
    @Nullable
    private final String _parentColumn;
    @Nullable
    private final List<String> _sparseKeys;

    private Extras(@Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions,
        @Nullable String parentColumn, @Nullable List<String> sparseKeys) {
      _partitionFunction = partitionFunction;
      _partitions = partitions;
      _parentColumn = parentColumn;
      _sparseKeys = sparseKeys;
    }

    @Nullable
    private static Extras create(@Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions,
        @Nullable String parentColumn, @Nullable List<String> sparseKeys) {
      return partitionFunction == null && partitions == null && parentColumn == null && sparseKeys == null ? null
          : new Extras(partitionFunction, partitions, parentColumn, sparseKeys);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Extras that = (Extras) o;
      return Objects.equals(_partitionFunction, that._partitionFunction)
          && Objects.equals(_partitions, that._partitions)
          && Objects.equals(_parentColumn, that._parentColumn)
          && Objects.equals(_sparseKeys, that._sparseKeys);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_partitionFunction, _partitions, _parentColumn, _sparseKeys);
    }
  }

  private static final class CompressionMetadata {
    private final long _uncompressedValueSizeInBytes;
    @Nullable
    private final ChunkCompressionType _forwardIndexChunkCompressionType;
    private final long _dictionaryUncompressedValueSizeInBytes;

    private CompressionMetadata(long uncompressedValueSizeInBytes,
        @Nullable ChunkCompressionType forwardIndexChunkCompressionType,
        long dictionaryUncompressedValueSizeInBytes) {
      _uncompressedValueSizeInBytes = uncompressedValueSizeInBytes;
      _forwardIndexChunkCompressionType = forwardIndexChunkCompressionType;
      _dictionaryUncompressedValueSizeInBytes = dictionaryUncompressedValueSizeInBytes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CompressionMetadata that = (CompressionMetadata) o;
      return _uncompressedValueSizeInBytes == that._uncompressedValueSizeInBytes
          && _forwardIndexChunkCompressionType == that._forwardIndexChunkCompressionType
          && _dictionaryUncompressedValueSizeInBytes == that._dictionaryUncompressedValueSizeInBytes;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_uncompressedValueSizeInBytes, _forwardIndexChunkCompressionType,
          _dictionaryUncompressedValueSizeInBytes);
    }

    @Override
    public String toString() {
      return "CompressionMetadata{"
          + "_uncompressedValueSizeInBytes=" + _uncompressedValueSizeInBytes
          + ", _forwardIndexChunkCompressionType=" + _forwardIndexChunkCompressionType
          + ", _dictionaryUncompressedValueSizeInBytes=" + _dictionaryUncompressedValueSizeInBytes
          + '}';
    }

    @Nullable
    private static CompressionMetadata create(long uncompressedValueSizeInBytes,
        @Nullable ChunkCompressionType forwardIndexChunkCompressionType,
        long dictionaryUncompressedValueSizeInBytes) {
      return uncompressedValueSizeInBytes == UNAVAILABLE && forwardIndexChunkCompressionType == null
          && dictionaryUncompressedValueSizeInBytes == UNAVAILABLE ? null
          : new CompressionMetadata(uncompressedValueSizeInBytes, forwardIndexChunkCompressionType,
              dictionaryUncompressedValueSizeInBytes);
    }
  }

  public static class Builder {
    private FieldSpec _fieldSpec;
    private int _totalDocs;
    private int _cardinality;
    private boolean _hasDictionary;
    private EncodingType _forwardIndexEncoding;
    private boolean _sorted;
    private boolean _nonNull;
    private Comparable<?> _minValue;
    private Comparable<?> _maxValue;
    private boolean _minMaxValueInvalid;
    private int _lengthOfShortestElement;
    private int _lengthOfLongestElement;
    private int _dictionaryElementSize;
    private boolean _isAscii;
    private int _totalNumberOfEntries;
    private int _maxNumberOfMultiValues;
    private int _maxRowLengthInBytes;
    private int _bitsPerElement;
    private PartitionFunction _partitionFunction;
    private Set<Integer> _partitions;
    private boolean _autoGenerated;
    private String _parentColumn;
    private List<String> _sparseKeys;
    private long _uncompressedValueSizeInBytes = UNAVAILABLE;
    private ChunkCompressionType _forwardIndexChunkCompressionType;
    private long _dictionaryUncompressedValueSizeInBytes = UNAVAILABLE;

    public Builder setFieldSpec(FieldSpec fieldSpec) {
      _fieldSpec = fieldSpec;
      return this;
    }

    public Builder setTotalDocs(int totalDocs) {
      _totalDocs = totalDocs;
      return this;
    }

    public Builder setCardinality(int cardinality) {
      _cardinality = cardinality;
      return this;
    }

    public Builder setHasDictionary(boolean hasDictionary) {
      _hasDictionary = hasDictionary;
      return this;
    }

    public Builder setForwardIndexEncoding(EncodingType forwardIndexEncoding) {
      _forwardIndexEncoding = forwardIndexEncoding;
      return this;
    }

    public Builder setSorted(boolean sorted) {
      _sorted = sorted;
      return this;
    }

    public Builder setNonNull(boolean nonNull) {
      _nonNull = nonNull;
      return this;
    }

    public Builder setMinValue(Comparable<?> minValue) {
      _minValue = minValue;
      return this;
    }

    public Builder setMaxValue(Comparable<?> maxValue) {
      _maxValue = maxValue;
      return this;
    }

    public Builder setMinMaxValueInvalid(boolean minMaxValueInvalid) {
      _minMaxValueInvalid = minMaxValueInvalid;
      return this;
    }

    public Builder setLengthOfShortestElement(int lengthOfShortestElement) {
      _lengthOfShortestElement = lengthOfShortestElement;
      return this;
    }

    public Builder setLengthOfLongestElement(int lengthOfLongestElement) {
      _lengthOfLongestElement = lengthOfLongestElement;
      return this;
    }

    public Builder setDictionaryElementSize(int dictionaryElementSize) {
      _dictionaryElementSize = dictionaryElementSize;
      return this;
    }

    public Builder setAscii(boolean isAscii) {
      _isAscii = isAscii;
      return this;
    }

    public Builder setTotalNumberOfEntries(int totalNumberOfEntries) {
      _totalNumberOfEntries = totalNumberOfEntries;
      return this;
    }

    public Builder setMaxNumberOfMultiValues(int maxNumberOfMultiValues) {
      _maxNumberOfMultiValues = maxNumberOfMultiValues;
      return this;
    }

    public Builder setMaxRowLengthInBytes(int maxRowLengthInBytes) {
      _maxRowLengthInBytes = maxRowLengthInBytes;
      return this;
    }

    public Builder setBitsPerElement(int bitsPerElement) {
      _bitsPerElement = bitsPerElement;
      return this;
    }

    public Builder setPartitionFunction(PartitionFunction partitionFunction) {
      _partitionFunction = partitionFunction;
      return this;
    }

    public Builder setPartitions(Set<Integer> partitions) {
      _partitions = partitions;
      return this;
    }

    public Builder setAutoGenerated(boolean autoGenerated) {
      _autoGenerated = autoGenerated;
      return this;
    }

    public Builder setParentColumn(String parentColumn) {
      _parentColumn = parentColumn;
      return this;
    }

    public Builder setSparseKeys(List<String> sparseKeys) {
      _sparseKeys = sparseKeys;
      return this;
    }

    /// Sets the uncompressed bytes represented by a raw forward index.
    public Builder setRawForwardIndexUncompressedValueSizeInBytes(long uncompressedValueSizeInBytes) {
      _uncompressedValueSizeInBytes = uncompressedValueSizeInBytes;
      return this;
    }

    /// Sets the chunk compression type persisted for a raw forward index.
    public Builder setRawForwardIndexChunkCompressionType(
        @Nullable ChunkCompressionType forwardIndexChunkCompressionType) {
      _forwardIndexChunkCompressionType = forwardIndexChunkCompressionType;
      return this;
    }

    /// Sets the uncompressed serialized column-value bytes represented by a dictionary-encoded column.
    public Builder setDictionaryEncodedUncompressedValueSizeInBytes(long dictionaryUncompressedValueSizeInBytes) {
      _dictionaryUncompressedValueSizeInBytes = dictionaryUncompressedValueSizeInBytes;
      return this;
    }

    public ColumnMetadataImpl build() {
      // Canonicalize forward index encoding
      if (_forwardIndexEncoding == null) {
        _forwardIndexEncoding = _hasDictionary ? EncodingType.DICTIONARY : EncodingType.RAW;
      }

      // Canonicalize length of shortest/longest element
      DataType storedType = _fieldSpec.getDataType().getStoredType();
      if (storedType.isFixedWidth()) {
        int size = storedType.size();
        _lengthOfShortestElement = size;
        _lengthOfLongestElement = size;
      } else {
        // Pre-1.6.0 segments don't write LENGTH_OF_LONGEST_ELEMENT; fall back to DICTIONARY_ELEMENT_SIZE,
        // which has been written for dictionary-encoded columns since well before 1.6.0 (including the
        // zero-length case where every entry is an empty string). Leaving the field at the UNAVAILABLE
        // sentinel would propagate as `numBytesPerValue = -1` into BaseImmutableDictionary.getBuffer().
        if (_lengthOfLongestElement < 0 && _hasDictionary) {
          _lengthOfLongestElement = _dictionaryElementSize;
        }
      }

      // Canonicalize MV related fields
      if (_fieldSpec.isSingleValueField()) {
        _totalNumberOfEntries = _totalDocs;
        _maxNumberOfMultiValues = 0;
        _maxRowLengthInBytes = _lengthOfLongestElement;
      } else if (storedType.isFixedWidth()) {
        _maxRowLengthInBytes = _maxNumberOfMultiValues * storedType.size();
      }

      // Canonicalize bits per element
      if (!_hasDictionary) {
        _bitsPerElement = UNAVAILABLE;
      }

      short flags = 0;
      if (_hasDictionary) {
        flags |= HAS_DICTIONARY;
      }
      if (_forwardIndexEncoding == EncodingType.DICTIONARY) {
        flags |= DICTIONARY_ENCODED_FORWARD_INDEX;
      }
      if (_sorted) {
        flags |= SORTED;
      }
      if (_nonNull) {
        flags |= NON_NULL;
      }
      if (_minMaxValueInvalid) {
        flags |= MIN_MAX_VALUE_INVALID;
      }
      if (_isAscii) {
        flags |= ASCII;
      }
      if (_autoGenerated) {
        flags |= AUTO_GENERATED;
      }

      // Fill the two words with whichever of the two uses this column has (see ColumnMetadataImpl#_minWord).
      long minWord = 0;
      long maxWord = 0;
      Comparable<?> minValue = _minValue;
      Comparable<?> maxValue = _maxValue;
      if (storedType.isFixedWidth()) {
        Long minBits = toValueWord(storedType, minValue);
        if (minBits != null) {
          minWord = minBits;
          minValue = null;
          flags |= MIN_VALUE_IN_WORD;
        }
        Long maxBits = toValueWord(storedType, maxValue);
        if (maxBits != null) {
          maxWord = maxBits;
          maxValue = null;
          flags |= MAX_VALUE_IN_WORD;
        }
      } else {
        minWord = ((long) _lengthOfShortestElement << 32) | (_lengthOfLongestElement & 0xffffffffL);
        maxWord = _maxRowLengthInBytes & 0xffffffffL;
      }

      SharedShape shape = SHAPE_INTERNER.intern(
          new SharedShape(_totalDocs, _totalNumberOfEntries, _maxNumberOfMultiValues, _bitsPerElement));
      return new ColumnMetadataImpl(_fieldSpec, shape, _cardinality, minWord, maxWord, minValue, maxValue, flags,
          Extras.create(_partitionFunction, _partitions, _parentColumn, _sparseKeys),
          CompressionMetadata.create(_uncompressedValueSizeInBytes, _forwardIndexChunkCompressionType,
              _dictionaryUncompressedValueSizeInBytes));
    }

    /// Returns the raw bits of a min/max value of a fixed-width stored type, or `null` when there is no value or the
    /// value is not the box class of the stored type (in which case it stays an object ref, so an unexpected type
    /// from a [Builder] caller is preserved rather than dropped or mistranslated). FLOAT and DOUBLE go through
    /// [Float#floatToIntBits] / [Double#doubleToLongBits] rather than the raw variants, so a NaN keeps comparing
    /// equal to a NaN exactly as [Float#equals] does today.
    @Nullable
    private static Long toValueWord(DataType storedType, @Nullable Comparable<?> value) {
      switch (storedType) {
        case INT:
          return value instanceof Integer ? (long) (Integer) value : null;
        case LONG:
          return value instanceof Long ? (Long) value : null;
        case FLOAT:
          return value instanceof Float ? (long) Float.floatToIntBits((Float) value) : null;
        case DOUBLE:
          return value instanceof Double ? Double.doubleToLongBits((Double) value) : null;
        default:
          return null;
      }
    }
  }
}
