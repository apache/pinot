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

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.CRC32C;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.store.ColumnIndexUtils;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ColumnNameInterner;
import org.apache.pinot.spi.utils.JsonUtils;

/// Versioned, file-backed metadata for a sealed segment. The source `metadata.properties` remains the canonical
/// representation for old readers and preprocessing.
///
/// The big-endian file starts with a 64-byte header containing the magic, format version, total length, source
/// `metadata.properties` and `index_map` length/CRC fingerprints, payload CRC, segment-property blob reference,
/// column count, and record-table bounds. Sorted columns occupy fixed 180-byte records, followed by variable blobs;
/// each blob reference is an absolute 32-bit offset and length. Numeric column statistics are read directly from the
/// mapping. Only column names and shared [FieldSpec] references are retained on heap.
///
/// A stale or corrupt sidecar is rebuilt from the canonical source and installed by atomic rename. Change `VERSION`
/// whenever the header, record, or blob encoding changes; old versions are rebuilt rather than migrated.
final class MappedSegmentMetadataStore implements Closeable {
  static final String FILE_NAME = "segment_metadata.bin";

  private static final int MAGIC = 0x504d4431; // PMD1
  private static final int VERSION = 1;
  private static final int HEADER_SIZE = 64;
  private static final int RECORD_SIZE = 180;

  static final int NAME = 0;
  static final int FIELD_SPEC = 8;
  static final int TOTAL_DOCS = 16;
  static final int CARDINALITY = 20;
  static final int FLAGS = 24;
  static final int ENCODING = 28;
  static final int SHORTEST = 32;
  static final int LONGEST = 36;
  static final int TOTAL_ENTRIES = 40;
  static final int MAX_MV = 44;
  static final int MAX_ROW = 48;
  static final int BITS = 52;
  static final int MIN_TAG = 56;
  static final int MIN_RAW = 60;
  static final int MIN_BLOB = 68;
  static final int MAX_TAG = 76;
  static final int MAX_RAW = 80;
  static final int MAX_BLOB = 88;
  static final int PARTITION_FUNCTION = 96;
  static final int PARTITIONS = 104;
  static final int PARENT = 112;
  static final int SPARSE_KEYS = 120;
  static final int SPARSE_MULTI_VALUE_KEYS = 128;
  static final int SPARSE_KEY_TYPES = 136;
  static final int RAW_UNCOMPRESSED_SIZE = 144;
  static final int DICTIONARY_UNCOMPRESSED_SIZE = 152;
  static final int COMPRESSION_TYPE = 160;
  static final int INDEXES = 168;
  static final int INDEX_COUNT = 176;

  static final int HAS_DICTIONARY = 1;
  static final int SORTED = 1 << 1;
  static final int NON_NULL = 1 << 2;
  static final int INVALID_MIN_MAX = 1 << 3;
  static final int ASCII = 1 << 4;
  static final int AUTO_GENERATED = 1 << 5;

  private static final int NO_VALUE = 0;
  private static final int INT_VALUE = 1;
  private static final int LONG_VALUE = 2;
  private static final int FLOAT_VALUE = 3;
  private static final int DOUBLE_VALUE = 4;
  private static final int DECIMAL_VALUE = 5;
  private static final int STRING_VALUE = 6;
  private static final int BYTES_VALUE = 7;

  private static final Interner<FieldSpec> FIELD_SPECS = Interners.newWeakInterner();
  private static final Interner<ColumnLayout> COLUMN_LAYOUTS = Interners.newWeakInterner();

  private final PinotDataBuffer _buffer;
  private final ColumnLayout _columns;
  private final int _tableOffset;
  private final int _segmentConfigOffset;
  private final int _segmentConfigLength;

  private MappedSegmentMetadataStore(PinotDataBuffer buffer, ColumnLayout columns, int tableOffset,
      int segmentConfigOffset, int segmentConfigLength) {
    _buffer = buffer;
    _columns = columns;
    _tableOffset = tableOffset;
    _segmentConfigOffset = segmentConfigOffset;
    _segmentConfigLength = segmentConfigLength;
  }

  static MappedSegmentMetadataStore open(File indexDir)
      throws IOException, ConfigurationException {
    File source = SegmentDirectoryPaths.findMetadataFile(indexDir);
    if (source == null) {
      throw new IOException("Missing metadata.properties in " + indexDir);
    }
    File sidecar = new File(indexDir, FILE_NAME);
    for (int attempt = 0; attempt < 3; attempt++) {
      Fingerprint propertiesFingerprint = Fingerprint.of(source);
      Fingerprint indexMapFingerprint = Fingerprint.of(findIndexMap(indexDir));
      byte[] bytes = readValid(sidecar, propertiesFingerprint, indexMapFingerprint);
      if (bytes == null) {
        writeAtomically(sidecar, build(indexDir, propertiesFingerprint, indexMapFingerprint));
        bytes = readValid(sidecar, propertiesFingerprint, indexMapFingerprint);
        if (bytes == null) {
          continue;
        }
      }
      PinotDataBuffer buffer = PinotDataBuffer.mapFile(sidecar, true, 0, bytes.length, ByteOrder.BIG_ENDIAN,
          "segment metadata");
      boolean keepBuffer = false;
      try {
        // Another loader may atomically replace the sidecar between validation and mapFile(). Check the exact mapped
        // bytes and the canonical inputs before publishing a view over this mapping.
        if (!matchesMapped(buffer, bytes) || !propertiesFingerprint.sameAs(Fingerprint.of(source))
            || !indexMapFingerprint.sameAs(Fingerprint.of(findIndexMap(indexDir)))) {
          continue;
        }
        ByteBuffer header = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        int count = header.getInt(52);
        int tableOffset = header.getInt(56);
        String[] names = new String[count];
        FieldSpec[] fieldSpecs = new FieldSpec[count];
        for (int i = 0; i < count; i++) {
          int record = tableOffset + i * RECORD_SIZE;
          names[i] = ColumnNameInterner.intern(readString(bytes, header.getInt(record + NAME),
              header.getInt(record + NAME + 4)));
          FieldSpec parsed = JsonUtils.stringToObject(readString(bytes, header.getInt(record + FIELD_SPEC),
              header.getInt(record + FIELD_SPEC + 4)), FieldSpec.class);
          fieldSpecs[i] = parsed instanceof ComplexFieldSpec ? parsed : FIELD_SPECS.intern(parsed);
        }
        ColumnLayout columns = COLUMN_LAYOUTS.intern(new ColumnLayout(names, fieldSpecs));
        MappedSegmentMetadataStore store = new MappedSegmentMetadataStore(buffer, columns, tableOffset,
            header.getInt(44), header.getInt(48));
        keepBuffer = true;
        return store;
      } catch (IOException | RuntimeException e) {
        throw new IOException("Invalid field specs in mapped segment metadata: " + sidecar, e);
      } finally {
        if (!keepBuffer) {
          buffer.close();
        }
      }
    }
    throw new IOException("Metadata changed while mapping segment: " + indexDir);
  }

  private static boolean matchesMapped(PinotDataBuffer buffer, byte[] bytes) {
    if (buffer.size() != bytes.length) {
      return false;
    }
    byte[] block = new byte[Math.min(8192, bytes.length)];
    for (int offset = 0; offset < bytes.length; offset += block.length) {
      int length = Math.min(block.length, bytes.length - offset);
      buffer.copyTo(offset, block, 0, length);
      for (int i = 0; i < length; i++) {
        if (block[i] != bytes[offset + i]) {
          return false;
        }
      }
    }
    return true;
  }

  int size() {
    return _columns._names.length;
  }

  String name(int ordinal) {
    return _columns._names[ordinal];
  }

  int ordinal(String column) {
    return Arrays.binarySearch(_columns._names, column);
  }

  int getInt(int ordinal, int offset) {
    return _buffer.getInt((long) _tableOffset + (long) ordinal * RECORD_SIZE + offset);
  }

  long getLong(int ordinal, int offset) {
    return _buffer.getLong((long) _tableOffset + (long) ordinal * RECORD_SIZE + offset);
  }

  boolean flag(int ordinal, int flag) {
    return (getInt(ordinal, FLAGS) & flag) != 0;
  }

  @Nullable
  String string(int ordinal, int field) {
    byte[] bytes = blob(ordinal, field);
    return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
  }

  @Nullable
  byte[] blob(int ordinal, int field) {
    int offset = getInt(ordinal, field);
    if (offset < 0) {
      return null;
    }
    int length = getInt(ordinal, field + 4);
    byte[] bytes = new byte[length];
    _buffer.copyTo(offset, bytes);
    return bytes;
  }

  FieldSpec fieldSpec(int ordinal) {
    return _columns._fieldSpecs[ordinal];
  }

  @Nullable
  Comparable<?> value(int ordinal, boolean minimum) {
    int tag = getInt(ordinal, minimum ? MIN_TAG : MAX_TAG);
    long raw = getLong(ordinal, minimum ? MIN_RAW : MAX_RAW);
    int blobField = minimum ? MIN_BLOB : MAX_BLOB;
    switch (tag) {
      case NO_VALUE:
        return null;
      case INT_VALUE:
        return (int) raw;
      case LONG_VALUE:
        return raw;
      case FLOAT_VALUE:
        return Float.intBitsToFloat((int) raw);
      case DOUBLE_VALUE:
        return Double.longBitsToDouble(raw);
      case DECIMAL_VALUE:
        return new BigDecimal(string(ordinal, blobField));
      case STRING_VALUE:
        return string(ordinal, blobField);
      case BYTES_VALUE:
        return new ByteArray(blob(ordinal, blobField));
      default:
        throw new IllegalStateException("Invalid value tag: " + tag);
    }
  }

  PropertiesConfiguration segmentConfiguration()
      throws IOException, ConfigurationException {
    byte[] bytes = new byte[_segmentConfigLength];
    _buffer.copyTo(_segmentConfigOffset, bytes);
    // Values were already split by the source parser. A second delimiter pass would change escaped commas.
    PropertiesConfiguration config = CommonsConfigurationUtils.fromInputStream(null, false, null);
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
      int count = input.readInt();
      for (int i = 0; i < count; i++) {
        String key = readString(input);
        int valueCount = input.readInt();
        if (valueCount == 1) {
          config.setProperty(key, readString(input));
        } else {
          List<String> values = new ArrayList<>(valueCount);
          for (int j = 0; j < valueCount; j++) {
            values.add(readString(input));
          }
          config.setProperty(key, values);
        }
      }
    }
    return config;
  }

  @Override
  public void close()
      throws IOException {
    _buffer.close();
  }

  private static byte[] build(File indexDir, Fingerprint propertiesFingerprint, Fingerprint indexMapFingerprint)
      throws IOException, ConfigurationException {
    SegmentMetadataImpl source = new SegmentMetadataImpl(indexDir);
    PropertiesConfiguration config = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    List<String> columns = new ArrayList<>(source.getColumnMetadataMap().keySet());
    columns.sort(Comparator.naturalOrder());
    if ((long) columns.size() * RECORD_SIZE + HEADER_SIZE > Integer.MAX_VALUE) {
      throw new IOException("Too many columns for mapped metadata: " + columns.size());
    }
    int blobBase = HEADER_SIZE + columns.size() * RECORD_SIZE;
    BlobWriter blobs = new BlobWriter(blobBase);
    BlobRef segmentConfig = blobs.add(encodeSegmentConfiguration(config));
    // EmptyColumnMetadata does not load index sizes even if a stale index_map is still present.
    Map<String, List<IndexSize>> indexSizes = source.getTotalDocs() > 0 ? readIndexSizes(indexDir) : Map.of();
    byte[] records = new byte[columns.size() * RECORD_SIZE];
    ByteBuffer recordBuffer = ByteBuffer.wrap(records).order(ByteOrder.BIG_ENDIAN);
    for (int ordinal = 0; ordinal < columns.size(); ordinal++) {
      String column = columns.get(ordinal);
      ColumnMetadata metadata = source.getColumnMetadataFor(column);
      int offset = ordinal * RECORD_SIZE;
      putRef(recordBuffer, offset + NAME, blobs.add(column));
      putRef(recordBuffer, offset + FIELD_SPEC, blobs.add(metadata.getFieldSpec().toJsonObject().toString()));
      recordBuffer.putInt(offset + TOTAL_DOCS, metadata.getTotalDocs());
      recordBuffer.putInt(offset + CARDINALITY, metadata.getCardinality());
      int flags = (metadata.hasDictionary() ? HAS_DICTIONARY : 0) | (metadata.isSorted() ? SORTED : 0)
          | (metadata.isNonNull() ? NON_NULL : 0) | (metadata.isMinMaxValueInvalid() ? INVALID_MIN_MAX : 0)
          | (metadata.isAscii() ? ASCII : 0) | (metadata.isAutoGenerated() ? AUTO_GENERATED : 0);
      recordBuffer.putInt(offset + FLAGS, flags);
      EncodingType encoding = metadata.getForwardIndexEncoding();
      recordBuffer.putInt(offset + ENCODING, encoding == null ? 0 : encoding == EncodingType.RAW ? 1 : 2);
      recordBuffer.putInt(offset + SHORTEST, metadata.getLengthOfShortestElement());
      recordBuffer.putInt(offset + LONGEST, metadata.getLengthOfLongestElement());
      recordBuffer.putInt(offset + TOTAL_ENTRIES, metadata.getTotalNumberOfEntries());
      recordBuffer.putInt(offset + MAX_MV, metadata.getMaxNumberOfMultiValues());
      recordBuffer.putInt(offset + MAX_ROW, metadata.getMaxRowLengthInBytes());
      recordBuffer.putInt(offset + BITS, metadata.getBitsPerElement());
      writeValue(recordBuffer, offset + MIN_TAG, offset + MIN_RAW, offset + MIN_BLOB, metadata.getMinValue(), blobs);
      writeValue(recordBuffer, offset + MAX_TAG, offset + MAX_RAW, offset + MAX_BLOB, metadata.getMaxValue(), blobs);
      PartitionFunction partitionFunction = metadata.getPartitionFunction();
      putRef(recordBuffer, offset + PARTITION_FUNCTION,
          partitionFunction != null ? blobs.add(encodePartitionFunction(column, config)) : BlobRef.ABSENT);
      putRef(recordBuffer, offset + PARTITIONS,
          metadata.getPartitions() != null ? blobs.add(encodePartitions(metadata.getPartitions())) : BlobRef.ABSENT);
      if (metadata instanceof ColumnMetadataImpl) {
        ColumnMetadataImpl impl = (ColumnMetadataImpl) metadata;
        putRef(recordBuffer, offset + PARENT, blobs.add(impl.getParentColumn()));
        putRef(recordBuffer, offset + SPARSE_KEYS, blobs.addJson(impl.getSparseKeys()));
        putRef(recordBuffer, offset + SPARSE_MULTI_VALUE_KEYS, blobs.addJson(impl.getSparseMultiValueKeys()));
        putRef(recordBuffer, offset + SPARSE_KEY_TYPES, blobs.addJson(impl.getSparseKeyTypes()));
      } else {
        putRef(recordBuffer, offset + PARENT, BlobRef.ABSENT);
        putRef(recordBuffer, offset + SPARSE_KEYS, BlobRef.ABSENT);
        putRef(recordBuffer, offset + SPARSE_MULTI_VALUE_KEYS, BlobRef.ABSENT);
        putRef(recordBuffer, offset + SPARSE_KEY_TYPES, BlobRef.ABSENT);
      }
      recordBuffer.putLong(offset + RAW_UNCOMPRESSED_SIZE,
          metadata.getRawForwardIndexUncompressedValueSizeInBytes());
      recordBuffer.putLong(offset + DICTIONARY_UNCOMPRESSED_SIZE,
          metadata.getDictionaryEncodedUncompressedValueSizeInBytes());
      putRef(recordBuffer, offset + COMPRESSION_TYPE,
          blobs.add(metadata.getRawForwardIndexChunkCompressionType() != null
              ? metadata.getRawForwardIndexChunkCompressionType().name() : null));
      List<IndexSize> sizes = indexSizes.getOrDefault(column, List.of());
      recordBuffer.putInt(offset + INDEX_COUNT, sizes.size());
      putRef(recordBuffer, offset + INDEXES, sizes.isEmpty() ? BlobRef.ABSENT : blobs.add(encodeIndexSizes(sizes)));
    }
    byte[] blobBytes = blobs.toByteArray();
    long totalSize = (long) HEADER_SIZE + records.length + blobBytes.length;
    if (totalSize > Integer.MAX_VALUE) {
      throw new IOException("Mapped metadata exceeds 2 GiB: " + totalSize);
    }
    byte[] bytes = new byte[(int) totalSize];
    ByteBuffer header = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    header.putInt(0, MAGIC);
    header.putInt(4, VERSION);
    header.putLong(8, totalSize);
    header.putLong(16, propertiesFingerprint._length);
    header.putInt(24, propertiesFingerprint._crc);
    header.putLong(28, indexMapFingerprint._length);
    header.putInt(36, indexMapFingerprint._crc);
    header.putInt(44, segmentConfig._offset);
    header.putInt(48, segmentConfig._length);
    header.putInt(52, columns.size());
    header.putInt(56, HEADER_SIZE);
    header.putInt(60, blobBase);
    System.arraycopy(records, 0, bytes, HEADER_SIZE, records.length);
    System.arraycopy(blobBytes, 0, bytes, blobBase, blobBytes.length);
    header.putInt(40, crc(bytes, HEADER_SIZE, bytes.length - HEADER_SIZE));
    return bytes;
  }

  private static byte[] encodeSegmentConfiguration(PropertiesConfiguration config)
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      List<String> keys = CommonsConfigurationUtils.getKeys(config).stream()
          .filter(key -> !key.startsWith(V1Constants.MetadataKeys.Column.COLUMN_PROPS_KEY_PREFIX))
          .sorted().collect(Collectors.toList());
      output.writeInt(keys.size());
      for (String key : keys) {
        writeString(output, key);
        Object value = config.getProperty(key);
        if (value instanceof List<?>) {
          List<?> values = (List<?>) value;
          output.writeInt(values.size());
          for (Object entry : values) {
            writeString(output, String.valueOf(entry));
          }
        } else {
          output.writeInt(1);
          writeString(output, String.valueOf(value));
        }
      }
    }
    return bytes.toByteArray();
  }

  private static byte[] encodePartitionFunction(String column, PropertiesConfiguration metadata)
      throws IOException {
    String prefix = V1Constants.MetadataKeys.Column.COLUMN_PROPS_KEY_PREFIX + column + ".";
    String name = metadata.getString(prefix + V1Constants.MetadataKeys.Column.PARTITION_FUNCTION);
    int numPartitions = metadata.getInt(prefix + V1Constants.MetadataKeys.Column.NUM_PARTITIONS);
    Configuration functionConfig = metadata.subset(prefix + V1Constants.MetadataKeys.Column.PARTITION_FUNCTION_CONFIG);
    Map<String, String> config = new HashMap<>();
    functionConfig.forEach((key, value) -> config.put(key,
        value instanceof List ? ((List<?>) value).stream().map(String::valueOf).collect(Collectors.joining(","))
            : value.toString()));
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      writeString(output, name);
      output.writeInt(numPartitions);
      output.writeInt(config.size());
      for (String key : config.keySet().stream().sorted().collect(Collectors.toList())) {
        writeString(output, key);
        writeString(output, config.get(key));
      }
    }
    return bytes.toByteArray();
  }

  private static byte[] encodePartitions(Set<Integer> partitions)
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeInt(partitions.size());
      for (int partition : partitions.stream().sorted().collect(Collectors.toList())) {
        output.writeInt(partition);
      }
    }
    return bytes.toByteArray();
  }

  private static byte[] encodeIndexSizes(List<IndexSize> sizes)
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      for (IndexSize size : sizes) {
        writeString(output, size._type);
        output.writeLong(size._size);
      }
    }
    return bytes.toByteArray();
  }

  private static Map<String, List<IndexSize>> readIndexSizes(File indexDir)
      throws ConfigurationException {
    File indexMap = findIndexMap(indexDir);
    if (indexMap == null) {
      return Map.of();
    }
    // The local v3 index map is read by SegmentMetadataImpl and retains zero-sized indexes. The root-level map is
    // staged by tiered storage, whose existing reporting path omits zero-sized placeholders.
    boolean stagedIndexMap = indexMap.getParentFile().equals(indexDir);
    PropertiesConfiguration config = CommonsConfigurationUtils.fromFile(indexMap);
    Map<String, List<IndexSize>> result = new HashMap<>();
    for (String key : CommonsConfigurationUtils.getKeys(config)) {
      try {
        String[] parsed = ColumnIndexUtils.parseIndexMapKeys(key, indexDir.getPath());
        if (ColumnIndexUtils.MAP_KEY_NAME_SIZE.equals(parsed[2])) {
          long size = config.getLong(key);
          if (size < 0 || size > (1L << 48) - 1 || (stagedIndexMap && size == 0)) {
            continue;
          }
          IndexType<?, ?, ?> type = IndexService.getInstance().get(parsed[1]);
          result.computeIfAbsent(parsed[0], ignored -> new ArrayList<>())
              .add(new IndexSize(type.getId(), size));
        }
      } catch (Exception ignored) {
        // Match the tolerant index_map reporting path in SegmentMetadataImpl.
      }
    }
    return result;
  }

  @Nullable
  private static File findIndexMap(File indexDir) {
    File v3 = new File(new File(indexDir, "v3"), V1Constants.INDEX_MAP_FILE_NAME);
    if (v3.isFile()) {
      return v3;
    }
    File root = new File(indexDir, V1Constants.INDEX_MAP_FILE_NAME);
    return root.isFile() ? root : null;
  }

  private static void writeValue(ByteBuffer target, int tagOffset, int rawOffset, int blobOffset,
      @Nullable Comparable<?> value, BlobWriter blobs) {
    int tag = NO_VALUE;
    long raw = 0;
    BlobRef blob = BlobRef.ABSENT;
    if (value instanceof Integer) {
      tag = INT_VALUE;
      raw = (Integer) value;
    } else if (value instanceof Long) {
      tag = LONG_VALUE;
      raw = (Long) value;
    } else if (value instanceof Float) {
      tag = FLOAT_VALUE;
      raw = Float.floatToRawIntBits((Float) value);
    } else if (value instanceof Double) {
      tag = DOUBLE_VALUE;
      raw = Double.doubleToRawLongBits((Double) value);
    } else if (value instanceof BigDecimal) {
      tag = DECIMAL_VALUE;
      blob = blobs.add(value.toString());
    } else if (value instanceof String) {
      tag = STRING_VALUE;
      blob = blobs.add((String) value);
    } else if (value instanceof ByteArray) {
      tag = BYTES_VALUE;
      blob = blobs.add(((ByteArray) value).getBytes());
    } else if (value != null) {
      throw new IllegalArgumentException("Unsupported metadata value: " + value.getClass());
    }
    target.putInt(tagOffset, tag);
    target.putLong(rawOffset, raw);
    putRef(target, blobOffset, blob);
  }

  private static void putRef(ByteBuffer target, int offset, BlobRef ref) {
    target.putInt(offset, ref._offset);
    target.putInt(offset + 4, ref._length);
  }

  @Nullable
  private static byte[] readValid(File file, Fingerprint properties, Fingerprint indexMap)
      throws IOException {
    if (!file.isFile() || file.length() < HEADER_SIZE || file.length() > Integer.MAX_VALUE) {
      return null;
    }
    byte[] bytes = Files.readAllBytes(file.toPath());
    try {
      validate(bytes, properties, indexMap);
      return bytes;
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static void validate(byte[] bytes, Fingerprint properties, Fingerprint indexMap) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    if (buffer.getInt(0) != MAGIC || buffer.getInt(4) != VERSION || buffer.getLong(8) != bytes.length
        || buffer.getLong(16) != properties._length || buffer.getInt(24) != properties._crc
        || buffer.getLong(28) != indexMap._length || buffer.getInt(36) != indexMap._crc
        || buffer.getInt(40) != crc(bytes, HEADER_SIZE, bytes.length - HEADER_SIZE)) {
      throw new IllegalArgumentException("Invalid metadata header");
    }
    int count = buffer.getInt(52);
    int table = buffer.getInt(56);
    int blobStart = buffer.getInt(60);
    if (count < 0 || table != HEADER_SIZE || (long) table + (long) count * RECORD_SIZE != blobStart
        || blobStart > bytes.length) {
      throw new IllegalArgumentException("Invalid column table bounds");
    }
    validateBlob(buffer, buffer.getInt(44), buffer.getInt(48), blobStart, bytes.length, false);
    validateSegmentConfiguration(bytes, buffer.getInt(44), buffer.getInt(48));
    String prior = null;
    for (int i = 0; i < count; i++) {
      int record = table + i * RECORD_SIZE;
      for (int field : new int[]{NAME, FIELD_SPEC, MIN_BLOB, MAX_BLOB, PARTITION_FUNCTION, PARTITIONS, PARENT,
          SPARSE_KEYS, SPARSE_MULTI_VALUE_KEYS, SPARSE_KEY_TYPES, COMPRESSION_TYPE, INDEXES}) {
        validateBlob(buffer, buffer.getInt(record + field), buffer.getInt(record + field + 4), blobStart,
            bytes.length, field != NAME && field != FIELD_SPEC);
      }
      int nameOffset = buffer.getInt(record + NAME);
      int nameLength = buffer.getInt(record + NAME + 4);
      String name = readString(bytes, nameOffset, nameLength);
      if (prior != null && prior.compareTo(name) >= 0) {
        throw new IllegalArgumentException("Column names are not sorted and unique");
      }
      prior = name;
      int minTag = buffer.getInt(record + MIN_TAG);
      int maxTag = buffer.getInt(record + MAX_TAG);
      int encoding = buffer.getInt(record + ENCODING);
      if (minTag < NO_VALUE || minTag > BYTES_VALUE || maxTag < NO_VALUE || maxTag > BYTES_VALUE
          || encoding < 0 || encoding > 2 || buffer.getInt(record + INDEX_COUNT) < 0) {
        throw new IllegalArgumentException("Invalid column value tag or index count");
      }
      validateValueBlob(buffer, record + MIN_BLOB, minTag);
      validateValueBlob(buffer, record + MAX_BLOB, maxTag);
      validateIndexSizes(bytes, buffer.getInt(record + INDEXES), buffer.getInt(record + INDEXES + 4),
          buffer.getInt(record + INDEX_COUNT));
    }
  }

  private static void validateValueBlob(ByteBuffer buffer, int field, int tag) {
    boolean usesBlob = tag == DECIMAL_VALUE || tag == STRING_VALUE || tag == BYTES_VALUE;
    int offset = buffer.getInt(field);
    int length = buffer.getInt(field + 4);
    if (usesBlob ? offset < 0 : offset != -1 || length != 0) {
      throw new IllegalArgumentException("Invalid typed value blob");
    }
  }

  private static void validateSegmentConfiguration(byte[] bytes, int offset, int length) {
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes, offset, length))) {
      int count = input.readInt();
      if (count < 0 || count > length / 8) {
        throw new IllegalArgumentException("Invalid segment property count");
      }
      for (int i = 0; i < count; i++) {
        readString(input);
        int valueCount = input.readInt();
        if (valueCount < 0 || valueCount > input.available() / 4) {
          throw new IllegalArgumentException("Invalid segment property value count");
        }
        for (int j = 0; j < valueCount; j++) {
          readString(input);
        }
      }
      if (input.available() != 0) {
        throw new IllegalArgumentException("Trailing segment property bytes");
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid segment properties", e);
    }
  }

  private static void validateBlob(ByteBuffer buffer, int offset, int length, int blobStart, int total,
      boolean optional) {
    if (optional && offset == -1 && length == 0) {
      return;
    }
    if (offset < blobStart || length < 0 || (long) offset + length > total) {
      throw new IllegalArgumentException("Invalid metadata blob bounds");
    }
  }

  private static void validateIndexSizes(byte[] bytes, int offset, int length, int count) {
    if (count == 0) {
      if (offset != -1 || length != 0) {
        throw new IllegalArgumentException("Invalid empty index-size list");
      }
      return;
    }
    ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length).order(ByteOrder.BIG_ENDIAN);
    int cursor = offset;
    int end = offset + length;
    for (int i = 0; i < count; i++) {
      if (cursor + 4 > end) {
        throw new IllegalArgumentException("Truncated index-size name");
      }
      int nameLength = buffer.getInt(cursor);
      cursor += 4;
      if (nameLength < 0 || (long) cursor + nameLength + 8 > end) {
        throw new IllegalArgumentException("Invalid index-size entry");
      }
      String type = new String(bytes, cursor, nameLength, StandardCharsets.UTF_8);
      if (IndexService.getInstance().get(type) == null) {
        throw new IllegalArgumentException("Unknown index type: " + type);
      }
      cursor += nameLength + 8;
    }
    if (cursor != end) {
      throw new IllegalArgumentException("Trailing index-size bytes");
    }
  }

  static String readString(DataInputStream input)
      throws IOException {
    int length = input.readInt();
    if (length < 0 || length > input.available()) {
      throw new IOException("Invalid string length: " + length);
    }
    byte[] bytes = new byte[length];
    input.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static String readString(byte[] bytes, int offset, int length) {
    return new String(bytes, offset, length, StandardCharsets.UTF_8);
  }

  private static void writeString(DataOutputStream output, String string)
      throws IOException {
    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  private static int crc(byte[] bytes, int offset, int length) {
    CRC32C crc = new CRC32C();
    crc.update(bytes, offset, length);
    return (int) crc.getValue();
  }

  private static void writeAtomically(File sidecar, byte[] bytes)
      throws IOException {
    Path path = sidecar.toPath();
    Path temp = Files.createTempFile(path.getParent(), ".segment_metadata.", ".tmp");
    try {
      Files.write(temp, bytes);
      try {
        Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(temp);
    }
  }

  /// Shares an identical column directory across segments while each segment retains its own mapped statistics.
  /// Field specs are mutable, so identity keeps the interner hash stable even if a caller changes a shared spec.
  private static final class ColumnLayout {
    private final String[] _names;
    private final FieldSpec[] _fieldSpecs;
    private final int _hashCode;

    private ColumnLayout(String[] names, FieldSpec[] fieldSpecs) {
      _names = names;
      _fieldSpecs = fieldSpecs;
      int hashCode = names.length;
      for (int i = 0; i < names.length; i++) {
        hashCode = 31 * (31 * hashCode + System.identityHashCode(names[i]))
            + System.identityHashCode(fieldSpecs[i]);
      }
      _hashCode = hashCode;
    }

    @Override
    public int hashCode() {
      return _hashCode;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ColumnLayout)) {
        return false;
      }
      ColumnLayout that = (ColumnLayout) other;
      if (_hashCode != that._hashCode || _names.length != that._names.length) {
        return false;
      }
      for (int i = 0; i < _names.length; i++) {
        if (_names[i] != that._names[i] || _fieldSpecs[i] != that._fieldSpecs[i]) {
          return false;
        }
      }
      return true;
    }
  }

  private static final class Fingerprint {
    private final long _length;
    private final int _crc;

    private Fingerprint(long length, int crc) {
      _length = length;
      _crc = crc;
    }

    private boolean sameAs(Fingerprint other) {
      return _length == other._length && _crc == other._crc;
    }

    static Fingerprint of(@Nullable File file)
        throws IOException {
      if (file == null || !file.isFile()) {
        return new Fingerprint(-1, 0);
      }
      byte[] bytes = Files.readAllBytes(file.toPath());
      return new Fingerprint(bytes.length, crc(bytes, 0, bytes.length));
    }
  }

  private static final class IndexSize {
    private final String _type;
    private final long _size;

    private IndexSize(String type, long size) {
      _type = type;
      _size = size;
    }
  }

  private static final class BlobRef {
    private static final BlobRef ABSENT = new BlobRef(-1, 0);
    private final int _offset;
    private final int _length;

    private BlobRef(int offset, int length) {
      _offset = offset;
      _length = length;
    }
  }

  private static final class BlobWriter {
    private final int _base;
    private final ByteArrayOutputStream _bytes = new ByteArrayOutputStream();

    private BlobWriter(int base) {
      _base = base;
    }

    private BlobRef add(@Nullable String value) {
      return value != null ? add(value.getBytes(StandardCharsets.UTF_8)) : BlobRef.ABSENT;
    }

    private BlobRef addJson(@Nullable Object value)
        throws IOException {
      return value != null ? add(JsonUtils.objectToString(value)) : BlobRef.ABSENT;
    }

    private BlobRef add(byte[] value) {
      int offset = _base + _bytes.size();
      _bytes.writeBytes(value);
      return new BlobRef(offset, value.length);
    }

    private byte[] toByteArray() {
      return _bytes.toByteArray();
    }
  }
}
